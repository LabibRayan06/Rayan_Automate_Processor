import os
import json
import sys
from datetime import datetime, timedelta, timezone
import firebase_admin
from firebase_admin import credentials,firestore
from google.cloud.firestore_v1 import field_path
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
import yt_dlp
import logging
import tempfile
import requests
from PIL import Image
import ffmpeg

#--------------------- testing only
from dotenv import load_dotenv
load_dotenv()


# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Firebase Initialization ---
def initialize_firebase():
    try:
        service_account_info = json.loads(os.environ['FIREBASE_SERVICE_ACCOUNT_KEY'])
        cred = credentials.Certificate(service_account_info)
        firebase_admin.initialize_app(cred)
        logging.info("Firebase Admin initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize Firebase: {e}")
        sys.exit(1)

# --- Google API Authentication ---
def get_refreshed_credentials(db, uid):
    token_ref = db.collection('user_tokens').document(uid)
    token_doc = token_ref.get()
    if not token_doc.exists:
        raise Exception(f"Refresh token missing for user {uid}.")

    token_data = token_doc.to_dict()
    creds = Credentials.from_authorized_user_info({
        'refresh_token': token_data.get('refreshToken'),
        'token_uri': 'https://oauth2.googleapis.com/token',
        'client_id': os.environ['GOOGLE_CLIENT_ID'],
        'client_secret': os.environ['GOOGLE_CLIENT_SECRET']
    })

    try:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        return creds
    except Exception as e:
        logging.error(f"Error refreshing access token for user {uid}: {e}")
        if 'invalid_grant' in str(e):
            token_ref.delete()
            raise Exception(f"Authentication expired for user {uid}. Please re-authenticate.")
        raise

# --- Video Processing ---
def download_video(video_url, output_path, cookies_file):
    ydl_opts = {
        'format': 'bestvideo+bestaudio/best',
        'outtmpl': output_path,
        'merge_output_format': 'mp4',           # final merged format
        'quiet': True,
        'socket_timeout': 60
    }
    if os.path.exists(cookies_file):
        ydl_opts['cookiefile'] = cookies_file
        logging.info(f"Using cookies file: {cookies_file}")

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download([video_url])
    logging.info(f"Video downloaded successfully to {output_path}")

def upload_to_youtube(db, uid, video_path, title, description):
    creds = get_refreshed_credentials(db, uid)
    youtube = build('youtube', 'v3', credentials=creds)

    body = {
        'snippet': {
            'title': title,
            'description': description,
            'categoryId': '24'
        },
        'status': {
            'privacyStatus': 'public'
        }
    }

    media = MediaFileUpload(video_path, chunksize=-1, resumable=True)

    request = youtube.videos().insert(
        part=",".join(body.keys()),
        body=body,
        media_body=media
    )
    
    response = None
    while response is None:
        try:
            status, response = request.next_chunk()
            if status:
                logging.info(f"Uploaded {int(status.progress() * 100)}%.")
        except HttpError as e:
            if e.resp.status in [500, 502, 503, 504]:
                logging.warning(f"A retriable HTTP error {e.resp.status} occurred: {e}")
                # Implement retry logic if needed
            else:
                raise
    
    logging.info(f"Upload complete. New Video ID: {response.get('id')}")
    return response.get('id')


def round_to_quarter(dt):
    """Round datetime down to the previous 15-minute mark."""
    # Floor minutes to previous 15
    minute = (dt.minute // 15) * 15
    return dt.replace(minute=minute, second=0, microsecond=0)

# --- Main Logic ---
def process_queue():
    """
    Processes scheduled video uploads by querying Firestore for the active
    time slot, identifying scheduled users, and then processing the
    earliest queued video (one per user) for those users.
    """
    db = firestore.client()

    now_utc = datetime.now(timezone.utc)
    current_quarter_id = round_to_quarter(now_utc).strftime('%H_%M')
    logging.info(f"Starting queue processing for current UTC quarter: {current_quarter_id}")

    schedules_ref = db.collection('schedules')
    schedule_doc = schedules_ref.document(current_quarter_id).get()

    if not schedule_doc.exists:
        logging.info(f"No schedules found for the current quarter: {current_quarter_id}")
        return

    users = schedule_doc.to_dict().get('users', [])
    if not users:
        logging.info("No users scheduled in this time slot.")
        return

    # Limit to 30 users max to prevent overload
    if len(users) > 30:
        logging.warning("More than 30 users scheduled. Processing only the first 30.")
        users = users[:30]

    logging.info(f"Found {len(users)} users scheduled in this quarter.")

    videos_ref = db.collection('video_submissions')
    videos_to_process = []

    # Fetch only one queued video per user (earliest submitted)
    for uid in users:
        user_video_query = (
            videos_ref
            .where('status', '==', 'queued')
            .where('uid', '==', uid)
            .order_by('submittedAt')
            .limit(1)
        )
        user_videos = user_video_query.get()
        videos_to_process.extend(user_videos)

    if not videos_to_process:
        logging.info("No queued videos found for scheduled users.")
        return

    logging.info(f"Processing {len(videos_to_process)} videos (one per user).")

    with tempfile.TemporaryDirectory() as temp_dir:
        for doc in videos_to_process:
            video_data = doc.to_dict()
            video_id = doc.id
            uid = video_data.get('uid')
            title = video_data.get('title')
            original_url = video_data.get('originalUrl')

            logging.info(f"Processing video: {video_id} for user {uid} - '{title}'")
            video_doc_ref = db.collection('video_submissions').document(video_id)

            try:
                video_doc_ref.update({'status': 'processing', 'updatedAt': firestore.SERVER_TIMESTAMP})

                video_path = os.path.join(temp_dir, f"{video_id}.mp4")
                cookies_path = './cookies.txt'

                download_video(original_url, video_path, cookies_path)

                new_video_id = upload_to_youtube(
                    db,
                    uid,
                    video_path,
                    title,
                    video_data.get('description', '')
                )

                update_data = {
                    'status': 'published',
                    'publishedAt': firestore.SERVER_TIMESTAMP,
                    'updatedAt': firestore.SERVER_TIMESTAMP,
                    'newVideoId': new_video_id
                }
                video_doc_ref.update(update_data)
                logging.info(f"Successfully processed and published video: {video_id}")

            except Exception as e:
                logging.error(f"Error processing video {video_id}: {e}")
                video_doc_ref.update({
                    'status': 'error',
                    'errorMessage': str(e),
                    'updatedAt': firestore.SERVER_TIMESTAMP
                })
            finally:
                if os.path.exists(video_path):
                    os.remove(video_path)

if __name__ == "__main__":
    initialize_firebase()
    process_queue()
    logging.info("Finished video queue processing script.")
