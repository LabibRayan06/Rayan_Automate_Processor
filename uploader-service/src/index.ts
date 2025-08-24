
import { initializeApp, cert } from 'firebase-admin/app';
import { getFirestore, FieldValue } from 'firebase-admin/firestore';
import { google } from 'googleapis';
import type { GaxiosError } from 'gaxios';
import { spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import axios from 'axios';
import ffmpeg from 'fluent-ffmpeg';
import ffmpegStatic from 'ffmpeg-static';
import { DateTime } from "luxon";

// Ensure environment variables are loaded if you run this script locally
import { config } from 'dotenv';
config({ path: '.env.local' }); // For local testing if you have one
config(); // For .env file

// Tell fluent-ffmpeg where to find FFmpeg
if (ffmpegStatic) {
    ffmpeg.setFfmpegPath(ffmpegStatic);
}

function initializeAdminApp() {
    if (process.env.FIREBASE_SERVICE_ACCOUNT_KEY) {
        const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_KEY);
        initializeApp({
            credential: cert(serviceAccount),
        });
    } else {
        console.warn("FIREBASE_SERVICE_ACCOUNT_KEY not found. Using default credentials for local development.");
        initializeApp();
    }
}

initializeAdminApp();

const db = getFirestore();

const oauth2Client = new google.auth.OAuth2(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  process.env.GOOGLE_REDIRECT_URI
);

class ApiClientError extends Error {
    public isApiClientError = true;
    constructor(message: string, public status: number) {
        super(message);
    }
}

async function getRefreshedToken(uid: string) {
    const tokenDoc = await db.collection('user_tokens').doc(uid).get();
    const refreshToken = tokenDoc.data()?.refreshToken;

    if (!refreshToken) {
         throw new ApiClientError(`Refresh token missing for user ${uid}.`, 401);
    }
    
    oauth2Client.setCredentials({ refresh_token: refreshToken });
    try {
        const { credentials } = await oauth2Client.refreshAccessToken();
        oauth2Client.setCredentials(credentials);
        return credentials;

    } catch(error: any) {
        const err = error as GaxiosError;
        console.error(`Error refreshing access token for user ${uid}:`, err.response?.data || err.message);
        if (err.response?.data?.error === 'invalid_grant') {
             await db.collection('user_tokens').doc(uid).delete();
             throw new ApiClientError(`Authentication has expired for user ${uid}.`, 401);
        }
        throw new ApiClientError(`Could not refresh authentication token for user ${uid}.`, 500);
    }
}

async function downloadFile(url: string, outputPath: string): Promise<void> {
    const writer = fs.createWriteStream(outputPath);
    const response = await axios({
        url,
        method: 'GET',
        responseType: 'stream',
    });
    response.data.pipe(writer);
    return new Promise((resolve, reject) => {
        writer.on('finish', resolve);
        writer.on('error', reject);
    });
}

async function addWatermark(videoPath: string, watermarkPath: string, outputPath: string): Promise<void> {
    return new Promise((resolve, reject) => {
        ffmpeg(videoPath)
            .input(watermarkPath)
            .complexFilter('[1:v] scale=120:-1 [watermark]; [0:v][watermark] overlay=10:10') // Overlay watermark in top-left corner
            .outputOptions('-c:a copy')
            .on('end', () => {
                console.log('Watermarking finished');
                resolve();
            })
            .on('error', (err) => {
                console.error('Error during watermarking:', err);
                reject(err);
            })
            .save(outputPath);
    });
}

async function uploadVideo(uid: string, videoUrl: string, title: string, description: string): Promise<string | undefined> {
    console.log(`Starting upload for user ${uid}, video: ${title}`);
    
    await getRefreshedToken(uid);
    const youtube = google.youtube({ version: 'v3', auth: oauth2Client });

    const tempDir = fs.mkdtempSync(path.join('/tmp', 'video-'));
    const originalVideoPath = path.join(tempDir, 'original.mp4');
    let finalVideoPath = originalVideoPath;

    try {
        // 1. Download video using yt-dlp
        console.log("Downloading video with yt-dlp...");
        const ytDlpArgs = [
            videoUrl,
            '-f', 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
            '-o', originalVideoPath,
        ];
        if (fs.existsSync('./cookies.txt')) {
            ytDlpArgs.push('--cookies', './cookies.txt');
        }

        await new Promise<void>((resolve, reject) => {
            const ytDlp = spawn('yt-dlp', ytDlpArgs);
            ytDlp.on('close', code => code === 0 ? resolve() : reject(new Error(`yt-dlp exited with code ${code}`)));
            ytDlp.stderr.on('data', data => console.error(`yt-dlp stderr: ${data}`));
        });
        console.log("Video downloaded successfully.");

        // 2. Check for and apply watermark
        const userProfile = await db.collection('user_profiles').doc(uid).get();
        const watermarkUrl = userProfile.data()?.watermarkUrl;

        if (watermarkUrl) {
            console.log("Watermark found, applying...");
            const watermarkPath = path.join(tempDir, 'watermark.png');
            const watermarkedVideoPath = path.join(tempDir, 'watermarked.mp4');
            
            await downloadFile(watermarkUrl, watermarkPath);
            await addWatermark(originalVideoPath, watermarkPath, watermarkedVideoPath);

            finalVideoPath = watermarkedVideoPath;
        } else {
            console.log("No watermark found for user, skipping watermarking.");
        }

        // 3. Upload the final video
        console.log(`Starting upload of ${path.basename(finalVideoPath)} to YouTube.`);
        const response = await youtube.videos.insert({
            part: ['snippet', 'status'],
            requestBody: {
                snippet: {
                    title: title,
                    description: description,
                    tags: ['ai', 'automation', 'generated'],
                    categoryId: '28', // Science & Technology
                },
                status: { privacyStatus: 'private' },
            },
            media: {
                body: fs.createReadStream(finalVideoPath),
            },
        });

        const newVideoId = response.data.id;
        console.log(`Successfully uploaded. Video ID: ${newVideoId}`);
        return newVideoId ?? undefined;

    } finally {
        // 4. Cleanup temporary files
        fs.rmSync(tempDir, { recursive: true, force: true });
        console.log("Cleaned up temporary files.");
    }
}


async function processQueue() {
  const now = new Date();
  const windowMinutes = 10;

  // 10-minute window (backward + forward)
  const start = new Date(now.getTime() - windowMinutes * 60 * 1000);
  const end = new Date(now.getTime() + windowMinutes * 60 * 1000);

  console.log(`Checking schedules between ${start.toISOString()} and ${end.toISOString()}`);

  const slotKeys = getSlotKeysInWindow(start, end);
  console.log("Checking slot keys:", slotKeys);

  if (slotKeys.length === 0) {
    console.log("No valid slot keys found in this window.");
    return;
  }

  // Fetch all users scheduled in these slots
  const scheduledUsers: any[] = [];
  for (const slot of slotKeys) {
    const slotDoc = await db.collection("schedules").doc(slot).get();
    if (slotDoc.exists) {
      const data = slotDoc.data();
      if (data?.users) {
        for (const [uid, videos] of Object.entries(data.users)) {
          scheduledUsers.push({ uid, videos });
        }
      }
    }
  }

  if (scheduledUsers.length === 0) {
    console.log("No users scheduled in this 10-minute window.");
    return;
  }

  console.log(`Found ${scheduledUsers.length} scheduled users.`);

  // Process each user's videos
  for (const user of scheduledUsers) {
    for (const video of user.videos as any[]) {
      console.log(`Processing video ${video.id} for user ${user.uid}`);
      try {
        await uploadVideo(user.uid, video.url, video.title, video.description);
      } catch (err) {
        console.error(`Error processing video ${video.id} for user ${user.uid}:`, err);
      }
    }
  }

  console.log("Finished video queue processing.");
}

/**
 * Returns slot keys (HH_MM) inside a given window.
 * Only returns rounded slots (00,15,30,45).
 */
function getSlotKeysInWindow(start: Date, end: Date): string[] {
  const slots: string[] = [];
  const validMinutes = [0, 15, 30, 45];

  let current = new Date(start);
  while (current <= end) {
    const minutes = current.getUTCMinutes();
    if (validMinutes.includes(minutes)) {
      const slot = `${String(current.getUTCHours()).padStart(2, "0")}_${String(minutes).padStart(2, "0")}`;
      if (!slots.includes(slot)) slots.push(slot);
    }
    current.setUTCMinutes(current.getUTCMinutes() + 1);
  }

  return slots;
}

processQueue().catch(error => {
    console.error("Fatal error in process-queue script:", error);
    process.exit(1);
});
