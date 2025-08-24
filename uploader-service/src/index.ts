
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
    const now = DateTime.utc();

    // Define a 10-minute window (last 10 minutes → now + 5 minutes)
    const windowStart = now.minus({ minutes: 10 });
    const windowEnd = now.plus({ minutes: 5 });

    console.log(`Checking schedules between ${windowStart.toISO()} and ${windowEnd.toISO()}`);

    const schedulesRef = db.collection('schedules');
    const scheduleDocsSnapshot = await schedulesRef
        .where('scheduledAt', '>=', windowStart.toJSDate())
        .where('scheduledAt', '<=', windowEnd.toJSDate())
        .get();

    if (scheduleDocsSnapshot.empty) {
        console.log("No users scheduled in this 10-minute window.");
        return;
    }

    // Collect all users to process
    const usersToProcess: string[] = [];
    scheduleDocsSnapshot.forEach(doc => {
        const scheduledUsers = doc.data()?.users || [];
        usersToProcess.push(...scheduledUsers);
    });

    console.log(`Found ${usersToProcess.length} users scheduled in this window.`);

    if (usersToProcess.length === 0) return;

    const submissionsRef = db.collection('video_submissions');
    const videosToProcessSnapshot = await submissionsRef
        .where('status', '==', 'queued')
        .where('uid', 'in', usersToProcess)
        .orderBy('submittedAt')
        .get();

    if (videosToProcessSnapshot.empty) {
        console.log("No queued videos found for the scheduled users.");
        return;
    }

    console.log(`Found ${videosToProcessSnapshot.docs.length} videos to process.`);

    for (const doc of videosToProcessSnapshot.docs) {
        const videoData = doc.data();
        const videoId = doc.id;
        const { uid, title, description, originalUrl } = videoData;

        console.log(`Processing video: ${videoId} for user ${uid} - "${title}"`);
        const videoDocRef = db.collection('video_submissions').doc(videoId);

        try {
            await videoDocRef.update({ status: 'processing', updatedAt: FieldValue.serverTimestamp() });
            const newVideoId = await uploadVideo(uid, originalUrl, title, description);

            const updateData: { [key: string]: any } = {
                status: 'published',
                publishedAt: FieldValue.serverTimestamp(),
                updatedAt: FieldValue.serverTimestamp(),
            };

            if (newVideoId) {
                updateData.newVideoId = newVideoId;
            }

            await videoDocRef.update(updateData);
            console.log(`Successfully processed video: ${videoId}`);
        } catch (error: any) {
            console.error(`Error processing video ${videoId} for user ${uid}:`, error.message);
            await videoDocRef.update({ 
                status: 'error', 
                errorMessage: error.message || 'An unknown error occurred', 
                updatedAt: FieldValue.serverTimestamp() 
            });
        }
    }

    console.log("Finished video queue processing.");
}

processQueue().catch(error => {
    console.error("Fatal error in process-queue script:", error);
    process.exit(1);
});
