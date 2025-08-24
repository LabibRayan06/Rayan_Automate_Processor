import { initializeApp, cert } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import { google } from "googleapis";
import fs from "fs";
import path from "path";
import os from "os";
import ytdl from "ytdl-core";
import { download as fbDownload } from "facebook-video-downloader";
import got from "got";

import type { GaxiosError } from "gaxios";

// --- FIREBASE INIT ---
const serviceAccountKey = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
if (!serviceAccountKey) throw new Error("FIREBASE_SERVICE_ACCOUNT_KEY not set");
initializeApp({ credential: cert(JSON.parse(serviceAccountKey)) });
const db = getFirestore();

// --- GOOGLE AUTH ---
function getOAuthClient(tokens: any) {
  const { access_token, refresh_token, expiry_date } = tokens || {};
  if (!access_token || !refresh_token) throw new Error("Invalid OAuth tokens");

  const oAuth2Client = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    process.env.GOOGLE_REDIRECT_URI
  );

  oAuth2Client.setCredentials({ access_token, refresh_token, expiry_date });

  // Refresh token automatically updates Firestore
  oAuth2Client.on("tokens", async (tokens) => {
    if (tokens.refresh_token) {
      await db.collection("user_tokens").doc(tokens.uid).set(tokens, { merge: true });
    }
  });

  return oAuth2Client;
}

// --- UNIVERSAL VIDEO DOWNLOAD ---
async function downloadVideo(url: string, output: string): Promise<void> {
  if (ytdl.validateURL(url)) {
    console.log("🎥 Downloading YouTube video...");
    const stream = ytdl(url, { quality: "highest" });
    await streamToFile(stream, output);

  } else if (url.includes("facebook.com")) {
    console.log("🎥 Downloading Facebook video...");
    const videoUrl = await fbDownload(url);
    const stream = got.stream(videoUrl);
    await streamToFile(stream, output);

  } else {
    console.log("🎥 Downloading direct video link...");
    const stream = got.stream(url);
    await streamToFile(stream, output);
  }
}

function streamToFile(stream: NodeJS.ReadableStream, filePath: string) {
  return new Promise<void>((resolve, reject) => {
    const writer = fs.createWriteStream(filePath);
    stream.pipe(writer);
    writer.on("finish", resolve);
    writer.on("error", reject);
  });
}

// --- UPLOAD TO YOUTUBE ---
async function uploadToYouTube(oAuth2Client: any, videoPath: string, title: string, description: string) {
  const youtube = google.youtube("v3");
  const res = await youtube.videos.insert({
    auth: oAuth2Client,
    part: ["snippet", "status"],
    requestBody: {
      snippet: { title, description },
      status: { privacyStatus: "public" },
    },
    media: { body: fs.createReadStream(videoPath) },
  });

  return res.data;
}

// --- PROCESS SCHEDULES ---
async function processSchedules(slotId: string) {
  console.log(`🔍 Checking slot: ${slotId}`);

  const slotDoc = await db.collection("schedules").doc(slotId).get();
  if (!slotDoc.exists) {
    console.log("⚠️ No schedule found.");
    return;
  }

  const data = slotDoc.data();
  if (!data?.users || typeof data.users !== "object") {
    console.log("⚠️ No users in this slot or invalid format.");
    return;
  }

  for (const [uid, scheduled] of Object.entries<any>(data.users)) {
    console.log(`👤 Checking user: ${uid}`);

    const tokenDoc = await db.collection("user_tokens").doc(uid).get();
    if (!tokenDoc.exists) {
      console.log(`⚠️ No tokens for ${uid}`);
      continue;
    }

    const tokens = tokenDoc.data();
    if (!tokens) {
      console.log(`⚠️ Token data invalid for ${uid}`);
      continue;
    }

    const oAuth2Client = getOAuthClient(tokens);

    const videosSnap = await db.collection("video_submissions")
      .where("uid", "==", uid)
      .where("status", "==", "queued")
      .get();

    if (videosSnap.empty) {
      console.log(`📭 No queued videos for ${uid}`);
      continue;
    }

    for (const doc of videosSnap.docs) {
      const video = doc.data();
      if (!video.originalUrl || !video.title) {
        console.log(`⚠️ Invalid video data for ${doc.id}`);
        continue;
      }

      console.log(`🎬 Processing video: ${video.title}`);

      const tmpPath = path.join(os.tmpdir(), `${doc.id}.mp4`);

      try {
        await downloadVideo(video.originalUrl, tmpPath);

        const uploaded = await uploadToYouTube(oAuth2Client, tmpPath, video.title, video.description || "");

        console.log(`✅ Uploaded video: ${uploaded.id}`);

        await doc.ref.update({
          status: "uploaded",
          youtubeId: uploaded.id,
          uploadedAt: new Date(),
        });
      } catch (err) {
        const e = err as any;
        console.error(`❌ Failed to process video ${doc.id}`, e.message || e);

        await doc.ref.update({
          status: "failed",
          error: e.message || String(e),
        });
      } finally {
        if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
      }
    }
  }
}

// --- ENTRYPOINT ---
const slotId = process.argv[2];
if (!slotId) {
  console.error("⚠️ Slot ID required");
  process.exit(1);
}

processSchedules(slotId)
  .then(() => console.log("✅ Done processing slot"))
  .catch((err) => {
    console.error("❌ Fatal error:", err);
  });
