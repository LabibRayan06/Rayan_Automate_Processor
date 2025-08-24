import { initializeApp, cert } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import { google } from "googleapis";
import * as fs from "fs";
import * as path from "path";
import { spawn } from "child_process";
import axios from "axios";
import ffmpeg from "fluent-ffmpeg";

import type { GaxiosError } from "gaxios";

// --- FIREBASE INIT ---
initializeApp({
  credential: cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_KEY!)),
});
const db = getFirestore();

// --- GOOGLE AUTH ---
function getOAuthClient(tokens: any) {
  const oAuth2Client = new google.auth.OAuth2(
    process.env.GOOGLE_CLIENT_ID,
    process.env.GOOGLE_CLIENT_SECRET,
    process.env.GOOGLE_REDIRECT_URI
  );
  oAuth2Client.setCredentials(tokens);
  return oAuth2Client;
}

// --- DOWNLOAD VIDEO (from YouTube/Facebook/etc) ---
async function downloadVideo(url: string, output: string): Promise<void> {
  const writer = fs.createWriteStream(output);

  const response = await axios({
    url,
    method: "GET",
    responseType: "stream",
  });

  response.data.pipe(writer);

  return new Promise((resolve, reject) => {
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
      snippet: {
        title,
        description,
      },
      status: {
        privacyStatus: "public",
      },
    },
    media: {
      body: fs.createReadStream(videoPath),
    },
  });

  return res.data;
}

// --- MAIN PROCESSOR ---
async function processSchedules(slotId: string) {
  console.log(`🔍 Checking slot: ${slotId}`);

  const slotDoc = await db.collection("schedules").doc(slotId).get();
  if (!slotDoc.exists) {
    console.log("⚠️ No schedule found.");
    return;
  }

  const data = slotDoc.data();
  if (!data?.users) {
    console.log("⚠️ No users in this slot.");
    return;
  }

  for (const [uid, scheduled] of Object.entries<any>(data.users)) {
    console.log(`👤 Checking user: ${uid}`);

    // --- Get user tokens ---
    const tokenDoc = await db.collection("user_tokens").doc(uid).get();
    if (!tokenDoc.exists) {
      console.log(`⚠️ No tokens for ${uid}`);
      continue;
    }

    const tokens = tokenDoc.data();
    const oAuth2Client = getOAuthClient(tokens);

    // --- Get queued videos ---
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
      console.log(`🎬 Processing video: ${video.title}`);

      const tmpPath = path.join("/tmp", `${doc.id}.mp4`);

      try {
        // 1. Download
        await downloadVideo(video.originalUrl, tmpPath);

        // 2. Upload
        const uploaded = await uploadToYouTube(
          oAuth2Client,
          tmpPath,
          video.title,
          video.description
        );

        console.log(`✅ Uploaded video: ${uploaded.id}`);

        // 3. Update Firestore
        await doc.ref.update({
          status: "uploaded",
          youtubeId: uploaded.id,
          uploadedAt: new Date(),
        });
      } catch (err) {
        const e = err as GaxiosError;
        console.error(`❌ Failed to upload video ${doc.id}`, e.message);

        await doc.ref.update({
          status: "failed",
          error: e.message,
        });
      } finally {
        if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
      }
    }
  }
}

// --- ENTRYPOINT ---
// Suppose your GitHub Actions passes slotId
const slotId = process.argv[2];
if (!slotId) {
  console.error("⚠️ Slot ID required");
  process.exit(1);
}

processSchedules(slotId).then(() => {
  console.log("✅ Done processing slot");
  process.exit(0);
});
