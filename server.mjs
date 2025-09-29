import express from "express";
import fetch from "node-fetch";
import crypto from "crypto";
import { URL } from "url";
import { createClient } from "@supabase/supabase-js";

/* ===================== ENV ===================== */
const {
  PORT = 3000,
  PUBLIC_BASE_URL,
  WAVESPEED_API_KEY,
  WAVESPEED_BASE = "https://api.wavespeed.ai",
  WAVESPEED_SUBMIT_PATH = "/api/v3/bytedance/seedream-v4/edit-sequential",
  WAVESPEED_RESULT_PATH = "/api/v3/predictions",
  WAVESPEED_AUTH_HEADER = "Authorization",

  SUPABASE_URL,
  SUPABASE_SERVICE_KEY,
  SUPABASE_BUCKET_GENERATIONS = "image_cloner_generations",
} = process.env;

if (!PUBLIC_BASE_URL || !WAVESPEED_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("[BOOT] Missing required env vars. Need PUBLIC_BASE_URL, WAVESPEED_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY");
  process.exit(1);
}

/* ===================== APP & CONSTANTS ===================== */
const app = express();
app.use(express.urlencoded({ extended: true }));
app.use(express.json({ limit: "10mb" }));

const MODEL_NAME = "Seedream v4 (edit-sequential)";

const SUBMIT_MAX_RETRIES = 3;
const SUBMIT_BASE_DELAY_MS = 500;
const JOB_SPACING_MS = 1200;

const POLL_INTERVAL_MS = 7000;
const POLL_TIMEOUT_MS = 20 * 60 * 1000;
const POLL_MAX_RETRIES = 3;
const POLL_BASE_DELAY_MS = 800;

const MIN_PIXELS = 921600; // 960x960 Minimum
const ensureMinPixels = (w, h, min = MIN_PIXELS) => {
  if (w * h >= min) return { w, h };
  const s = Math.sqrt(min / (w * h));
  return { w: Math.ceil(w * s), h: Math.ceil(h * s) };
};

const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
const backoff = (attempt, base = 500) => Math.round(base * Math.pow(2, attempt) * (0.75 + Math.random() * 0.5));
const nowISO = () => new Date().toISOString();
const uuid = () => crypto.randomUUID?.() || crypto.randomBytes(16).toString("hex");

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

/* ===================== Helpers ===================== */
const splitCSV = (str) => (str || "").split(",").map((s) => s.trim()).filter(Boolean);

async function urlToDataURL(imageUrl) {
  let res;
  try { res = await fetch(imageUrl, { timeout: 30000 }); } catch (e) { throw new Error(`Fetch failed for ${imageUrl}: ${e.message}`); }
  if (!res.ok) throw new Error(`Non-200 for ${imageUrl}: ${res.status} ${res.statusText}`);
  const buf = Buffer.from(await res.arrayBuffer());
  const ct = res.headers.get("content-type") || "image/jpeg";
  return `data:${ct};base64,${buf.toString("base64")}`;
}
async function fetchToBuffer(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`Fetch failed ${r.status} ${r.statusText}`);
  return { buf: Buffer.from(await r.arrayBuffer()), ct: r.headers.get("content-type") || "image/png" };
}

/* ===================== Supabase Row ops ===================== */
async function sbGetGenerationsRow(id) {
  const { data, error } = await supabase.from("Generations").select("*").eq("id", id).single();
  if (error) throw error;
  return data;
}
async function sbUpdateGenerationsRow(id, fields) {
  const { error } = await supabase.from("Generations").update(fields).eq("id", id);
  if (error) throw error;
}

/* ===================== WaveSpeed API ===================== */
const authHeader = () =>
  (WAVESPEED_AUTH_HEADER.toLowerCase() === "authorization")
    ? { Authorization: `Bearer ${WAVESPEED_API_KEY}` }
    : { [WAVESPEED_AUTH_HEADER]: WAVESPEED_API_KEY };

// requestId -> parentRowId
const memoryRequestMap = new Map();

async function submitGeneration({ prompt, subjectDataUrl, refDataUrls, width, height }, parentRowId) {
  // IMPORTANT for edit-sequential: REFs first, SUBJECT last
  const images = [...(refDataUrls || []), subjectDataUrl].filter(Boolean);

  const payload = {
    size: `${width}*${height}`,
    max_images: 1,
    enable_base64_output: false,
    enable_sync_mode: false,
    prompt: String(prompt || ""),
    negative_prompt: "text, caption, watermark, logo, emoji, subtitles, overlay, banner, stickers, handwriting",
    images
  };

  const url = new URL(`${WAVESPEED_BASE}${WAVESPEED_SUBMIT_PATH}`);
  url.searchParams.set("webhook", `${PUBLIC_BASE_URL}/webhooks/wavespeed`);

  const res = await fetch(url.toString(), {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeader() },
    body: JSON.stringify(payload)
  });

  if (!res.ok) throw new Error(`WaveSpeed submit failed: ${res.status} ${await res.text()}`);
  const json = await res.json();

  const requestId = json?.data?.id || json?.requestId || json?.id || json?.request_id;
  if (!requestId) throw new Error(`Missing requestId in response: ${JSON.stringify(json)}`);

  if (parentRowId) memoryRequestMap.set(requestId, parentRowId);
  return requestId;
}

async function getResult(requestId) {
  const r = await fetch(`${WAVESPEED_BASE}${WAVESPEED_RESULT_PATH}/${encodeURIComponent(requestId)}/result`, { headers: { ...authHeader() } });
  if (!r.ok) throw new Error(`WaveSpeed result failed: ${r.status} ${await r.text()}`);
  const json = await r.json();
  const data = json.data || json;

  const status = (data.status || json.status || json.state || "processing").toLowerCase();
  let outs = [];
  if (Array.isArray(data.output)) outs = data.output;
  else if (Array.isArray(data.outputs)) outs = data.outputs;
  else if (typeof data.output === "string") outs = [data.output];
  else if (typeof json.output === "string") outs = [json.output];

  return { status, outputs: (outs || []).map(String).filter(Boolean), raw: json };
}

/* ===================== Upload outputs to Supabase Storage ===================== */
async function storeOutputsToSupabaseFolder(folderPath, requestId, outputUrls = []) {
  const publicUrls = [];

  for (let i = 0; i < outputUrls.length; i++) {
    const url = outputUrls[i];
    try {
      const { buf, ct } = await fetchToBuffer(url);
      const key = `${folderPath}/output_${requestId}_${String(i + 1).padStart(2, "0")}.png`;
      const { error } = await supabase.storage.from(SUPABASE_BUCKET_GENERATIONS).upload(key, buf, {
        contentType: ct,
        upsert: true
      });
      if (error) throw error;

      const { data: pub } = supabase.storage.from(SUPABASE_BUCKET_GENERATIONS).getPublicUrl(key);
      publicUrls.push(pub.publicUrl);
    } catch (e) {
      console.warn("[STORE WARN]", url, e.message);
    }
  }
  return publicUrls;
}

/* ===================== Append + finalize ===================== */
function mergeIdString(s, add) {
  const set = new Set((Array.isArray(s) ? s : String(s || "").split(",")).map(t => String(t).trim()).filter(Boolean));
  for (const v of add) set.add(String(v));
  return [...set].join(", ");
}

const SOURCE_CONTENT_TABLE = "image_cloner_source_content";

function extractSourceContentId(row = {}) {
  if (!row || typeof row !== "object") return null;

  const directId = row.image_cloner_source_content_id || row.source_content_id;
  if (directId && typeof directId !== "object") return directId;

  const nested = row.image_cloner_source_content || row.source_content;
  if (nested && typeof nested === "object") {
    if (nested.id) return nested.id;
    if (nested.image_cloner_source_content_id) return nested.image_cloner_source_content_id;
    if (nested.source_content_id) return nested.source_content_id;
  }

  if (row.metadata && typeof row.metadata === "object") {
    const meta = row.metadata;
    if (meta.source_content_id) return meta.source_content_id;
    if (meta.image_cloner_source_content_id) return meta.image_cloner_source_content_id;
  }
  return null;
}

async function resolveOutputFolder(row) {
  if (!row) return `runs/unknown`;
  if (row.output_path_full_folder) return row.output_path_full_folder;

  const sourceContentId = extractSourceContentId(row);
  if (sourceContentId) {
    try {
      const { data, error } = await supabase
        .from(SOURCE_CONTENT_TABLE)
        .select("id,parent_awme_id")
        .eq("id", sourceContentId)
        .single();
      if (error) throw error;
      if (data?.parent_awme_id && data?.id) {
        const folder = `${data.parent_awme_id}_${data.id}`;
        await sbUpdateGenerationsRow(row.id, {
          output_path_full_folder: folder,
          last_update: nowISO()
        });
        row.output_path_full_folder = folder;
        return folder;
      }
    } catch (err) {
      console.warn(`[FOLDER WARN] Failed resolving folder for source content ${sourceContentId}: ${err.message || err}`);
    }
  }

  return `runs/${row.id}`;
}

async function appendOutputsToSupabase(rowId, { outputUrls = [], requestId, failed = false }) {
  const row = await sbGetGenerationsRow(rowId);

  // Folder comes from row (e.g. "image_cloner_generations/3682...") :contentReference[oaicite:1]{index=1}
  const folder = await resolveOutputFolder(row);
  const savedPublicUrls = await storeOutputsToSupabaseFolder(folder, requestId, outputUrls);

  // Strings like in Airtable version, kept for parity
  const seen_ids = mergeIdString(row.seen_ids, requestId ? [requestId] : []);
  const failed_ids = failed ? mergeIdString(row.failed_ids, requestId ? [requestId] : []) : row.failed_ids;

  await sbUpdateGenerationsRow(rowId, {
    seen_ids,
    failed_ids,
    last_update: nowISO()
  });

  // Optional: if your table has these columns, uncomment and keep.
  // await sbUpdateGenerationsRow(rowId, {
  //   output_first_url: savedPublicUrls[0] || null,
  //   output_count: (row.output_count || 0) + savedPublicUrls.length,
  //   output_urls: savedPublicUrls  // if text[] or jsonb column exists
  // });

  await markCompletedIfReady(rowId);
}

async function markCompletedIfReady(rowId) {
  const row = await sbGetGenerationsRow(rowId);
  const req = new Set(String(row.request_ids || "").split(",").map(s => s.trim()).filter(Boolean));
  const seen = new Set(String(row.seen_ids || "").split(",").map(s => s.trim()).filter(Boolean));
  const done = req.size > 0 && [...req].every(x => seen.has(x));
  if (done) {
    await sbUpdateGenerationsRow(rowId, { status: "completed", completed_at: nowISO(), last_update: nowISO() });
  }
}

/* ===================== Core Run ===================== */
async function startRunFromRow(rowId, opts = {}) {
  const rec = await sbGetGenerationsRow(rowId);
  await resolveOutputFolder(rec);

  // Fields from your Supabase row JSON (prompt, subject, reference, size, batch_count...) :contentReference[oaicite:2]{index=2}
  const prompt = String(rec.prompt || "");
  const subject_url = rec.subject || "";
  const reference_urls = rec.reference ? [rec.reference] : [];
  if (!prompt || !subject_url) throw new Error("Row needs prompt + subject");

  let W = 1024, H = 1344;
  if (rec.size) {
    const m = String(rec.size).match(/(\d+)\s*[xX*]\s*(\d+)/);
    if (m) { W = +m[1]; H = +m[2]; }
  }
  ({ w: W, h: H } = ensureMinPixels(W, H));

  // Convert inputs to data URLs
  const subjectDataUrl = await urlToDataURL(subject_url);
  const refDataUrls = [];
  for (const r of reference_urls) { try { refDataUrls.push(await urlToDataURL(r)); } catch (e) { console.warn("[REF WARN]", r, e.message); } }

  // Reset status
  await sbUpdateGenerationsRow(rowId, {
    status: "processing",
    request_ids: "",
    seen_ids: "",
    failed_ids: "",
    last_update: nowISO(),
    model: MODEL_NAME,
    size: `${W}x${H}`
  });

  // Batch count (cap 8 as before)
  const N = Math.max(1, Math.min(8, Number(rec.batch_count || opts.batch || 4)));

  const requestIds = [];
  for (let i = 0; i < N; i++) {
    let rid = null, lastErr = null;
    for (let a = 0; a < SUBMIT_MAX_RETRIES && !rid; a++) {
      try {
        rid = await submitGeneration({ prompt, subjectDataUrl, refDataUrls, width: W, height: H }, rowId);
      } catch (err) {
        lastErr = err; await sleep(backoff(a, SUBMIT_BASE_DELAY_MS));
      }
    }
    if (!rid) { console.error("[SUBMIT FAIL]", lastErr?.message || lastErr); continue; }
    requestIds.push(rid);
    if (i < N - 1) await sleep(JOB_SPACING_MS);
  }

  await sbUpdateGenerationsRow(rowId, { request_ids: requestIds.join(", "), last_update: nowISO() });

  // Poll all
  requestIds.forEach(rid => pollUntilDone(rid, rowId).catch(e => console.error("[POLL ERROR]", rid, e.message)));

  return { rowId, submitted: requestIds.length, request_ids: requestIds };
}

/* ===================== Poller ===================== */
const _lastStatus = new Map();

async function pollUntilDone(requestId, parentRowId) {
  const start = Date.now();
  let attempts = 0;
  let first = true;

  while (Date.now() - start < POLL_TIMEOUT_MS) {
    try {
      const { status, outputs, raw } = await getResult(requestId);
      const prev = _lastStatus.get(requestId);
      if (first) { console.log(`[POLL INIT] ${requestId} status=${status}`, JSON.stringify(raw?.data || raw)); first = false; }
      if (prev !== status) { console.log(`[POLL] ${requestId} ${prev || "(none)"} -> ${status}`); _lastStatus.set(requestId, status); }

      if (["completed", "succeeded", "success"].includes(status)) {
        console.log(`[POLL DONE] ${requestId} outputs=${outputs.length}`);
        await appendOutputsToSupabase(parentRowId, { outputUrls: outputs, requestId, failed: false });
        return;
      }
      if (["failed", "error"].includes(status)) {
        console.warn(`[POLL FAIL] ${requestId}`);
        await appendOutputsToSupabase(parentRowId, { outputUrls: [], requestId, failed: true });
        return;
      }
      await sleep(POLL_INTERVAL_MS);
    } catch (err) {
      console.warn(`[POLL ERROR] ${requestId}: ${err.message || err}`);
      if (attempts < POLL_MAX_RETRIES) { await sleep(backoff(attempts++, POLL_BASE_DELAY_MS)); }
      else { await appendOutputsToSupabase(parentRowId, { outputUrls: [], requestId, failed: true }); return; }
    }
  }
  console.warn(`[POLL TIMEOUT] ${requestId}`);
  await appendOutputsToSupabase(parentRowId, { outputUrls: [], requestId, failed: true });
}

/* ===================== Webhooks ===================== */
// 1) Supabase Database Webhook → on INSERT into Generations
app.post("/webhooks/supabase", async (req, res) => {
  try {
    // Supabase DB Webhooks send: { type, table, schema, record, old_record }
    const { type, table, record } = req.body || {};
    if (type === "INSERT" && (table?.name === "Generations" || table === "Generations")) {
      const rowId = record.id;
      // optional: only start if status is null/empty/queued
      const status = record.status ? String(record.status).toLowerCase() : "";
      if (!status || status === "queued" || status === "pending" || status === "null") {
        const out = await startRunFromRow(rowId);
        return res.json({ ok: true, started: out });
      }
    }
    return res.status(202).json({ ok: false, reason: "Ignored event" });
  } catch (e) {
    console.error("[/webhooks/supabase ERROR]", e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

// 2) WaveSpeed webhook (optional fast-path; poller also handles)
app.post("/webhooks/wavespeed", async (req, res) => {
  try {
    const b = req.body || {};
    const requestId = b.request_id || b.id || b.requestId || b.request || req.query.request_id;
    const status = (b.status || b.state || "").toLowerCase();
    const outputs = Array.isArray(b.output) ? b.output : Array.isArray(b.outputs) ? b.outputs : typeof b.output === "string" ? [b.output] : [];

    const parentRowId = memoryRequestMap.get(requestId);
    if (!requestId || !parentRowId) {
      console.warn(`[WEBHOOK] Unknown parent for requestId=${requestId}. Returning 202.`);
      return res.status(202).json({ ok: false, reason: "Unknown parent; poller will handle." });
    }

    if (["completed", "succeeded", "success"].includes(status)) {
      await appendOutputsToSupabase(parentRowId, { outputUrls: outputs, requestId, failed: false });
    } else if (["failed", "error"].includes(status)) {
      await appendOutputsToSupabase(parentRowId, { outputUrls: [], requestId, failed: true });
    }
    res.json({ ok: true });
  } catch (e) {
    console.error("[/webhooks/wavespeed ERROR]", e);
    res.status(500).json({ error: e.message || String(e) });
  }
});

/* ===================== Misc ===================== */
app.get("/", (_, res) => res.send("WaveSpeed x Supabase is running. POST /webhooks/supabase on INSERT."));
app.get("/debug/prediction/:id", async (req, res) => {
  try {
    const r = await fetch(`${WAVESPEED_BASE}${WAVESPEED_RESULT_PATH}/${encodeURIComponent(req.params.id)}/result`, { headers: { ...authHeader() } });
    const text = await r.text();
    res.status(r.status).type("application/json").send(text);
  } catch (e) {
    res.status(500).json({ error: String(e.message || e) });
  }
});

/* START */
app.listen(PORT, () => {
  console.log(`[BOOT] Server running on http://localhost:${PORT}`);
  console.log(`[BOOT] Public base URL: ${PUBLIC_BASE_URL}`);
  console.log(`[BOOT] Webhook (Supabase): ${PUBLIC_BASE_URL}/webhooks/supabase`);
  console.log(`[BOOT] Webhook (WaveSpeed): ${PUBLIC_BASE_URL}/webhooks/wavespeed`);
});
