import express from "express";
import amqp from "amqplib";
import { readFileSync, appendFileSync, mkdirSync, existsSync } from "node:fs";

const PORT = Number(process.env.PORT || 8801);
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const DATA_DIR = process.env.DATA_DIR || "/data";
const ORCH_URL = process.env.ORCHESTRATOR_URL || "http://orchestrator:8802";
const u = new URL(RABBITMQ_URL);
const MGMT_URL = process.env.MANAGEMENT_URL || `http://${u.hostname}:15672`;
const MGMT_AUTH = "Basic " + Buffer.from(`${decodeURIComponent(u.username)}:${decodeURIComponent(u.password)}`).toString("base64");
const LAG_THRESHOLD = Number(process.env.LAG_THRESHOLD || 5);   // justified in docs/runbook.md
const FILE = `${DATA_DIR}/trace-store.jsonl`;

mkdirSync(DATA_DIR, { recursive: true });
const byCid = new Map();
const seen = new Set();
function index(h) {
  if (seen.has(h.hopId)) return false;
  seen.add(h.hopId);
  const a = byCid.get(h.correlationId) || [];
  a.push(h); byCid.set(h.correlationId, a);
  return true;
}
if (existsSync(FILE)) {
  for (const line of readFileSync(FILE, "utf8").split("\n")) {
    if (!line.trim()) continue;
    try { index(JSON.parse(line)); } catch {}
  }
}

let conn = null, ch = null, consuming = false;
async function connect() {
  try {
    conn = await amqp.connect(RABBITMQ_URL);
    conn.on("error", () => {});
    conn.on("close", () => { conn = null; ch = null; consuming = false; setTimeout(connect, 1000); });
    ch = await conn.createChannel();
    ch.on("error", () => {});
    ch.on("close", () => { ch = null; consuming = false; conn?.close().catch(() => {}); });
    await ch.prefetch(50);
    await ch.consume("trace.store", (msg) => {
      if (!msg) return;
      try {
        const h = JSON.parse(msg.content.toString("utf8"));
        if (h && h.hopId && h.correlationId && index(h)) appendFileSync(FILE, JSON.stringify(h) + "\n");
      } catch (e) { console.error("bad hop", e?.message); }
      try { ch.ack(msg); } catch {}
    });
    consuming = true;
  } catch (e) {
    console.error("connect failed", e?.message);
    ch = null; consuming = false;
    try { await conn?.close(); } catch {}
    conn = null;
    setTimeout(connect, 1000);
  }
}

const QUEUES = ["saga.queue", "notifications.queue", "orders.dlq", "orders.invalid", "trace.store"];
async function queueStats() {
  const out = {};
  for (const q of QUEUES) {
    const c = await conn.createChannel();
    c.on("error", () => {});
    try {
      const st = await c.checkQueue(q);                    // exact ready + consumers, right now
      let unacked = 0;
      try {
        const r = await fetch(`${MGMT_URL}/api/queues/%2F/${q}`, { headers: { Authorization: MGMT_AUTH }, signal: AbortSignal.timeout(1500) });
        if (r.ok) unacked = Number((await r.json()).messages_unacknowledged ?? 0);
      } catch {}
      out[q] = { ready: st.messageCount, unacked, consumers: st.consumerCount };
    } finally { try { await c.close(); } catch {} }
  }
  return out;
}

const app = express();
app.get("/health", (_req, res) => {
  const up = conn !== null && ch !== null && consuming;
  res.status(up ? 200 : 503).json({ status: up ? "ok" : "degraded",
    checks: { broker: up ? "up" : "down", consumers: { "trace.store": consuming ? 1 : 0 } } });
});

app.get("/trace/:cid", (req, res) => {
  const hops = byCid.get(req.params.cid);
  if (!hops?.length) return res.status(404).json({ code: "not_found" });
  const sorted = [...hops].sort((a, b) => (a.startedAt < b.startedAt ? -1 : a.startedAt > b.startedAt ? 1 : 0));
  res.json({ correlationId: req.params.cid, hops: sorted });
});

app.get("/ops/metrics", async (_req, res) => {
  if (!conn) return res.status(503).json({ code: "broker_unavailable" });
  try {
    const queues = await queueStats();
    let stuckSagas = 0;
    try {
      const r = await fetch(`${ORCH_URL}/sagas/stuck`, { signal: AbortSignal.timeout(1500) });
      if (r.ok) stuckSagas = ((await r.json()).sagas || []).length;
    } catch {}
    const dlqDepth = queues["orders.dlq"].ready + queues["orders.dlq"].unacked;
    const invalidDepth = queues["orders.invalid"].ready + queues["orders.invalid"].unacked;
    const a = (key, value, firing, threshold = null) => ({ key, firing, value, threshold });
    const sg = queues["saga.queue"], nt = queues["notifications.queue"];
    res.json({
      generatedAt: new Date().toISOString(), queues, dlqDepth, invalidDepth, stuckSagas,
      alerts: [
        a("dlq_not_empty", dlqDepth, dlqDepth > 0),
        a("invalid_not_empty", invalidDepth, invalidDepth > 0),
        a("saga_consumer_missing", sg.consumers, sg.consumers === 0),
        a("notifier_consumer_missing", nt.consumers, nt.consumers === 0),
        a("saga_consumer_lag", sg.ready, sg.ready > LAG_THRESHOLD, LAG_THRESHOLD),
        a("notifier_consumer_lag", nt.ready, nt.ready > LAG_THRESHOLD, LAG_THRESHOLD),
        a("stuck_sagas", stuckSagas, stuckSagas > 0),
      ],
    });
  } catch (e) { console.error(e); res.status(500).json({ code: "internal_error" }); }
});

app.listen(PORT, () => console.log(`observer listening on ${PORT}`));
connect();
