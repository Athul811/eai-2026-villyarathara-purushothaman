import express from "express";
import amqp from "amqplib";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import { createHash, randomUUID } from "node:crypto";
import { readFileSync, writeFileSync, renameSync, existsSync, mkdirSync } from "node:fs";
import { detectAndTranslate, ValidationError } from "./translate.js";

const PORT = Number(process.env.PORT || 8800);
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const PRICING_URL = process.env.PRICING_URL;
const PRICING_API_KEY = process.env.PRICING_API_KEY;
const TIMEOUT = Number(process.env.REQUEST_TIMEOUT_MS || 2500);
const ITEMS_MAX = Number(process.env.ORDER_ITEMS_MAX || 50);
const DATA_DIR = process.env.DATA_DIR || "/data";

// ---------- stores (R-3.5): JSON files, atomic rename ----------
mkdirSync(DATA_DIR, { recursive: true });
const load = (f, d) => { try { return JSON.parse(readFileSync(`${DATA_DIR}/${f}`, "utf8")); } catch { return d; } };
const save = (f, v) => { const p = `${DATA_DIR}/${f}`; writeFileSync(`${p}.tmp`, JSON.stringify(v)); renameSync(`${p}.tmp`, p); };
const idem = load("idempotency-store.json", {});
const orders = load("orders.json", {});
for (const [k, v] of Object.entries(idem)) if (v.state === "in_flight") delete idem[k]; // stale after a crash
save("idempotency-store.json", idem);

// ---------- canonical validation ----------
const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const validateOrder = ajv.compile(JSON.parse(readFileSync("/canonical/order.schema.json", "utf8")));

// ---------- broker with runtime reconnect (R-4.9.14) ----------
let conn = null, ch = null;
async function connect() {
  try {
    conn = await amqp.connect(RABBITMQ_URL);
    conn.on("error", () => {});
    conn.on("close", () => { conn = null; ch = null; setTimeout(connect, 1000); });
    ch = await conn.createConfirmChannel();
    ch.on("error", () => {});
    ch.on("close", () => { ch = null; conn?.close().catch(() => {}); });
    await startOutcomeConsumer();
  } catch {
    ch = null;
    try { await conn?.close(); } catch {}
    conn = null;
    setTimeout(connect, 1000);
  }
}
connect();

function publishAccepted(order, headers) {
  return new Promise((resolve, reject) => {
    if (!ch) return reject(new Error("no broker"));
    const timer = setTimeout(() => reject(new Error("confirm timeout")), 5000);
    try {
      ch.publish("orders.exchange", "", Buffer.from(JSON.stringify(order)),
        { persistent: true, contentType: "application/json", messageId: randomUUID(), headers },
        (err) => { clearTimeout(timer); err ? reject(err) : resolve(); });
    } catch (e) { clearTimeout(timer); reject(e); }
  });
}

// ---------- pricing enrichment ----------
const money = (v) => {
  const n = typeof v === "number" ? v : Number(String(v).replace(",", "."));
  if (!Number.isFinite(n)) return null;
  return n.toFixed(2);
};
async function price(productId, correlationId, causationId) {
  try {
    const r = await fetch(`${PRICING_URL}/pricing/${encodeURIComponent(productId)}`, {
      headers: { "X-API-Key": PRICING_API_KEY, "X-Correlation-Id": correlationId, "X-Causation-Id": causationId },
      signal: AbortSignal.timeout(TIMEOUT),
    });
    if (r.status === 404) return { unknown: true };
    if (!r.ok) return { outage: true };
    const p = await r.json();
    const unitPrice = money(p.unitPrice ?? p.price);
    if (unitPrice === null || !p.currency || typeof p.taxRate !== "number") return { outage: true };
    return { unitPrice, currency: p.currency, taxRate: p.taxRate };
  } catch { return { outage: true }; }
}

// ---------- HTTP ----------
const app = express();
app.get("/health", (_req, res) => {
  const up = conn !== null && ch !== null && outcomeUp;
  res.status(up ? 200 : 503).json({ status: up ? "ok" : "degraded",
    checks: { broker: up ? "up" : "down", consumers: { "intake.outcome.queue": outcomeUp ? 1 : 0 } } });
});

app.get("/orders/:cid", (req, res) => {
  const v = orders[req.params.cid];
  v ? res.json(v) : res.status(404).json({ code: "not_found" });
});

app.post("/orders", express.raw({ type: () => true, limit: "2mb" }), async (req, res) => {
  const key = req.get("Idempotency-Key");
  const reply = (status, body, headers = {}) => res.status(status).set(headers).json(body);
  if (!key || !/^[A-Za-z0-9._:-]{1,128}$/.test(key))
    return reply(400, { code: "validation_error", message: "Idempotency-Key missing or malformed" });

  const body = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
  const sha = createHash("sha256").update(body).digest("hex");

  const rec = idem[key];
  if (rec) {
    if (rec.bodySha256 !== sha) return reply(409, { code: "idempotency_payload_mismatch" });
    if (rec.state === "in_flight") return reply(409, { code: "idempotency_conflict" });
    return reply(rec.response.status, rec.response.body);          // R-4.1.13: replay, publish nothing
  }
  idem[key] = { bodySha256: sha, state: "in_flight", createdAt: new Date().toISOString() };
  save("idempotency-store.json", idem);

  const release = () => { delete idem[key]; save("idempotency-store.json", idem); };
  const finish = (status, body2) => {
    idem[key] = { ...idem[key], state: "done", response: { status, body: body2 } };
    save("idempotency-store.json", idem);
    return reply(status, body2);
  };

  try {
    const correlationId = randomUUID();
    const enrichHop = randomUUID();
    const publishHop = randomUUID();

    // detect + translate
    const order = detectAndTranslate(req.get("Content-Type"), body);

    // enrich
    const ids = [...new Set(order.items.map((i) => i.productId))];
    const results = await Promise.all(ids.map((id) => price(id, correlationId, enrichHop)));
    const byId = Object.fromEntries(ids.map((id, i) => [id, results[i]]));
    if (results.some((r) => r.outage)) {
      release();
      return reply(503, { code: "pricing_unavailable" }, { "Retry-After": "1" });
    }
    const unknown = ids.filter((id) => byId[id].unknown);
    if (unknown.length) return finish(422, { code: "unknown_product", productIds: unknown });
    order.items = order.items.map((i) => ({ ...i, unitPrice: byId[i.productId].unitPrice,
      currency: byId[i.productId].currency, taxRate: byId[i.productId].taxRate }));

    // validate
    if (!validateOrder(order) || order.items.length > ITEMS_MAX) {
      release();
      const msg = order.items.length > ITEMS_MAX ? "too many items" : ajv.errorsText(validateOrder.errors);
      return reply(400, { code: "validation_error", message: msg });
    }

    // publish, answer only after the broker confirmed
    const businessKey = `${order.source}:${order.orderId}`;
    try {
      await publishAccepted(order, { correlationId, causationId: publishHop, businessKey });
    } catch {
      release();
      return reply(503, { code: "broker_unavailable" }, { "Retry-After": "1" });
    }
    if (!orders[correlationId]) orders[correlationId] = { correlationId, businessKey, orderId: order.orderId, source: order.source,
      orderType: order.orderType, state: "accepted", updatedAt: new Date().toISOString() };
    save("orders.json", orders);
    idem[key].correlationId = correlationId;
    return finish(202, { correlationId, status: "accepted" });
  } catch (e) {
    release();
    if (e instanceof ValidationError) return reply(400, { code: "validation_error", message: e.message });
    if (e?.http) return reply(e.http, { code: e.code, message: e.message });
    console.error(e);
    return reply(500, { code: "internal_error" });
  }
});

app.listen(PORT, () => console.log(`intake listening on ${PORT}`));

let outcomeUp = false;
const ANY_OUTCOME = ["parked", "completed", "failed", "compensated", "compensation_failed", "needs_attention", "duplicate"];
const MOVES = {
  processing: ANY_OUTCOME.filter((s) => s !== "duplicate"),
  parked: ["processing", ...ANY_OUTCOME.filter((s) => s !== "duplicate" && s !== "parked")],
  compensation_failed: ["compensated", "compensation_failed"],
  needs_attention: ["compensated", "compensation_failed"],
};
async function startOutcomeConsumer() {
  const oc = await conn.createChannel();
  oc.on("error", () => {});
  oc.on("close", () => { outcomeUp = false; });
  await oc.assertQueue("intake.outcome.queue", { durable: true });
  await oc.bindQueue("intake.outcome.queue", "orders.outcome", "#");
  await oc.prefetch(20);
  await oc.consume("intake.outcome.queue", (msg) => {
    if (!msg) return;
    try { applyOutcome(JSON.parse(msg.content.toString("utf8"))); } catch (e) { console.error(e); }
    try { oc.ack(msg); } catch {}
  });
  outcomeUp = true;
}
function applyOutcome(env) {
  if (!env?.correlationId || !env.state) return;
  let v = orders[env.correlationId];
  if (!v) v = orders[env.correlationId] = { correlationId: env.correlationId, businessKey: env.businessKey,
    orderId: env.orderId, source: env.source, orderType: env.orderType, state: "accepted", updatedAt: new Date().toISOString() };
  const ok = v.state === "accepted" || (MOVES[v.state] || []).includes(env.state);
  if (!ok) return;
  v.state = env.state;
  if (env.code !== undefined) v.code = env.code; else delete v.code;
  if (env.duplicateOf !== undefined) v.duplicateOf = env.duplicateOf; else delete v.duplicateOf;
  v.updatedAt = new Date().toISOString();
  save("orders.json", orders);
}

// ---------- DLQ operations (R-4.6) ----------
function confirmPublish(exchange, key, content, options) {
  return new Promise((resolve, reject) => {
    if (!ch) return reject(new Error("no broker"));
    const t = setTimeout(() => reject(new Error("confirm timeout")), 5000);
    try { ch.publish(exchange, key, content, options, (e) => { clearTimeout(t); e ? reject(e) : resolve(); }); }
    catch (e) { clearTimeout(t); reject(e); }
  });
}
const withoutRetryHistory = (h) => Object.fromEntries(Object.entries(h || {}).filter(([k]) =>
  !/^x-(death|first-death|last-death)/.test(k) && !["attempts", "lastError", "originQueue", "deadLetteredAt"].includes(k)));

let dlqLock = Promise.resolve();
const withDlq = (fn) => { const run = dlqLock.then(fn, fn); dlqLock = run.catch(() => {}); return run; };

async function readDlq(rc) {
  const msgs = [];
  for (;;) { const m = await rc.get("orders.dlq", { noAck: false }); if (!m) break; msgs.push(m); }
  return msgs;
}
function entryOf(m) {
  const h = m.properties.headers || {};
  let attempts = Number(h.attempts);
  if (!Number.isFinite(attempts) || attempts < 1) {
    const d = Array.isArray(h["x-death"]) ? h["x-death"].filter((e) => e.queue === h.originQueue && e.reason === "rejected")
      .reduce((n, e) => n + Number(e.count || 0), 0) : 0;
    attempts = d + 1;
  }
  return { messageId: m.properties.messageId || "", correlationId: String(h.correlationId ?? ""),
    originQueue: String(h.originQueue ?? ""), attempts, lastError: String(h.lastError ?? ""),
    deadLetteredAt: h.deadLetteredAt ? String(h.deadLetteredAt)
      : (m.properties.timestamp ? new Date(m.properties.timestamp * 1000).toISOString() : new Date().toISOString()) };
}

app.get("/dlq", async (_req, res) => {
  if (!conn) return res.status(503).json({ code: "broker_unavailable" });
  try {
    const entries = await withDlq(async () => {
      const rc = await conn.createChannel(); rc.on("error", () => {});
      try {
        const msgs = await readDlq(rc);
        const out = msgs.map(entryOf);
        msgs.forEach((m) => rc.nack(m, false, true));        // non-destructive: put them all back
        return out;
      } finally { try { await rc.close(); } catch {} }
    });
    res.json({ count: entries.length, entries });
  } catch (e) { console.error(e); res.status(500).json({ code: "internal_error" }); }
});

app.post("/dlq/replay", express.json(), async (req, res) => {
  if (!conn || !ch) return res.status(503).json({ code: "broker_unavailable" });
  const want = req.body && typeof req.body === "object" ? req.body.correlationId : undefined;
  try {
    const result = await withDlq(async () => {
      const rc = await conn.createChannel(); rc.on("error", () => {});
      try {
        const msgs = await readDlq(rc);
        let replayed = 0;
        for (const m of msgs) {
          const e = entryOf(m);
          if ((want && e.correlationId !== want) || !e.originQueue) { rc.nack(m, false, true); continue; }
          const headers = withoutRetryHistory(m.properties.headers);
          headers.causationId = randomUUID();                // the dlq.replay hop (step 5 will record it)
          try {
            await confirmPublish("orders.return.exchange", e.originQueue, m.content, {
              persistent: true, messageId: randomUUID(), headers,
              ...(m.properties.contentType ? { contentType: m.properties.contentType } : {}) });
            rc.ack(m); replayed++;                           // ack only after the confirmed republish
          } catch { rc.nack(m, false, true); }
        }
        return { replayed, remaining: msgs.length - replayed };
      } finally { try { await rc.close(); } catch {} }
    });
    res.json(result);
  } catch (e) { console.error(e); res.status(500).json({ code: "internal_error" }); }
});
