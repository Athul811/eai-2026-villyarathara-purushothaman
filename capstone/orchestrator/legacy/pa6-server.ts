/**
 * PA6 orchestrator scaffold — ported from the practice-03-orchestration JS lab.
 *
 * This scaffold is intentionally incomplete. `GET /health`, request validation
 * against the canonical order schema, the Idempotency-Key contract shell, and
 * restart-safe file persistence are already wired up. What is NOT implemented
 * is the actual saga:
 *
 *   1) payment authorize
 *   2) inventory reserve
 *   3) shipping create
 *   4) notification send
 *
 * with strict sequencing, trace recording, timeout handling, compensation
 * (in reverse order of completed steps), and the full idempotent-replay
 * policy. See the TODO block inside the POST /checkout handler.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import express, { type Request, type Response } from "express";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import type { CanonicalOrder } from "./canonical-order";

const app = express();
app.use(express.json());
app.use(express.static(path.join(process.cwd(), "public")));

interface Config {
  port: number;
  paymentUrl: string;
  inventoryUrl: string;
  shippingUrl: string;
  notificationUrl: string;
  requestTimeoutMs: number;
}

function readRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function loadConfig(): Config {
  return {
    port: Number(process.env.ORCHESTRATOR_PORT || 3000),
    paymentUrl: readRequiredEnv("PAYMENT_URL"),
    inventoryUrl: readRequiredEnv("INVENTORY_URL"),
    shippingUrl: readRequiredEnv("SHIPPING_URL"),
    notificationUrl: readRequiredEnv("NOTIFICATION_URL"),
    requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 2500),
  };
}

const config = loadConfig();

const DATA_DIR = "/data";
const IDEMPOTENCY_STORE_PATH = path.join(DATA_DIR, "idempotency-store.json");
const SAGA_STORE_PATH = path.join(DATA_DIR, "saga-store.json");

// ---------------------------------------------------------------- contract --
// These shapes mirror grading/schema/*.json. Keep them in sync if you change
// either side — the tests validate persisted files against those schemas.

export interface TraceItem {
  step: string;
  status: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
}

export type SagaState = "completed" | "failed" | "compensated";
export type IdempotencyState = "in_progress" | "completed" | "failed" | "compensated";

interface IdempotencyRecord {
  requestHash: string;
  state: IdempotencyState;
  httpStatus: number;
  response: unknown;
  updatedAt: string;
}

interface IdempotencyStore {
  records: Record<string, IdempotencyRecord>;
}

interface SagaRecord {
  idempotencyKey: string;
  state: SagaState;
  steps: TraceItem[];
  updatedAt: string;
}

interface SagaStore {
  sagas: Record<string, SagaRecord>;
}

function ensureJsonFile(filePath: string, initialData: unknown): void {
  const dirPath = path.dirname(filePath);
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, JSON.stringify(initialData, null, 2), "utf8");
  }
}

function readJsonFile<T>(filePath: string): T {
  ensureJsonFile(filePath, {});
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw || "{}") as T;
}

function writeJsonFile(filePath: string, value: unknown): void {
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function nowIso(): string {
  return new Date().toISOString();
}

function payloadHash(payload: unknown): string {
  const normalized = JSON.stringify(payload);
  const hash = crypto.createHash("sha256").update(normalized).digest("hex");
  return `sha256:${hash}`;
}

// The request body is a canonical order. docker-compose.yml mounts the
// course's canonical/ folder read-only at /canonical, so the orchestrator
// validates against the very file PA4's output was validated against.
const CANONICAL_SCHEMA_PATH = "/canonical/order.schema.json";

if (!fs.existsSync(CANONICAL_SCHEMA_PATH)) {
  throw new Error(
    `${CANONICAL_SCHEMA_PATH} not found. docker-compose.yml mounts ../canonical ` +
      "from your repository root: copy canonical/ from eai-2026 next to pa6/ " +
      '(root README, "Starting an assignment"), then run docker compose up again.',
  );
}

const ajv = new Ajv({ allErrors: true });
addFormats(ajv);
const validateCanonicalOrder = ajv.compile(JSON.parse(fs.readFileSync(CANONICAL_SCHEMA_PATH, "utf8")));

function validateCheckoutPayload(payload: unknown): string | null {
  if (validateCanonicalOrder(payload)) {
    return null;
  }
  return `Request body is not a canonical order: ${ajv.errorsText(validateCanonicalOrder.errors, { dataVar: "body" })}`;
}

function bootstrapStores(): void {
  ensureJsonFile(IDEMPOTENCY_STORE_PATH, { records: {} });
  ensureJsonFile(SAGA_STORE_PATH, { sagas: {} });
}

app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({ status: "ok" });
});

app.get("/debug/trace/:orderId", (req: Request, res: Response) => {
  const orderIdParam = req.params.orderId ?? "";
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  const saga = sagaStore?.sagas?.[orderIdParam];
  if (!saga) {
    res.status(404).json({ code: "not_found", message: "No saga found for this orderId" });
    return;
  }
  res.status(200).json(saga);
});

// The contract (openapi.yaml) promises at most 100 items. The canonical schema
// has no bound, so the service enforces it here. Keep the two numbers equal.
const MAX_ITEMS = 100;

type StepName =
  | "payment"
  | "inventory"
  | "shipping"
  | "notification"
  | "inventory_release"
  | "payment_refund";

interface StepResult {
  ok: boolean;
  timedOut: boolean;
  code: string;
}

// One downstream call with a hard timeout. Always records a trace entry.
async function callDownstream(
  step: StepName,
  url: string,
  body: unknown,
  trace: TraceItem[],
): Promise<StepResult> {
  const startedAt = nowIso();
  const startMs = Date.now();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), config.requestTimeoutMs);
  let result: StepResult = { ok: false, timedOut: false, code: "downstream_unreachable" };

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    if (response.ok) {
      result = { ok: true, timedOut: false, code: "" };
    } else {
      let code = `${step}_failed`;
      try {
        const data = (await response.json()) as { code?: unknown };
        if (typeof data.code === "string" && data.code) {
          code = data.code;
        }
      } catch {
        // body was not JSON: keep the default code
      }
      result = { ok: false, timedOut: false, code };
    }
  } catch (err) {
    const aborted = (err as Error).name === "AbortError";
    result = {
      ok: false,
      timedOut: aborted,
      code: aborted ? "timeout" : "downstream_unreachable",
    };
  } finally {
    clearTimeout(timer);
  }

  trace.push({
    step,
    status: result.ok ? "success" : result.timedOut ? "timeout" : "failed",
    startedAt,
    finishedAt: nowIso(),
    durationMs: Date.now() - startMs,
  });
  return result;
}

// Money is added as whole cents (integers) and formatted once at the end.
// Going through parseFloat would turn "18.90" into 1889.9999999999998 cents.
function toCents(price: string): number {
  const match = /^(-?)(\d+)\.(\d{2})$/.exec(price);
  if (!match) {
    throw new Error(`unitPrice is not a two-digit decimal string: ${price}`);
  }
  const cents = parseInt(match[2]!, 10) * 100 + parseInt(match[3]!, 10);
  return match[1] === "-" ? -cents : cents;
}

function computeAmount(order: CanonicalOrder): string {
  let total = 0;
  for (const item of order.items) {
    total += toCents(item.unitPrice) * item.quantity;
  }
  const negative = total < 0;
  const abs = Math.abs(total);
  const whole = Math.floor(abs / 100);
  const fraction = String(abs % 100).padStart(2, "0");
  return `${negative ? "-" : ""}${whole}.${fraction}`;
}

// Persist the finished saga and its idempotency record. Everything here is
// synchronous (no await between read and write), so two requests finishing at
// the same time cannot overwrite each other's changes.
function finishSaga(
  idempotencyKey: string,
  requestHash: string,
  orderId: string,
  state: SagaState,
  httpStatus: number,
  response: unknown,
  trace: TraceItem[],
): void {
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  if (!sagaStore.sagas) {
    sagaStore.sagas = {};
  }
  sagaStore.sagas[orderId] = {
    idempotencyKey,
    state,
    steps: trace,
    updatedAt: nowIso(),
  };
  writeJsonFile(SAGA_STORE_PATH, sagaStore);

  const store = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!store.records) {
    store.records = {};
  }
  store.records[idempotencyKey] = {
    requestHash,
    state,
    httpStatus,
    response,
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, store);
}

app.post("/checkout", async (req: Request, res: Response) => {
  const idempotencyKey = req.header("Idempotency-Key");
  if (!idempotencyKey) {
    res.status(400).json({
      code: "validation_error",
      message: "Idempotency-Key header is required",
    });
    return;
  }

  const validationError = validateCheckoutPayload(req.body);
  if (validationError) {
    res.status(400).json({
      code: "validation_error",
      message: validationError,
    });
    return;
  }

  const order = req.body as CanonicalOrder;
  if (order.items.length > MAX_ITEMS) {
    res.status(400).json({
      code: "validation_error",
      message: `Request body has ${order.items.length} items, the maximum is ${MAX_ITEMS}`,
    });
    return;
  }

  const orderId = order.orderId;
  const requestHash = payloadHash(req.body);

  // Read, check and claim the key with no await in between, so two requests
  // carrying the same key cannot both pass the check.
  const idempotencyStore = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!idempotencyStore.records) {
    idempotencyStore.records = {};
  }

  const existing = idempotencyStore.records[idempotencyKey];
  if (existing) {
    if (existing.requestHash !== requestHash) {
      res.status(409).json({
        code: "idempotency_payload_mismatch",
        message: "This Idempotency-Key is already used for a different payload",
      });
      return;
    }
    if (existing.state === "in_progress") {
      res.status(409).json({
        code: "idempotency_conflict",
        message: "A request with this Idempotency-Key is still in progress",
      });
      return;
    }
    // Same key, same payload, saga already finished: replay it exactly.
    res.status(existing.httpStatus).json(existing.response);
    return;
  }

  idempotencyStore.records[idempotencyKey] = {
    requestHash,
    state: "in_progress",
    httpStatus: 202,
    response: { orderId, status: "in_progress" },
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, idempotencyStore);

  try {
    const trace: TraceItem[] = [];
    const completed: StepName[] = [];
    const amount = computeAmount(order);

    const plan: Array<{ name: StepName; url: string; body: unknown }> = [
      {
        name: "payment",
        url: `${config.paymentUrl}/payment/authorize`,
        body: { orderId, amount },
      },
      {
        name: "inventory",
        url: `${config.inventoryUrl}/inventory/reserve`,
        body: { orderId, items: order.items },
      },
      {
        name: "shipping",
        url: `${config.shippingUrl}/shipping/create`,
        body: { orderId },
      },
      {
        name: "notification",
        url: `${config.notificationUrl}/notification/send`,
        body: { orderId, recipient: order.customer.email },
      },
    ];

    // Strictly one step at a time: each call starts only after the previous
    // one has succeeded.
    let failure: StepResult | undefined;
    for (const step of plan) {
      const outcome = await callDownstream(step.name, step.url, step.body, trace);
      if (!outcome.ok) {
        failure = outcome;
        break;
      }
      completed.push(step.name);
    }

    // Compensate in reverse order, and only the steps that have an undo action
    // (shipping and notification have none). A failed undo does not stop the
    // others from being attempted.
    let compensationFailed = false;
    if (failure) {
      if (completed.includes("inventory")) {
        const undo = await callDownstream(
          "inventory_release",
          `${config.inventoryUrl}/inventory/release`,
          { orderId },
          trace,
        );
        if (!undo.ok) {
          compensationFailed = true;
        }
      }
      if (completed.includes("payment")) {
        const undo = await callDownstream(
          "payment_refund",
          `${config.paymentUrl}/payment/refund`,
          { orderId },
          trace,
        );
        if (!undo.ok) {
          compensationFailed = true;
        }
      }
    }

    let httpStatus: number;
    let state: SagaState;
    let code: string | undefined;
    if (!failure) {
      httpStatus = 200;
      state = "completed";
    } else if (compensationFailed) {
      httpStatus = 422;
      state = "failed";
      code = "compensation_failed";
    } else {
      httpStatus = failure.timedOut ? 504 : 422;
      state = completed.length === 0 ? "failed" : "compensated";
      code = failure.code;
    }

    const response = {
      orderId,
      status: state,
      ...(code ? { code } : {}),
      trace,
    };

    finishSaga(idempotencyKey, requestHash, orderId, state, httpStatus, response, trace);
    res.status(httpStatus).json(response);
  } catch (err) {
    // Unexpected error: release the key so the caller can retry instead of
    // being stuck behind a permanent in_progress record.
    const store = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
    if (store.records) {
      delete store.records[idempotencyKey];
      writeJsonFile(IDEMPOTENCY_STORE_PATH, store);
    }
    console.error("[orchestrator] unexpected error", err);
    res.status(500).json({
      code: "internal_error",
      message: "Unexpected error while running the checkout",
    });
  }
});

bootstrapStores();

app.listen(config.port, () => {
  console.log(`[orchestrator] listening on port ${config.port}`);
  console.log("[orchestrator] downstream targets loaded from env", {
    paymentUrl: config.paymentUrl,
    inventoryUrl: config.inventoryUrl,
    shippingUrl: config.shippingUrl,
    notificationUrl: config.notificationUrl,
    requestTimeoutMs: config.requestTimeoutMs,
  });
});
