/**
 * The checkout workflow: the PA6 saga, ported.
 * No I/O here; every call to the outside world is an activity through `act`.
 * Only types are imported from ../activities.
 */
import {
  proxyActivities,
  ActivityFailure,
  ApplicationFailure,
  TimeoutFailure,
} from "@temporalio/workflow";
import type * as activities from "../activities";
import type { CanonicalOrder, CheckoutResult, TraceItem } from "../types";

const act = proxyActivities<typeof activities>({
  startToCloseTimeout: "2500 ms",
  retry: {
    maximumAttempts: 3,
    initialInterval: "200 ms",
    backoffCoefficient: 2,
  },
});

// Money is added as whole cents (integers) and formatted once at the end.
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

type StepOutcome = { ok: true } | { ok: false; code: string };

// Why an activity failed for good: a timeout, or the code the activity threw.
function failureCode(err: unknown): string {
  if (err instanceof ActivityFailure) {
    const cause = err.cause;
    if (cause instanceof TimeoutFailure) return "timeout";
    if (cause instanceof ApplicationFailure) return cause.type || "downstream_failed";
  }
  return "downstream_failed";
}

export async function checkout(order: CanonicalOrder): Promise<CheckoutResult> {
  const orderId = order.orderId;
  const trace: TraceItem[] = [];

  // One trace entry per step, not per attempt.
  async function run(step: string, call: () => Promise<void>): Promise<StepOutcome> {
    const started = new Date();
    try {
      await call();
      const finished = new Date();
      trace.push({
        step,
        status: "success",
        startedAt: started.toISOString(),
        finishedAt: finished.toISOString(),
        durationMs: finished.getTime() - started.getTime(),
      });
      return { ok: true };
    } catch (err) {
      const code = failureCode(err);
      const finished = new Date();
      trace.push({
        step,
        status: code === "timeout" ? "timeout" : "failed",
        startedAt: started.toISOString(),
        finishedAt: finished.toISOString(),
        durationMs: finished.getTime() - started.getTime(),
      });
      return { ok: false, code };
    }
  }

  let amount: string;
  try {
    amount = computeAmount(order);
  } catch {
    return { orderId, status: "failed", code: "validation_error", trace };
  }

  const completed: string[] = [];
  const plan: Array<{ step: string; call: () => Promise<void> }> = [
    { step: "payment", call: () => act.authorizePayment(orderId, amount) },
    { step: "inventory", call: () => act.reserveInventory(orderId, order.items) },
    { step: "shipping", call: () => act.createShipment(orderId) },
    { step: "notification", call: () => act.sendNotification(orderId, order.customer.email) },
  ];

  // Strictly one at a time.
  let failure: { code: string } | undefined;
  for (const { step, call } of plan) {
    const outcome = await run(step, call);
    if (!outcome.ok) {
      failure = outcome;
      break;
    }
    completed.push(step);
  }

  if (!failure) {
    return { orderId, status: "completed", trace };
  }

  // Compensate newest first. Shipping and notification have no undo.
  // A failed undo does not stop the next one.
  let compensationFailed = false;
  if (completed.includes("inventory")) {
    const undo = await run("inventory_release", () => act.releaseInventory(orderId));
    if (!undo.ok) compensationFailed = true;
  }
  if (completed.includes("payment")) {
    const undo = await run("payment_refund", () => act.refundPayment(orderId));
    if (!undo.ok) compensationFailed = true;
  }

  if (compensationFailed) {
    return { orderId, status: "failed", code: "compensation_failed", trace };
  }
  return {
    orderId,
    status: completed.length === 0 ? "failed" : "compensated",
    code: failure.code,
    trace,
  };
}
