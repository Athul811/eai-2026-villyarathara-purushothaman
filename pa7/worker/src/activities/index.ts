/**
 * The six activities. Each makes one HTTP call to one mock service.
 *
 * Retry decision (the thing PA6 never had to make):
 *   - 2xx: success, return.
 *   - 4xx: a business refusal. It will refuse again, so non-retryable, with
 *     the mock's own `code` as the failure type.
 *   - 5xx or no answer: temporary, so retryable and the retry policy decides.
 *   - Too slow: not handled here; Temporal fails the attempt itself when
 *     startToCloseTimeout runs out.
 */
import { ApplicationFailure } from "@temporalio/activity";
import type { CanonicalItem } from "../types";

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing required environment variable: ${name}`);
  return value;
}

// docker-compose.yml sets these. Never hardcode a downstream URL.
const PAYMENT_URL = requiredEnv("PAYMENT_URL");
const INVENTORY_URL = requiredEnv("INVENTORY_URL");
const SHIPPING_URL = requiredEnv("SHIPPING_URL");
const NOTIFICATION_URL = requiredEnv("NOTIFICATION_URL");

async function post(url: string, body: unknown): Promise<void> {
  let response: Response;
  try {
    response = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (err) {
    throw ApplicationFailure.retryable(
      `downstream unreachable: ${(err as Error).message}`,
      "downstream_unreachable",
    );
  }
  if (response.ok) return;

  let code = "downstream_failed";
  try {
    const data = (await response.json()) as { code?: unknown };
    if (typeof data.code === "string" && data.code) code = data.code;
  } catch {
    // body was not JSON: keep the default code
  }

  if (response.status >= 400 && response.status < 500) {
    throw ApplicationFailure.nonRetryable(`refused (${response.status}): ${code}`, code);
  }
  throw ApplicationFailure.retryable(`server error (${response.status}): ${code}`, code);
}

/** POST {PAYMENT_URL}/payment/authorize with { orderId, amount }. */
export async function authorizePayment(orderId: string, amount: string): Promise<void> {
  await post(`${PAYMENT_URL}/payment/authorize`, { orderId, amount });
}

/** POST {INVENTORY_URL}/inventory/reserve with { orderId, items }. */
export async function reserveInventory(orderId: string, items: CanonicalItem[]): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/reserve`, { orderId, items });
}

/** POST {SHIPPING_URL}/shipping/create with { orderId }. */
export async function createShipment(orderId: string): Promise<void> {
  await post(`${SHIPPING_URL}/shipping/create`, { orderId });
}

/** POST {NOTIFICATION_URL}/notification/send with { orderId, recipient }. */
export async function sendNotification(orderId: string, recipient: string): Promise<void> {
  await post(`${NOTIFICATION_URL}/notification/send`, { orderId, recipient });
}

/** Compensates authorizePayment: POST {PAYMENT_URL}/payment/refund with { orderId }. */
export async function refundPayment(orderId: string): Promise<void> {
  await post(`${PAYMENT_URL}/payment/refund`, { orderId });
}

/** Compensates reserveInventory: POST {INVENTORY_URL}/inventory/release with { orderId }. */
export async function releaseInventory(orderId: string): Promise<void> {
  await post(`${INVENTORY_URL}/inventory/release`, { orderId });
}
