/**
 * PA4 - three sources to one canonical order model.
 *
 * Translator (web / mobile / b2b -> canonical), content enricher (prices come
 * from the pricing API, never from the source or from this file), and content
 * filter (payment data is never copied: the canonical objects are built field
 * by field from an allowlist, nothing is spread or cloned from the source).
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { XMLParser } from "fast-xml-parser";

// ---------------------------------------------------------------- contract --

export interface Address {
  street: string;
  city: string;
  postalCode: string;
  country: string;
}

export interface CanonicalCustomer {
  name: string;
  email: string;
  address: Address;
}

export interface CanonicalItem {
  productId: string;
  productName: string;
  quantity: number;
  /** Decimal STRING, e.g. "24.99" - see toDecimalAmount() below. Never a number. */
  unitPrice: string;
  currency: string;
  taxRate: number;
}

export type OrderStatus = "new" | "processing" | "shipped" | "delivered";

export interface CanonicalOrder {
  orderId: string;
  orderType: "standard" | "express" | "b2b";
  source: "web" | "mobile" | "b2b";
  /** When this translator produced the record - new Date().toISOString(), or options.now(). */
  receivedAt: string;
  /** The order's own timestamp, normalized to ISO-8601 UTC. */
  orderDate: string;
  customer: CanonicalCustomer;
  items: CanonicalItem[];
  currency: string;
  status: OrderStatus;
}

export type TransformWarningCode = "UNKNOWN_PRODUCT" | "PRICING_API_ERROR";

export interface TransformWarning {
  code: TransformWarningCode;
  productId: string;
  message: string;
}

export interface TransformResult {
  /**
   * null when every line item failed enrichment (so there is nothing left
   * that would validate against the schema's `minItems: 1`). Otherwise a
   * schema-valid order, possibly with fewer items than the source.
   */
  order: CanonicalOrder | null;
  warnings: TransformWarning[];
}

export interface TranslateOptions {
  /** e.g. "http://localhost:4100" (docker-compose's host port mapping). */
  pricingBaseUrl: string;
  /** Defaults to the key docker-compose.yml sets for the mock-pricing container. */
  apiKey?: string;
  /** Injectable clock, for deterministic tests. Defaults to () => new Date(). */
  now?: () => Date;
}

export const DEFAULT_PRICING_API_KEY = "pa4-pricing-key-2026";

// ------------------------------------------------------------------- paths --

const PA4_ROOT = fileURLToPath(new URL("../../", import.meta.url));

export const DEFAULT_WEB_ORDER_PATH = path.join(PA4_ROOT, "data", "web-order.json");
export const DEFAULT_MOBILE_ORDER_PATH = path.join(PA4_ROOT, "data", "mobile-order.json");
export const DEFAULT_B2B_ORDER_PATH = path.join(PA4_ROOT, "data", "b2b-order.xml");
export const DEFAULT_PRICING_BASE_URL = "http://localhost:4100";

const OUT_DIR = path.join(PA4_ROOT, "out");

// ------------------------------------------------------------- raw shapes --

export interface WebOrderInput {
  orderId: string;
  orderType: string;
  customer: {
    name: string;
    email: string;
    address: Address;
    payment: {
      method: string;
      cardHolder: string;
      cardNumber: string;
      expiryMonth: number;
      expiryYear: number;
    };
  };
  items: Array<{ productId: string; productName: string; quantity: number }>;
  orderDate: string;
  status: string;
  currency: string;
}

export interface MobileOrderInput {
  oid: string;
  ot: string;
  cust_name: string;
  cust_email: string;
  /** "Street, City, PostalCode, Country" - one comma-separated string. */
  addr: string;
  items: Array<{ pid: string; pname: string; qty: number }>;
  /** Unix epoch, seconds. */
  ts: number;
  /** 1=new, 2=processing, 3=shipped, 4=delivered. */
  st: number;
  /** ISO 4217 numeric currency code, e.g. 978 = EUR. */
  cur: number;
  pm: string;
  pan: string;
  pexp: string;
}

export interface ParsedB2BOrder {
  PurchaseOrder: {
    "@_orderId": string;
    "@_orderType": string;
    "@_orderDate": string;
    BuyerParty: {
      Name: string;
      ContactEmail: string;
      ShipToAddress: {
        "@_country": string;
        Street: string;
        City: string;
        PostalCode: string;
      };
      [key: string]: unknown;
    };
    LineItems: {
      "@_currency": string;
      LineItem: unknown;
    };
    Status: string;
    [key: string]: unknown;
  };
}

const xmlParser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });

// ------------------------------------------------------------------ helpers --

/** Any value from a parsed document -> trimmed string ("" for missing). */
function text(value: unknown): string {
  return value === undefined || value === null ? "" : String(value).trim();
}

function readText(filePath: string): string {
  // Strip a UTF-8 BOM if there is one; JSON.parse rejects it.
  return readFileSync(filePath, "utf8").replace(/^\uFEFF/, "");
}

/**
 * Normalize a source-specific product id to the pricing API's own format,
 * "PROD-XXX", before calling it.
 *   web:    "PROD-001"      -> unchanged
 *   mobile: "001"           -> "PROD-001"
 *   b2b:    "SKU-PROD-001"  -> "PROD-001"
 */
export function normalizeProductId(source: "web" | "mobile" | "b2b", rawId: string): string {
  const id = text(rawId);
  switch (source) {
    case "web":
      return id;
    case "mobile":
      return /^\d+$/.test(id) ? `PROD-${id}` : id;
    case "b2b":
      return id.startsWith("SKU-") ? id.slice("SKU-".length) : id;
  }
}

/**
 * Price (number from the pricing API) -> decimal string with exactly two
 * fraction digits: 24.99 -> "24.99", 4.5 -> "4.50", 22 -> "22.00".
 * Pure formatting, no arithmetic.
 */
export function toDecimalAmount(price: number): string {
  return price.toFixed(2);
}

export type EnrichResult =
  | { status: "ok"; unitPrice: string; currency: string; taxRate: number; productName: string }
  | { status: "unknown_product" }
  | { status: "error"; httpStatus: number };

/**
 * GET {pricingBaseUrl}/pricing/{productId} with the X-API-Key header.
 *   200 -> ok, price formatted via toDecimalAmount()
 *   404 -> unknown_product (no invented price)
 *   anything else, including a network failure or timeout -> error (never a price of 0)
 */
export async function enrichFromPricing(
  productId: string,
  options: TranslateOptions,
): Promise<EnrichResult> {
  const base = options.pricingBaseUrl.replace(/\/+$/, "");
  const url = `${base}/pricing/${encodeURIComponent(productId)}`;

  let response: Response;
  try {
    response = await fetch(url, {
      headers: { "X-API-Key": options.apiKey ?? DEFAULT_PRICING_API_KEY },
      signal: AbortSignal.timeout(5000),
    });
  } catch {
    return { status: "error", httpStatus: 0 };
  }

  if (response.status === 404) {
    return { status: "unknown_product" };
  }
  if (!response.ok) {
    return { status: "error", httpStatus: response.status };
  }

  try {
    const body = (await response.json()) as {
      unitPrice?: unknown;
      currency?: unknown;
      taxRate?: unknown;
      productName?: unknown;
    };
    const price = typeof body.unitPrice === "string" ? Number(body.unitPrice) : body.unitPrice;
    if (
      typeof price !== "number" ||
      !Number.isFinite(price) ||
      typeof body.currency !== "string" ||
      typeof body.taxRate !== "number"
    ) {
      return { status: "error", httpStatus: response.status };
    }
    return {
      status: "ok",
      unitPrice: toDecimalAmount(price),
      currency: body.currency,
      taxRate: body.taxRate,
      productName: typeof body.productName === "string" ? body.productName : "",
    };
  } catch {
    return { status: "error", httpStatus: response.status };
  }
}

/**
 * Enrich each (already normalized) line against the pricing API. Priced lines
 * become CanonicalItems; unknown products and API errors become warnings.
 * One bad line never stops the others.
 */
export async function enrichItems(
  lines: Array<{ productId: string; productName: string; quantity: number }>,
  options: TranslateOptions,
): Promise<{ items: CanonicalItem[]; warnings: TransformWarning[] }> {
  const results = await Promise.all(
    lines.map(async (line) => ({ line, result: await enrichFromPricing(line.productId, options) })),
  );

  const items: CanonicalItem[] = [];
  const warnings: TransformWarning[] = [];

  for (const { line, result } of results) {
    if (result.status === "ok") {
      items.push({
        productId: line.productId,
        productName: line.productName || result.productName,
        quantity: line.quantity,
        unitPrice: result.unitPrice,
        currency: result.currency,
        taxRate: result.taxRate,
      });
    } else if (result.status === "unknown_product") {
      warnings.push({
        code: "UNKNOWN_PRODUCT",
        productId: line.productId,
        message: `pricing API does not know product ${line.productId}; line item dropped`,
      });
    } else {
      warnings.push({
        code: "PRICING_API_ERROR",
        productId: line.productId,
        message: `pricing API failed for ${line.productId} (http status ${result.httpStatus}); line item dropped`,
      });
    }
  }

  return { items, warnings };
}

/** Unix epoch SECONDS (UTC) -> ISO-8601: 1788258600 -> "2026-09-01T10:30:00.000Z". */
export function epochSecondsToIso(epochSeconds: number): string {
  return new Date(epochSeconds * 1000).toISOString();
}

/**
 * ISO 4217 numeric -> alpha. An unrecognized code is passed through as a
 * string rather than guessed at or thrown on (tolerant reader); the schema
 * does not restrict currency values, so the order stays valid and the
 * unfamiliar code stays visible downstream.
 */
export function mapCurrencyCode(numericCode: number): string {
  switch (numericCode) {
    case 978:
      return "EUR";
    case 840:
      return "USD";
    case 826:
      return "GBP";
    default:
      return String(numericCode);
  }
}

const VALID_STATUSES: readonly OrderStatus[] = ["new", "processing", "shipped", "delivered"];

function asOrderStatus(raw: string): OrderStatus {
  const value = raw.trim().toLowerCase();
  if ((VALID_STATUSES as readonly string[]).includes(value)) {
    return value as OrderStatus;
  }
  throw new Error(`unrecognized order status "${raw}"`);
}

/** Mobile's integer status enum: 1=new, 2=processing, 3=shipped, 4=delivered. */
export function mapMobileStatus(code: number): OrderStatus {
  const status = VALID_STATUSES[code - 1];
  if (!status || !Number.isInteger(code)) {
    throw new Error(`unrecognized mobile status code ${code}`);
  }
  return status;
}

/** B2B's uppercase status string -> canonical ("NEW" -> "new"). Unknown values throw. */
export function mapB2BStatus(raw: string): OrderStatus {
  return asOrderStatus(text(raw));
}

/**
 * "Street, City, PostalCode, Country" -> Address. Country, postal code and
 * city are taken from the right-hand end, so a street that itself contains
 * ", " still parses correctly.
 */
export function parseMobileAddress(addr: string): Address {
  const parts = addr.split(", ").map((p) => p.trim());
  if (parts.length < 4) {
    throw new Error(`mobile address needs 4 comma-separated parts, got ${parts.length}: "${addr}"`);
  }
  const country = parts[parts.length - 1] ?? "";
  const postalCode = parts[parts.length - 2] ?? "";
  const city = parts[parts.length - 3] ?? "";
  const street = parts.slice(0, parts.length - 3).join(", ");
  return { street, city, postalCode, country };
}

/** Decode the B2B file's raw bytes. The prolog says windows-1257 and means it. */
export function decodeB2BXmlBytes(bytes: Buffer): string {
  return new TextDecoder("windows-1257").decode(bytes);
}

// A filled-in order, or null when no item survived enrichment.
function finish(
  base: Omit<CanonicalOrder, "items">,
  items: CanonicalItem[],
  warnings: TransformWarning[],
): TransformResult {
  if (items.length === 0) {
    return { order: null, warnings };
  }
  return { order: { ...base, items }, warnings };
}

function receivedAt(options: TranslateOptions): string {
  return (options.now?.() ?? new Date()).toISOString();
}

// -------------------------------------------------------------- translators --

export async function translateWeb(
  webOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
  const src = JSON.parse(readText(webOrderPath)) as WebOrderInput;

  const { items, warnings } = await enrichItems(
    (src.items ?? []).map((item) => ({
      productId: normalizeProductId("web", item.productId),
      productName: text(item.productName),
      quantity: Number(item.quantity),
    })),
    options,
  );

  // Customer is built field by field: src.customer.payment is never touched.
  return finish(
    {
      orderId: text(src.orderId),
      orderType: text(src.orderType) as CanonicalOrder["orderType"],
      source: "web",
      receivedAt: receivedAt(options),
      orderDate: new Date(src.orderDate).toISOString(),
      customer: {
        name: text(src.customer.name),
        email: text(src.customer.email),
        address: {
          street: text(src.customer.address.street),
          city: text(src.customer.address.city),
          postalCode: text(src.customer.address.postalCode),
          country: text(src.customer.address.country),
        },
      },
      currency: text(src.currency),
      status: asOrderStatus(text(src.status)),
    },
    items,
    warnings,
  );
}

export async function translateMobile(
  mobileOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
  const src = JSON.parse(readText(mobileOrderPath)) as MobileOrderInput;

  const { items, warnings } = await enrichItems(
    (src.items ?? []).map((item) => ({
      productId: normalizeProductId("mobile", item.pid),
      productName: text(item.pname),
      quantity: Number(item.qty),
    })),
    options,
  );

  // pm / pan / pexp are never read.
  return finish(
    {
      orderId: text(src.oid),
      orderType: text(src.ot) as CanonicalOrder["orderType"],
      source: "mobile",
      receivedAt: receivedAt(options),
      orderDate: epochSecondsToIso(src.ts),
      customer: {
        name: text(src.cust_name),
        email: text(src.cust_email),
        address: parseMobileAddress(src.addr),
      },
      currency: mapCurrencyCode(src.cur),
      status: mapMobileStatus(src.st),
    },
    items,
    warnings,
  );
}

export async function translateB2B(
  b2bOrderPath: string,
  options: TranslateOptions,
): Promise<TransformResult> {
  const xml = decodeB2BXmlBytes(readFileSync(b2bOrderPath));
  const po = (xmlParser.parse(xml) as ParsedB2BOrder).PurchaseOrder;

  // One <LineItem> parses to an object, several to an array: normalize.
  // <UnitListPrice> and every other element we do not map is simply never read.
  const rawLines = ([] as unknown[]).concat(po.LineItems?.LineItem ?? []) as Array<
    Record<string, unknown>
  >;

  const { items, warnings } = await enrichItems(
    rawLines.map((line) => ({
      productId: normalizeProductId("b2b", text(line["@_sku"])),
      productName: text(line.Description),
      quantity: Number.parseInt(text(line["@_quantity"]), 10),
    })),
    options,
  );

  const buyer = po.BuyerParty;
  const ship = buyer.ShipToAddress;

  // PaymentDetails (IBAN, BIC) is never read.
  return finish(
    {
      orderId: text(po["@_orderId"]),
      orderType: text(po["@_orderType"]) as CanonicalOrder["orderType"],
      source: "b2b",
      receivedAt: receivedAt(options),
      orderDate: new Date(text(po["@_orderDate"])).toISOString(),
      customer: {
        name: text(buyer.Name),
        email: text(buyer.ContactEmail),
        address: {
          street: text(ship.Street),
          city: text(ship.City),
          postalCode: text(ship.PostalCode),
          country: text(ship["@_country"]),
        },
      },
      currency: text(po.LineItems?.["@_currency"]),
      status: mapB2BStatus(text(po.Status)),
    },
    items,
    warnings,
  );
}

// -------------------------------------------------------------------- main --

/** Runs all three translators against this assignment's own fixtures and writes pa4/out/*.json. Run with: npm start */
export async function main(): Promise<void> {
  const options: TranslateOptions = { pricingBaseUrl: DEFAULT_PRICING_BASE_URL };

  const results = {
    web: await translateWeb(DEFAULT_WEB_ORDER_PATH, options),
    mobile: await translateMobile(DEFAULT_MOBILE_ORDER_PATH, options),
    b2b: await translateB2B(DEFAULT_B2B_ORDER_PATH, options),
  };

  mkdirSync(OUT_DIR, { recursive: true });
  for (const [name, result] of Object.entries(results)) {
    const outPath = path.join(OUT_DIR, `${name}.json`);
    writeFileSync(outPath, JSON.stringify(result, null, 2) + "\n", "utf8");
    console.log(`wrote ${outPath}`);
  }
}

// Only run main() when this file is executed directly, not when imported by the tests.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
