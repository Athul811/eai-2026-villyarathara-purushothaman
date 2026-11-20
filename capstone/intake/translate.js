import { XMLParser } from "fast-xml-parser";

export class ValidationError extends Error {}

// ASSUMPTIONS (check against PA4's README if you have it): mobile st 1=new, 2=processing, 3=shipped, 4=delivered
const STATUS = { 1: "new", 2: "processing", 3: "shipped", 4: "delivered" };
const CURRENCY = { 978: "EUR", 840: "USD", 826: "GBP" };

const req = (v, name) => {
  if (v === undefined || v === null || String(v).trim() === "") throw new ValidationError(`missing ${name}`);
  return v;
};
export const normPid = (v) => {
  const m = String(req(v, "productId")).match(/(\d+)\s*$/);
  if (!m) throw new ValidationError(`bad productId ${v}`);
  return `PROD-${m[1]}`;
};
const iso = (v, name) => {
  const d = new Date(req(v, name));
  if (isNaN(d)) throw new ValidationError(`bad ${name}`);
  return d.toISOString();
};
const qty = (v) => {
  const n = Number(req(v, "quantity"));
  if (!Number.isInteger(n) || n < 1) throw new ValidationError("bad quantity");
  return n;
};

export function translateWeb(j) {
  const c = req(j.customer, "customer"), a = req(c.address, "customer.address");
  return {
    orderId: String(req(j.orderId, "orderId")), orderType: req(j.orderType, "orderType"), source: "web",
    receivedAt: new Date().toISOString(), orderDate: iso(j.orderDate, "orderDate"),
    customer: { name: req(c.name, "name"), email: req(c.email, "email"),
      address: { street: req(a.street, "street"), city: req(a.city, "city"),
                 postalCode: req(a.postalCode, "postalCode"), country: req(a.country, "country") } },
    items: req(j.items, "items").map((i) => ({ productId: normPid(i.productId), productName: req(i.productName, "productName"), quantity: qty(i.quantity) })),
    currency: req(j.currency, "currency"), status: req(j.status, "status"),
  };
}

export function translateMobile(j) {
  const parts = String(req(j.addr, "addr")).split(",").map((s) => s.trim());
  if (parts.length < 4) throw new ValidationError("bad addr");
  const [street, city, postalCode, country] = [parts.slice(0, parts.length - 3).join(", "), ...parts.slice(-3)];
  const status = STATUS[j.st], currency = CURRENCY[j.cur];
  if (!status) throw new ValidationError("bad st");
  if (!currency) throw new ValidationError("bad cur");
  return {
    orderId: String(req(j.oid, "oid")), orderType: req(j.ot, "ot"), source: "mobile",
    receivedAt: new Date().toISOString(), orderDate: iso(Number(req(j.ts, "ts")) * 1000, "ts"),
    customer: { name: req(j.cust_name, "cust_name"), email: req(j.cust_email, "cust_email"),
      address: { street, city, postalCode, country } },
    items: req(j.items, "items").map((i) => ({ productId: normPid(i.pid), productName: req(i.pname, "pname"), quantity: qty(i.qty) })),
    currency, status,
  };
}

export function decodeXml(buf, charset) {
  const head = buf.subarray(0, 200).toString("latin1");
  const m = head.match(/encoding\s*=\s*["']([^"']+)["']/i);
  const enc = (charset || m?.[1] || "utf-8").toLowerCase();
  try { return new TextDecoder(enc).decode(buf); }
  catch { throw new ValidationError(`unsupported encoding ${enc}`); }
}

export function translateB2b(buf, charset) {
  let doc;
  try {
    doc = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_", parseTagValue: false,
      parseAttributeValue: false, isArray: (n) => n === "LineItem" }).parse(decodeXml(buf, charset));
  } catch (e) { if (e instanceof ValidationError) throw e; throw new ValidationError("unparseable XML"); }
  const po = req(doc.PurchaseOrder, "PurchaseOrder"), b = req(po.BuyerParty, "BuyerParty"), s = req(b.ShipToAddress, "ShipToAddress");
  const li = req(po.LineItems, "LineItems");
  return {
    orderId: String(req(po["@_orderId"], "orderId")), orderType: req(po["@_orderType"], "orderType"), source: "b2b",
    receivedAt: new Date().toISOString(), orderDate: iso(po["@_orderDate"], "orderDate"),
    customer: { name: req(b.Name, "Name"), email: req(b.ContactEmail, "ContactEmail"),
      address: { street: req(s.Street, "Street"), city: req(s.City, "City"),
                 postalCode: req(s.PostalCode, "PostalCode"), country: req(s["@_country"], "country") } },
    items: req(li.LineItem, "LineItem").map((i) => ({ productId: normPid(i["@_sku"]), productName: req(i.Description, "Description"), quantity: qty(i["@_quantity"]) })),
    currency: req(li["@_currency"], "currency"), status: String(req(po.Status, "Status")).toLowerCase(),
  };
}

// R-4.1.1 to R-4.1.5. Returns an order without prices, or throws {http, code}.
export function detectAndTranslate(contentType, body) {
  const ct = String(contentType || "").toLowerCase();
  const type = ct.split(";")[0].trim();
  const charset = ct.match(/charset\s*=\s*"?([^;"\s]+)/)?.[1];
  if (type === "application/xml" || type === "text/xml") return translateB2b(body, charset);
  if (type !== "application/json") { const e = new Error("unsupported media type"); e.http = 415; e.code = "unsupported_media_type"; throw e; }
  let j;
  try { j = JSON.parse(body.toString("utf8")); } catch { throw new ValidationError("unparseable JSON"); }
  if (j && typeof j === "object" && !Array.isArray(j)) {
    if (j.oid !== undefined) return translateMobile(j);
    if (j.orderId !== undefined && j.customer && typeof j.customer === "object") return translateWeb(j);
  }
  throw new ValidationError("body matches no known order shape");
}
