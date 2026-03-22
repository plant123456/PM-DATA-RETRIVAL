const GAMMA_BASE_URL = "https://gamma-api.polymarket.com";

export type GammaFetchOptions = {
  limit: number;
  offset: number;
  active?: boolean;
  closed?: boolean;
  slug?: string;
};

type JsonObject = Record<string, unknown>;

function asArray(value: unknown): unknown[] {
  if (Array.isArray(value)) return value;
  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }
  return [];
}

function asString(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return null;
}

function asBoolean(value: unknown): boolean | null {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    if (value.toLowerCase() === "true") return true;
    if (value.toLowerCase() === "false") return false;
  }
  return null;
}

function asTimestamp(value: unknown): string | null {
  const text = asString(value);
  if (!text) return null;

  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString();
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function pick<T = unknown>(obj: JsonObject, ...keys: string[]): T | undefined {
  for (const key of keys) {
    if (key in obj) return obj[key] as T;
  }
  return undefined;
}

export async function fetchEventsPage(
  options: GammaFetchOptions,
): Promise<JsonObject[]> {
  const url = new URL(`${GAMMA_BASE_URL}/events`);
  url.searchParams.set("limit", String(options.limit));
  url.searchParams.set("offset", String(options.offset));

  if (options.active !== undefined) {
    url.searchParams.set("active", String(options.active));
  }
  if (options.closed !== undefined) {
    url.searchParams.set("closed", String(options.closed));
  }
  if (options.slug) {
    url.searchParams.set("slug", options.slug);
  }

  const response = await fetch(url.toString(), {
    headers: {
      "Accept": "application/json",
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Gamma events request failed: ${response.status} ${body}`);
  }

  const json = await response.json();

  if (!Array.isArray(json)) {
    throw new Error("Expected Gamma /events response to be an array.");
  }

  return json as JsonObject[];
}

export function mapGammaEvent(raw: JsonObject) {
  const now = new Date().toISOString();

  const eventId = asNumber(pick(raw, "id"));
  if (eventId === null) {
    throw new Error("Gamma event missing numeric id.");
  }

  const categoryValue = pick(raw, "category");
  const category =
    typeof categoryValue === "string"
      ? categoryValue
      : typeof categoryValue === "object" && categoryValue !== null
      ? asString((categoryValue as JsonObject).name)
      : null;

  return {
    event_id: eventId,
    slug: asString(pick(raw, "slug")),
    title: asString(pick(raw, "title", "question")),
    description: asString(pick(raw, "description")),
    category,
    active: asBoolean(pick(raw, "active")),
    closed: asBoolean(pick(raw, "closed")),
    archived: asBoolean(pick(raw, "archived")),
    resolved: asBoolean(pick(raw, "resolved")),
    start_date: asTimestamp(pick(raw, "startDate", "start_date")),
    end_date: asTimestamp(
      pick(raw, "endDate", "end_date", "closedTime", "closed_time"),
    ),
    raw,
    updated_at: now,
  };
}

export function extractMarketsFromEvent(eventRaw: JsonObject): JsonObject[] {
  const eventId = asNumber(pick(eventRaw, "id"));
  const markets = asArray(pick(eventRaw, "markets"));

  return markets
    .filter((m): m is JsonObject => typeof m === "object" && m !== null)
    .map((market) => ({
      ...market,
      eventId: pick(market, "eventId", "event_id") ?? eventId,
    }));
}

export function mapGammaMarket(raw: JsonObject, fallbackEventId?: number | null) {
  const now = new Date().toISOString();

  const conditionId = asString(
    pick(raw, "conditionId", "condition_id", "conditionID"),
  );

  if (!conditionId) {
    return null;
  }

  const eventId =
    asNumber(pick(raw, "eventId", "event_id")) ?? fallbackEventId ?? null;

  return {
    condition_id: conditionId,
    event_id: eventId,
    market_id: asString(pick(raw, "id")),
    question_id: asString(pick(raw, "questionID", "questionId", "question_id")),
    market_address: asString(
      pick(raw, "marketAddress", "market_address"),
    ),
    slug: asString(pick(raw, "slug")),
    question: asString(pick(raw, "question", "title")),
    description: asString(pick(raw, "description")),
    active: asBoolean(pick(raw, "active")),
    closed: asBoolean(pick(raw, "closed")),
    archived: asBoolean(pick(raw, "archived")),
    resolved: asBoolean(pick(raw, "resolved")),
    enable_order_book: asBoolean(
      pick(raw, "enableOrderBook", "enable_order_book"),
    ),
    accepting_orders: asBoolean(
      pick(raw, "acceptingOrders", "accepting_orders"),
    ),
    neg_risk: asBoolean(pick(raw, "negRisk", "neg_risk")),
    minimum_tick_size: asString(
      pick(raw, "minimumTickSize", "minimum_tick_size"),
    ),
    start_date: asTimestamp(pick(raw, "startDate", "start_date")),
    end_date: asTimestamp(
      pick(raw, "endDate", "end_date", "closedTime", "closed_time"),
    ),
    raw,
    updated_at: now,
  };
}

export function mapGammaTokens(raw: JsonObject, conditionId: string) {
  const tokenIds = asArray(
    pick(raw, "clobTokenIds", "clobTokenIDs", "tokenIds", "token_ids"),
  ).map(asString);

  const outcomes = asArray(pick(raw, "outcomes")).map(asString);

  const embeddedTokens = asArray(pick(raw, "tokens"))
    .filter((x): x is JsonObject => typeof x === "object" && x !== null)
    .map((token, index) => ({
      token_id: asString(pick(token, "token_id", "tokenId", "id")) ?? tokenIds[index],
      outcome_name:
        asString(pick(token, "outcome", "outcome_name", "name")) ??
        outcomes[index] ??
        `outcome_${index}`,
      outcome_index: index,
    }))
    .filter((row) => !!row.token_id);

  if (embeddedTokens.length > 0) {
    return embeddedTokens.map((row) => ({
      token_id: row.token_id!,
      condition_id: conditionId,
      outcome_index: row.outcome_index,
      outcome_name: row.outcome_name,
    }));
  }

  const fallbackCount = Math.max(tokenIds.length, outcomes.length);

  return Array.from({ length: fallbackCount }, (_, index) => ({
    token_id: tokenIds[index] ?? null,
    condition_id: conditionId,
    outcome_index: index,
    outcome_name: outcomes[index] ?? `outcome_${index}`,
  })).filter((row): row is {
    token_id: string;
    condition_id: string;
    outcome_index: number;
    outcome_name: string;
  } => !!row.token_id);
}

export function dedupeByKey<T>(
  rows: T[],
  keyFn: (row: T) => string,
): T[] {
  const map = new Map<string, T>();
  for (const row of rows) {
    map.set(keyFn(row), row);
  }
  return [...map.values()];
}