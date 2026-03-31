import "jsr:@supabase/functions-js/edge-runtime.d.ts";
import { getAdminClient } from "../common/supabase.ts";

const POSITIONS_ENDPOINT =
  "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/positions-subgraph/0.0.7/gn";

const ORDERS_ENDPOINT =
  "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/orderbook-subgraph/0.0.1/gn";

const ACTIVITY_ENDPOINT =
  "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/activity-subgraph/0.0.4/gn";

const OPEN_INTEREST_ENDPOINT =
  "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/oi-subgraph/0.0.6/gn";

type GraphQLResponse<T> = {
  data?: T;
  errors?: Array<{ message: string }>;
};

async function graphqlPost<T>(
  endpoint: string,
  query: string,
  variables: Record<string, unknown>,
): Promise<T> {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Accept": "application/json",
    },
    body: JSON.stringify({ query, variables }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Subgraph request failed: ${response.status} ${body}`);
  }

  const json = (await response.json()) as GraphQLResponse<T>;

  if (json.errors?.length) {
    throw new Error(
      `Subgraph GraphQL errors: ${json.errors.map((e) => e.message).join("; ")}`,
    );
  }

  if (!json.data) {
    throw new Error("Subgraph response missing data.");
  }

  return json.data;
}

function asString(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

export function unixToIso(ts: number | string | null | undefined): string | null {
  const n = asNumber(ts);
  if (n === null) return null;
  const ms = n > 1e12 ? n : n * 1000;
  return new Date(ms).toISOString();
}

export type PositionsPageParams = {
  first: number;
  afterId: string;
};

export async function fetchPositionsPage(params: PositionsPageParams) {
  const query = `
    query PositionsPage($first: Int!, $afterId: String!) {
      userBalances(
        first: $first
        orderBy: id
        orderDirection: asc
        where: { id_gt: $afterId }
      ) {
        id
        user
        tokenId
        balance
      }
    }
  `;

  return graphqlPost<{
    userBalances: Array<{
      id: string;
      user?: string;
      tokenId?: string;
      balance?: string;
    }>;
  }>(POSITIONS_ENDPOINT, query, params);
}

export type TokenIdConditionsPageParams = {
  first: number;
  afterId: string;
};

export async function fetchTokenIdConditionsPage(params: TokenIdConditionsPageParams) {
  const query = `
    query TokenIdConditionsPage($first: Int!, $afterId: String!) {
      tokenIdConditions(
        first: $first
        orderBy: id
        orderDirection: asc
        where: { id_gt: $afterId }
      ) {
        id
        condition
        complement
      }
    }
  `;

  return graphqlPost<{
    tokenIdConditions: Array<{
      id: string;
      condition?: string;
      complement?: string;
    }>;
  }>(POSITIONS_ENDPOINT, query, params);
}

export type ActivityPageParams = {
  first: number;
  splitAfterId: string;
  mergeAfterId: string;
  redemptionAfterId: string;
};

export async function fetchActivityPage(params: ActivityPageParams) {
  const query = `
    query ActivityPage(
      $first: Int!,
      $splitAfterId: String!,
      $mergeAfterId: String!,
      $redemptionAfterId: String!
    ) {
      splits(
        first: $first
        orderBy: id
        orderDirection: asc
        where: { id_gt: $splitAfterId }
      ) {
        id
        timestamp
        stakeholder
        condition
        amount
      }

      merges(
        first: $first
        orderBy: id
        orderDirection: asc
        where: { id_gt: $mergeAfterId }
      ) {
        id
        timestamp
        stakeholder
        condition
        amount
      }

      redemptions(
        first: $first
        orderBy: id
        orderDirection: asc
        where: { id_gt: $redemptionAfterId }
      ) {
        id
        timestamp
        redeemer
        condition
        indexSets
        payout
      }
    }
  `;

  return graphqlPost<{
    splits: Array<{
      id: string;
      timestamp?: string;
      stakeholder?: string;
      condition?: string;
      amount?: string;
    }>;
    merges: Array<{
      id: string;
      timestamp?: string;
      stakeholder?: string;
      condition?: string;
      amount?: string;
    }>;
    redemptions: Array<{
      id: string;
      timestamp?: string;
      redeemer?: string;
      condition?: string;
      indexSets?: string[];
      payout?: string;
    }>;
  }>(ACTIVITY_ENDPOINT, query, params);
}

export type OrdersPageParams = {
  first: number;
  filledAfterId: string;
  matchedAfterId: string;
};

export async function fetchOrdersPage(params: OrdersPageParams) {
  const query = `
    query OrdersPage(
      $first: Int!,
      $filledAfterId: String!,
      $matchedAfterId: String!
    ) {
      orderFilledEvents(
        first: $first
        orderBy: id
        orderDirection: asc
        where: { id_gt: $filledAfterId }
      ) {
        id
        transactionHash
        timestamp
        maker
        taker
        makerAssetId
        takerAssetId
        makerAmountFilled
        takerAmountFilled
        fee
        side
        price
      }

      ordersMatchedEvents(
        first: $first
        orderBy: id
        orderDirection: asc
        where: { id_gt: $matchedAfterId }
      ) {
        id
        timestamp
        makerAssetID
        takerAssetID
        makerAmountFilled
        takerAmountFilled
        blockNumber
      }
    }
  `;

  return graphqlPost<{
    orderFilledEvents: Array<{
      id: string;
      transactionHash?: string;
      timestamp?: string;
      maker?: string;
      taker?: string;
      makerAssetId?: string;
      takerAssetId?: string;
      makerAmountFilled?: string;
      takerAmountFilled?: string;
      fee?: string;
      side?: string;
      price?: string;
    }>;
    ordersMatchedEvents: Array<{
      id: string;
      timestamp?: string;
      makerAssetID?: string;
      takerAssetID?: string;
      makerAmountFilled?: string;
      takerAmountFilled?: string;
      blockNumber?: string;
    }>;
  }>(ORDERS_ENDPOINT, query, params);
}

export type OpenInterestPageParams = {
  first: number;
  afterId: string;
};

export async function fetchOpenInterestPage(params: OpenInterestPageParams) {
  const query = `
    query OpenInterestPage($first: Int!, $afterId: String!) {
      marketOpenInterests(
        first: $first
        orderBy: id
        orderDirection: asc
        where: { id_gt: $afterId }
      ) {
        id
        amount
      }

      globalOpenInterests {
        id
        amount
      }
    }
  `;

  return graphqlPost<{
    marketOpenInterests: Array<{
      id: string;
      amount?: string;
    }>;
    globalOpenInterests: Array<{
      id: string;
      amount?: string;
    }>;
  }>(OPEN_INTEREST_ENDPOINT, query, params);
}

/* ---------- Mappers ---------- */

export function buildTokenToConditionMap(
  marketTokens: Array<{ token_id: string; condition_id: string }>,
  tokenIdConditions: Array<{ id: string; condition?: string }>,
): Map<string, string> {
  const map = new Map<string, string>();

  for (const row of marketTokens) {
    map.set(row.token_id, row.condition_id);
  }

  for (const row of tokenIdConditions) {
    if (row.id && row.condition) {
      map.set(row.id, row.condition);
    }
  }

  return map;
}

export function mapPositionSnapshot(
  raw: { id: string; user?: string; tokenId?: string; balance?: string },
  snapshotTs: string,
  tokenToCondition: Map<string, string>,
) {
  const wallet = asString(raw.user);
  const tokenId = asString(raw.tokenId);
  const balance = asString(raw.balance);
  const conditionId = tokenId ? tokenToCondition.get(tokenId) ?? null : null;

  if (!wallet || !tokenId || !balance || !conditionId) {
    return null;
  }

  return {
    snapshot_ts: snapshotTs,
    wallet_address: wallet.toLowerCase(),
    token_id: tokenId,
    condition_id: conditionId,
    balance,
    source: "subgraph_positions",
    raw,
  };
}

export function mapSplitToActivity(raw: {
  id: string;
  timestamp?: string;
  stakeholder?: string;
  condition?: string;
  amount?: string;
}) {
  const txHash = `split:${raw.id}`;
  const blockTs = unixToIso(raw.timestamp);

  if (!blockTs || !raw.condition) return null;

  return {
    tx_hash: txHash,
    log_index: 0,
    block_number: null,
    block_ts: blockTs,
    wallet_address: asString(raw.stakeholder)?.toLowerCase() ?? null,
    condition_id: asString(raw.condition),
    token_id: null,
    activity_type: "split",
    amount: asString(raw.amount),
    collateral_amount: null,
    source: "subgraph_activity",
    raw,
  };
}

export function mapMergeToActivity(raw: {
  id: string;
  timestamp?: string;
  stakeholder?: string;
  condition?: string;
  amount?: string;
}) {
  const txHash = `merge:${raw.id}`;
  const blockTs = unixToIso(raw.timestamp);

  if (!blockTs || !raw.condition) return null;

  return {
    tx_hash: txHash,
    log_index: 0,
    block_number: null,
    block_ts: blockTs,
    wallet_address: asString(raw.stakeholder)?.toLowerCase() ?? null,
    condition_id: asString(raw.condition),
    token_id: null,
    activity_type: "merge",
    amount: asString(raw.amount),
    collateral_amount: null,
    source: "subgraph_activity",
    raw,
  };
}

export function mapRedemptionToActivity(raw: {
  id: string;
  timestamp?: string;
  redeemer?: string;
  condition?: string;
  payout?: string;
}) {
  const txHash = `redemption:${raw.id}`;
  const blockTs = unixToIso(raw.timestamp);

  if (!blockTs || !raw.condition) return null;

  return {
    tx_hash: txHash,
    log_index: 0,
    block_number: null,
    block_ts: blockTs,
    wallet_address: asString(raw.redeemer)?.toLowerCase() ?? null,
    condition_id: asString(raw.condition),
    token_id: null,
    activity_type: "redeem",
    amount: null,
    collateral_amount: asString(raw.payout),
    source: "subgraph_activity",
    raw,
  };
}

export function mapRedemptionRow(raw: {
  id: string;
  timestamp?: string;
  redeemer?: string;
  condition?: string;
  payout?: string;
}) {
  const redemptionTs = unixToIso(raw.timestamp);
  if (!redemptionTs || !raw.condition) return null;

  return {
    tx_hash: `redemption:${raw.id}`,
    wallet_address: asString(raw.redeemer)?.toLowerCase() ?? null,
    condition_id: asString(raw.condition),
    token_id: null,
    redemption_ts: redemptionTs,
    payout_amount: asString(raw.payout),
    collateral_token: "USDC.e",
    source: "subgraph_activity",
    raw,
  };
}

export function mapOrderFillToActivityAndSettlement(
  raw: {
    id: string;
    transactionHash?: string;
    timestamp?: string;
    maker?: string;
    taker?: string;
    makerAssetId?: string;
    takerAssetId?: string;
    makerAmountFilled?: string;
    takerAmountFilled?: string;
    side?: string;
    price?: string;
  },
  tokenToCondition: Map<string, string>,
) {
  const txHash = asString(raw.transactionHash) ?? `orderfill:${raw.id}`;
  const settlementTs = unixToIso(raw.timestamp);
  const makerAssetId = asString(raw.makerAssetId);
  const takerAssetId = asString(raw.takerAssetId);
  const tokenId = makerAssetId && tokenToCondition.has(makerAssetId)
    ? makerAssetId
    : takerAssetId && tokenToCondition.has(takerAssetId)
    ? takerAssetId
    : null;
  const conditionId = tokenId ? tokenToCondition.get(tokenId) ?? null : null;

  if (!settlementTs) return null;

  return {
    activity: {
      tx_hash: txHash,
      log_index: 0,
      block_number: null,
      block_ts: settlementTs,
      wallet_address: asString(raw.taker)?.toLowerCase() ?? asString(raw.maker)?.toLowerCase() ?? null,
      condition_id: conditionId,
      token_id: tokenId,
      activity_type: "order_fill",
      amount: asString(raw.takerAmountFilled) ?? asString(raw.makerAmountFilled),
      collateral_amount: null,
      source: "subgraph_orders",
      raw,
    },
    settlement: {
      tx_hash: txHash,
      condition_id: conditionId,
      settlement_ts: settlementTs,
      settlement_type: "order_fill",
      block_number: null,
      source: "subgraph_orders",
      raw,
    },
  };
}

export function mapOrdersMatchedToSettlement(
  raw: {
    id: string;
    timestamp?: string;
    makerAssetID?: string;
    takerAssetID?: string;
    blockNumber?: string;
  },
  tokenToCondition: Map<string, string>,
) {
  const settlementTs = unixToIso(raw.timestamp);
  const makerAssetId = asString(raw.makerAssetID);
  const takerAssetId = asString(raw.takerAssetID);
  const tokenId = makerAssetId && tokenToCondition.has(makerAssetId)
    ? makerAssetId
    : takerAssetId && tokenToCondition.has(takerAssetId)
    ? takerAssetId
    : null;
  const conditionId = tokenId ? tokenToCondition.get(tokenId) ?? null : null;

  if (!settlementTs) return null;

  return {
    tx_hash: `ordermatch:${raw.id}`,
    condition_id: conditionId,
    settlement_ts: settlementTs,
    settlement_type: "order_match",
    block_number: asNumber(raw.blockNumber),
    source: "subgraph_orders",
    raw,
  };
}

export function mapOpenInterestSnapshot(
  raw: { id: string; amount?: string },
  snapshotTs: string,
) {
  if (!raw.id) return null;

  return {
    snapshot_ts: snapshotTs,
    condition_id: raw.id,
    source: "subgraph_open_interest",
    open_interest: asString(raw.amount) ?? "0",
    raw,
  };
}

const SEVERITY_INFO = "info";
const SEVERITY_WARNING = "warning";
const SEVERITY_CRITICAL = "critical";

function jsonResponse(status: number, body: unknown) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json",
    },
  });
}

async function insertReport(
  supabase: ReturnType<typeof getAdminClient>,
  reportType: string,
  severity: string,
  summary: string,
  details: unknown,
) {
  const { error } = await supabase.from("reconciliation_reports").insert({
    report_type: reportType,
    severity,
    summary,
    details,
  });

  if (error) {
    throw error;
  }
}

async function runTradeVsOnchain(supabase: ReturnType<typeof getAdminClient>) {
  // Fetch ALL order events from subgraph
  let allOrderEvents: Array<{
    id: string;
    transactionHash?: string;
    timestamp?: string;
    maker?: string;
    taker?: string;
    makerAssetId?: string;
    takerAssetId?: string;
    makerAmountFilled?: string;
    takerAmountFilled?: string;
    fee?: string;
    side?: string;
    price?: string;
  }> = [];
  
  let afterId = "";
  let hasMore = true;
  
  console.log("Fetching all order events from subgraph...");
  while (hasMore) {
    const page = await fetchOrdersPage({ first: 1000, filledAfterId: afterId, matchedAfterId: "" });
    allOrderEvents.push(...page.orderFilledEvents);
    console.log(`Fetched ${allOrderEvents.length} order events so far...`);
    
    if (page.orderFilledEvents.length < 1000) {
      hasMore = false;
    } else {
      afterId = page.orderFilledEvents[page.orderFilledEvents.length - 1].id;
    }
  }
  
  console.log(`Total order events fetched from subgraph: ${allOrderEvents.length}`);
  
  // Fetch all trades from database
  const { data: dbTrades, error: tradesError } = await supabase
    .from("trades")
    .select("*");

  if (tradesError) throw tradesError;

  const subgraphCount = allOrderEvents.length;
  const dbCount = dbTrades?.length ?? 0;

  const diff = Math.abs(subgraphCount - dbCount);
  const severity = diff === 0 ? SEVERITY_INFO : diff > 10 ? SEVERITY_CRITICAL : SEVERITY_WARNING;
  const summary = diff === 0
    ? "Subgraph and database trade counts match"
    : `Trade count mismatch: subgraph ${subgraphCount}, database ${dbCount}`;

  await insertReport(supabase, "trade_vs_onchain", severity, summary, { 
    subgraph_count: subgraphCount, 
    db_count: dbCount, 
    diff,
    subgraph_sample: allOrderEvents.slice(0, 10), // Sample of subgraph data
    db_sample: dbTrades?.slice(0, 10) // Sample of db data
  });

  return { report_type: "trade_vs_onchain", severity, summary, issues: diff, total_subgraph_events: allOrderEvents.length };
}

async function runResolvedVsRedemptions(supabase: ReturnType<typeof getAdminClient>) {
  // Fetch ALL redemptions from subgraph
  let allRedemptions: Array<{
    id: string;
    timestamp?: string;
    redeemer?: string;
    condition?: string;
    indexSets?: string[];
    payout?: string;
  }> = [];
  
  let afterId = "";
  let hasMore = true;
  
  console.log("Fetching all redemption events from subgraph...");
  while (hasMore) {
    const page = await fetchActivityPage({ first: 1000, splitAfterId: "", mergeAfterId: "", redemptionAfterId: afterId });
    allRedemptions.push(...page.redemptions);
    console.log(`Fetched ${allRedemptions.length} redemption events so far...`);
    
    if (page.redemptions.length < 1000) {
      hasMore = false;
    } else {
      afterId = page.redemptions[page.redemptions.length - 1].id;
    }
  }
  
  console.log(`Total redemption events fetched from subgraph: ${allRedemptions.length}`);

  // Fetch from database
  const { data: dbRedemptions, error: redemptionsError } = await supabase
    .from("redemptions")
    .select("*");

  if (redemptionsError) throw redemptionsError;

  const subgraphCount = allRedemptions.length;
  const dbCount = dbRedemptions?.length ?? 0;

  const diff = Math.abs(subgraphCount - dbCount);
  const severity = diff === 0 ? SEVERITY_INFO : diff > 10 ? SEVERITY_CRITICAL : SEVERITY_WARNING;
  const summary = diff === 0
    ? "Subgraph and database redemption counts match"
    : `Redemption count mismatch: subgraph ${subgraphCount}, database ${dbCount}`;

  await insertReport(supabase, "resolved_vs_redemptions", severity, summary, { 
    subgraph_count: subgraphCount, 
    db_count: dbCount, 
    diff,
    subgraph_sample: allRedemptions.slice(0, 10), // Sample of subgraph data
    db_sample: dbRedemptions?.slice(0, 10) // Sample of db data
  });

  return { report_type: "resolved_vs_redemptions", severity, summary, total_subgraph_events: allRedemptions.length };
}

declare const Deno: any;

Deno.serve(async (req: Request) => {
  try {
    const supabase = getAdminClient();

    const body = await req.json().catch(() => ({}));
    const reportType = (body.report_type as string | undefined) ?? new URL(req.url).searchParams.get("report_type") ?? "all";

    const results: unknown[] = [];

    if (reportType === "trade_vs_onchain" || reportType === "all") {
      results.push(await runTradeVsOnchain(supabase));
    }

    if (reportType === "resolved_vs_redemptions" || reportType === "all") {
      results.push(await runResolvedVsRedemptions(supabase));
    }

    return jsonResponse(200, {
      ok: true,
      report_type: reportType,
      results,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : JSON.stringify(error, Object.getOwnPropertyNames(error));
    const details = error instanceof Error ? error : { error };
    return jsonResponse(500, {
      ok: false,
      error: message,
      details,
    });
  }
});