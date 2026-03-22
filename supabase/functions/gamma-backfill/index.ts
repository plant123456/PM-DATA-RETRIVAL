import "jsr:@supabase/functions-js/edge-runtime.d.ts";

import { getAdminClient } from "../common/supabase.ts";
import {
  getCursor,
  markCursorAttempt,
  setCursorFailure,
  setCursorSuccess,
} from "../common/cursors.ts";
import {
  finishIngestRunFailure,
  finishIngestRunSuccess,
  insertRawIngestBatch,
  startIngestRun,
} from "../common/logging.ts";
import {
  dedupeByKey,
  extractMarketsFromEvent,
  fetchEventsPage,
  mapGammaEvent,
  mapGammaMarket,
  mapGammaTokens,
} from "../common/gamma.ts";

type RequestMode = "incremental" | "full";

type CursorShape = {
  offset: number;
  limit: number;
};

function jsonResponse(status: number, body: unknown) {
  return new Response(JSON.stringify(body, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json",
    },
  });
}

async function parseRequest(req: Request): Promise<{
  mode: RequestMode;
  limit: number;
  offset?: number;
}> {
  const url = new URL(req.url);
  const modeFromQuery = url.searchParams.get("mode");
  const limitFromQuery = url.searchParams.get("limit");
  const offsetFromQuery = url.searchParams.get("offset");

  let body: Record<string, unknown> = {};
  if (req.method === "POST") {
    try {
      body = await req.json();
    } catch {
      body = {};
    }
  }

  const rawMode = (body.mode as string | undefined) ?? modeFromQuery ?? "incremental";
  const mode: RequestMode = rawMode === "full" ? "full" : "incremental";

  const rawLimit = Number((body.limit as number | string | undefined) ?? limitFromQuery ?? 100);
  const rawOffset = Number((body.offset as number | string | undefined) ?? offsetFromQuery ?? NaN);

  return {
    mode,
    limit: Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(rawLimit, 500) : 100,
    offset: Number.isFinite(rawOffset) && rawOffset >= 0 ? rawOffset : undefined,
  };
}

Deno.serve(async (req) => {
  if (!["GET", "POST"].includes(req.method)) {
    return jsonResponse(405, { error: "Method not allowed" });
  }

  const supabase = getAdminClient();
  const { mode, limit, offset } = await parseRequest(req);

  const streamName = `gamma_events:${mode}`;
  const defaultCursor: CursorShape = {
    offset: 0,
    limit,
  };

  const cursor = await getCursor<CursorShape>(supabase, streamName, defaultCursor);
  const effectiveOffset = offset ?? cursor.offset ?? 0;
  const effectiveLimit = limit ?? cursor.limit ?? 100;

  const requestCursor: CursorShape = {
    offset: effectiveOffset,
    limit: effectiveLimit,
  };

  let runId: string | null = null;

  try {
    await markCursorAttempt(supabase, streamName, requestCursor);

    runId = await startIngestRun(supabase, streamName, "gamma-backfill", {
      mode,
      cursor: requestCursor,
    });

    const events = await fetchEventsPage({
      limit: effectiveLimit,
      offset: effectiveOffset,
      active: mode === "incremental" ? true : undefined,
      closed: mode === "incremental" ? false : undefined,
    });

    const eventRows = [];
    const marketRows = [];
    const tokenRows = [];
    const rawRows = [];

    for (const eventRaw of events) {
      const eventRow = mapGammaEvent(eventRaw);
      eventRows.push(eventRow);

      rawRows.push({
        source: "gamma",
        endpoint: "/events",
        natural_key: String(eventRow.event_id),
        payload: eventRaw,
      });

      const eventMarkets = extractMarketsFromEvent(eventRaw);

      for (const marketRaw of eventMarkets) {
        const marketRow = mapGammaMarket(marketRaw, eventRow.event_id);
        if (!marketRow) continue;

        marketRows.push(marketRow);

        const mappedTokens = mapGammaTokens(marketRaw, marketRow.condition_id);
        tokenRows.push(...mappedTokens);
      }
    }

    const dedupedEvents = dedupeByKey(eventRows, (row) => String(row.event_id));
    const dedupedMarkets = dedupeByKey(marketRows, (row) => row.condition_id);
    const dedupedTokens = dedupeByKey(tokenRows, (row) => row.token_id);

    await insertRawIngestBatch(supabase, rawRows);

    if (dedupedEvents.length > 0) {
      const { error } = await supabase
        .from("events")
        .upsert(dedupedEvents, { onConflict: "event_id" });

      if (error) throw error;
    }

    if (dedupedMarkets.length > 0) {
      const { error } = await supabase
        .from("markets")
        .upsert(dedupedMarkets, { onConflict: "condition_id" });

      if (error) throw error;
    }

    if (dedupedTokens.length > 0) {
      const { error } = await supabase
        .from("market_tokens")
        .upsert(dedupedTokens, { onConflict: "token_id" });

      if (error) throw error;
    }

    const wrapped = events.length < effectiveLimit;
    const nextCursor: CursorShape = {
      offset: wrapped ? 0 : effectiveOffset + events.length,
      limit: effectiveLimit,
    };

    await setCursorSuccess(supabase, streamName, nextCursor);

    const rowsWritten =
      dedupedEvents.length + dedupedMarkets.length + dedupedTokens.length;

    await finishIngestRunSuccess(supabase, runId, rowsWritten, {
      mode,
      fetched_events: events.length,
      written_events: dedupedEvents.length,
      written_markets: dedupedMarkets.length,
      written_tokens: dedupedTokens.length,
      next_cursor: nextCursor,
      wrapped,
    });

    return jsonResponse(200, {
      ok: true,
      mode,
      fetched_events: events.length,
      written_events: dedupedEvents.length,
      written_markets: dedupedMarkets.length,
      written_tokens: dedupedTokens.length,
      next_cursor: nextCursor,
      wrapped,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);

    await setCursorFailure(supabase, streamName, requestCursor, message);

    if (runId) {
      await finishIngestRunFailure(supabase, runId, message, {
        mode,
        cursor: requestCursor,
      });
    }

    return jsonResponse(500, {
      ok: false,
      error: message,
      mode,
      cursor: requestCursor,
    });
  }
});