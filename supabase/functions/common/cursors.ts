import type { SupabaseClient } from "supabase";

export async function getCursor<T>(
  supabase: SupabaseClient,
  streamName: string,
  defaultCursor: T,
): Promise<T> {
  const { data, error } = await supabase
    .from("sync_state")
    .select("cursor_json")
    .eq("stream_name", streamName)
    .maybeSingle();

  if (error) {
    throw error;
  }

  return (data?.cursor_json as T | null) ?? defaultCursor;
}

export async function markCursorAttempt(
  supabase: SupabaseClient,
  streamName: string,
  currentCursor: unknown,
): Promise<void> {
  const { error } = await supabase.from("sync_state").upsert(
    {
      stream_name: streamName,
      cursor_json: currentCursor,
      last_attempt_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
    { onConflict: "stream_name" },
  );

  if (error) {
    throw error;
  }
}

export async function setCursorSuccess(
  supabase: SupabaseClient,
  streamName: string,
  nextCursor: unknown,
): Promise<void> {
  const now = new Date().toISOString();

  const { error } = await supabase.from("sync_state").upsert(
    {
      stream_name: streamName,
      cursor_json: nextCursor,
      last_success_at: now,
      last_attempt_at: now,
      last_error: null,
      updated_at: now,
    },
    { onConflict: "stream_name" },
  );

  if (error) {
    throw error;
  }
}

export async function setCursorFailure(
  supabase: SupabaseClient,
  streamName: string,
  currentCursor: unknown,
  errorText: string,
): Promise<void> {
  const now = new Date().toISOString();

  const { error } = await supabase.from("sync_state").upsert(
    {
      stream_name: streamName,
      cursor_json: currentCursor,
      last_attempt_at: now,
      last_error: errorText,
      updated_at: now,
    },
    { onConflict: "stream_name" },
  );

  if (error) {
    throw error;
  }
}