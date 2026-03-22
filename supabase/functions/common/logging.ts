import type { SupabaseClient } from "supabase";

export async function startIngestRun(
  supabase: SupabaseClient,
  streamName: string,
  jobName: string,
  metadata: Record<string, unknown> = {},
): Promise<string> {
  const { data, error } = await supabase
    .from("ingest_runs")
    .insert({
      stream_name: streamName,
      job_name: jobName,
      status: "started",
      metadata,
    })
    .select("run_id")
    .single();

  if (error) {
    throw error;
  }

  return data.run_id as string;
}

export async function finishIngestRunSuccess(
  supabase: SupabaseClient,
  runId: string,
  rowsWritten: number,
  metadata: Record<string, unknown> = {},
): Promise<void> {
  const { error } = await supabase
    .from("ingest_runs")
    .update({
      status: "success",
      finished_at: new Date().toISOString(),
      rows_written: rowsWritten,
      metadata,
    })
    .eq("run_id", runId);

  if (error) {
    throw error;
  }
}

export async function finishIngestRunFailure(
  supabase: SupabaseClient,
  runId: string,
  errorText: string,
  metadata: Record<string, unknown> = {},
): Promise<void> {
  const { error } = await supabase
    .from("ingest_runs")
    .update({
      status: "failed",
      finished_at: new Date().toISOString(),
      error_text: errorText,
      metadata,
    })
    .eq("run_id", runId);

  if (error) {
    throw error;
  }
}

export async function insertRawIngestBatch(
  supabase: SupabaseClient,
  rows: Array<{
    source: string;
    endpoint: string;
    natural_key?: string | null;
    payload: unknown;
  }>,
): Promise<void> {
  if (rows.length === 0) return;

  const { error } = await supabase.from("raw_ingest").insert(
    rows.map((row) => ({
      source: row.source,
      endpoint: row.endpoint,
      natural_key: row.natural_key ?? null,
      payload: row.payload,
    })),
  );

  if (error) {
    throw error;
  }
}