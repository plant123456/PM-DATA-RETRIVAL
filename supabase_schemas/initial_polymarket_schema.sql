create extension if not exists pgcrypto;

-- =========================
-- 1. Catalog tables
-- =========================

create table if not exists events (
  event_id         bigint primary key,
  slug             text unique,
  title            text,
  description      text,
  category         text,
  active           boolean,
  closed           boolean,
  archived         boolean,
  resolved         boolean,
  start_date       timestamptz,
  end_date         timestamptz,
  raw              jsonb not null,
  inserted_at      timestamptz not null default now(),
  updated_at       timestamptz not null default now()
);

create table if not exists markets (
  condition_id     text primary key,
  event_id         bigint references events(event_id),
  market_id        text,
  question_id      text,
  market_address   text,
  slug             text,
  question          text,
  description      text,
  active           boolean,
  closed           boolean,
  archived         boolean,
  resolved         boolean,
  enable_order_book boolean,
  accepting_orders boolean,
  neg_risk         boolean,
  minimum_tick_size numeric,
  start_date       timestamptz,
  end_date         timestamptz,
  raw              jsonb not null,
  inserted_at      timestamptz not null default now(),
  updated_at       timestamptz not null default now()
);

create index if not exists idx_markets_event_id on markets(event_id);
create index if not exists idx_markets_slug on markets(slug);
create index if not exists idx_markets_active_closed on markets(active, closed);

create table if not exists market_tokens (
  token_id         text primary key,
  condition_id     text not null references markets(condition_id),
  outcome_index    int not null,
  outcome_name     text not null,
  inserted_at      timestamptz not null default now(),
  unique (condition_id, outcome_index),
  unique (condition_id, outcome_name)
);

create index if not exists idx_market_tokens_condition_id on market_tokens(condition_id);

-- =========================
-- 2. Historical API tables
-- =========================

create table if not exists price_history (
  token_id         text not null references market_tokens(token_id),
  ts               timestamptz not null,
  price            numeric(18,10) not null,
  granularity      text not null,           -- e.g. 1m, 1h, 1d
  fidelity_minutes int,
  source           text not null default 'clob_prices_history',
  raw              jsonb,
  inserted_at      timestamptz not null default now(),
  primary key (token_id, ts, granularity)
);

create index if not exists idx_price_history_token_ts
  on price_history(token_id, ts desc);

create table if not exists trades (
  trade_pk         uuid primary key default gen_random_uuid(),
  condition_id     text not null references markets(condition_id),
  token_id         text references market_tokens(token_id),
  trade_ts         timestamptz not null,
  side             text,
  price            numeric(18,10),
  size             numeric(28,10),
  outcome          text,
  tx_hash          text,
  user_address     text,
  source           text not null default 'data_api_trades',
  raw              jsonb not null,
  inserted_at      timestamptz not null default now()
);

create unique index if not exists uq_trades_dedupe
  on trades (
    condition_id,
    coalesce(token_id, ''),
    trade_ts,
    coalesce(price, -1),
    coalesce(size, -1),
    coalesce(side, ''),
    coalesce(tx_hash, '')
  );

create index if not exists idx_trades_condition_ts
  on trades(condition_id, trade_ts desc);

create index if not exists idx_trades_token_ts
  on trades(token_id, trade_ts desc);

create index if not exists idx_trades_tx_hash
  on trades(tx_hash);

-- optional but useful if you want API-side OI snapshots later
create table if not exists open_interest_snapshots (
  snapshot_ts      timestamptz not null,
  condition_id     text not null references markets(condition_id),
  source           text not null,           -- 'subgraph_open_interest' or 'data_api_oi'
  open_interest    numeric(38,18) not null,
  raw              jsonb not null,
  inserted_at      timestamptz not null default now(),
  primary key (snapshot_ts, condition_id, source)
);

-- =========================
-- 3. Onchain reconciliation tables
-- =========================

create table if not exists positions_snapshots (
  snapshot_ts      timestamptz not null,
  wallet_address   text not null,
  token_id         text not null references market_tokens(token_id),
  condition_id     text not null references markets(condition_id),
  balance          numeric(38,18) not null,
  source           text not null default 'subgraph_positions',
  raw              jsonb not null,
  inserted_at      timestamptz not null default now(),
  primary key (snapshot_ts, wallet_address, token_id)
);

create index if not exists idx_positions_wallet
  on positions_snapshots(wallet_address);

create index if not exists idx_positions_condition
  on positions_snapshots(condition_id);

create table if not exists onchain_activity (
  tx_hash          text not null,
  log_index        int not null default 0,
  block_number     bigint,
  block_ts         timestamptz,
  wallet_address   text,
  condition_id     text,
  token_id         text,
  activity_type    text not null,          -- split, merge, redeem, transfer, settle
  amount           numeric(38,18),
  collateral_amount numeric(38,18),
  source           text not null default 'subgraph_activity',
  raw              jsonb not null,
  inserted_at      timestamptz not null default now(),
  primary key (tx_hash, log_index)
);

create index if not exists idx_onchain_activity_condition_ts
  on onchain_activity(condition_id, block_ts desc);

create index if not exists idx_onchain_activity_wallet_ts
  on onchain_activity(wallet_address, block_ts desc);

create table if not exists redemptions (
  tx_hash          text primary key,
  wallet_address   text,
  condition_id     text,
  token_id         text,
  redemption_ts    timestamptz not null,
  payout_amount    numeric(38,18),
  collateral_token text,
  source           text not null default 'subgraph_activity',
  raw              jsonb not null,
  inserted_at      timestamptz not null default now()
);

create table if not exists settlements (
  tx_hash          text primary key,
  condition_id     text,
  settlement_ts    timestamptz not null,
  settlement_type  text,
  block_number     bigint,
  source           text not null default 'subgraph_orders_or_activity',
  raw              jsonb not null,
  inserted_at      timestamptz not null default now()
);

-- =========================
-- 4. Pipeline control tables
-- =========================

create table if not exists sync_state (
  stream_name      text primary key,
  cursor_json      jsonb not null default '{}'::jsonb,
  last_success_at  timestamptz,
  last_attempt_at  timestamptz,
  last_error       text,
  updated_at       timestamptz not null default now()
);

create table if not exists ingest_runs (
  run_id           uuid primary key default gen_random_uuid(),
  stream_name      text not null,
  job_name         text not null,
  status           text not null,          -- started, success, failed
  started_at       timestamptz not null default now(),
  finished_at      timestamptz,
  rows_written     int default 0,
  error_text       text,
  metadata         jsonb not null default '{}'::jsonb
);

create index if not exists idx_ingest_runs_stream_started
  on ingest_runs(stream_name, started_at desc);

create table if not exists raw_ingest (
  raw_id           uuid primary key default gen_random_uuid(),
  source           text not null,          -- gamma, clob, data_api, subgraph_*
  endpoint         text not null,
  natural_key      text,
  payload          jsonb not null,
  fetched_at       timestamptz not null default now()
);

create index if not exists idx_raw_ingest_source_time
  on raw_ingest(source, fetched_at desc);
