create or replace view v_market_catalog as
select
  m.condition_id,
  m.slug as market_slug,
  m.question,
  m.active,
  m.closed,
  m.resolved,
  e.event_id,
  e.slug as event_slug,
  e.title as event_title
from markets m
left join events e on e.event_id = m.event_id;

create or replace view v_latest_prices as
select distinct on (token_id)
  token_id, ts, price, granularity
from price_history
order by token_id, ts desc;

create or replace view v_market_latest_prices as
select
  mt.condition_id,
  mt.token_id,
  mt.outcome_name,
  lp.ts,
  lp.price
from market_tokens mt
join v_latest_prices lp on lp.token_id = mt.token_id;
