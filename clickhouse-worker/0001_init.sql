-- Beta clean-slate ClickHouse schema.
--
-- Archive ledgers, clearing analytics, history tables, PnL aggregates,
-- equity snapshots, and leaderboard serving share one initialization file
-- because the beta deployment is being reset. Do not replace an incremental
-- production migration history with this file on a database that already
-- contains data.

CREATE DATABASE IF NOT EXISTS if_market_logs;

-- The opaque matching-engine Archive ledgers (inputs, responses, snapshots,
-- quarantines) moved to the S3 archive tape owned by s3-worker. ClickHouse
-- holds only queryable projections.

-- BEGIN CLEARING ANALYTICS

CREATE TABLE IF NOT EXISTS if_market_logs.clearing_analytics_batches
(
    kafka_offset Int64,
    kafka_partition UInt8,
    archive_recording_id Int64,
    source_position Int64,
    processed_at DateTime64(3, 'UTC'),
    fill_event_index Array(UInt16),
    fill_index Array(UInt16),
    fill_role Array(LowCardinality(String)),
    fill_trade_id Array(String),
    fill_user_id Array(UInt64),
    fill_order_id Array(UInt64),
    fill_market_id Array(UInt64),
    fill_outcome_id Array(UInt64),
    fill_side Array(LowCardinality(String)),
    fill_position_effect Array(LowCardinality(String)),
    fill_pnl_type Array(LowCardinality(String)),
    fill_price Array(UInt64),
    fill_qty Array(UInt64),
    fill_closed_qty Array(UInt64),
    fill_opened_qty Array(UInt64),
    fill_closed_pnl Array(Int128),
    fill_fee Array(Int128),
    fill_crossed Array(UInt8),
    order_user_id Array(UInt64),
    order_id Array(UInt64),
    order_client_id Array(UInt64),
    order_outcome_id Array(UInt64),
    order_market_id Array(UInt64),
    order_created_at Array(DateTime64(3, 'UTC')),
    order_completed_at Array(DateTime64(3, 'UTC')),
    order_side Array(LowCardinality(String)),
    order_kind Array(LowCardinality(String)),
    order_tif Array(LowCardinality(String)),
    order_reduce_only Array(UInt8),
    order_price Array(UInt64),
    order_original_qty Array(UInt64),
    order_filled_qty Array(UInt64),
    order_status Array(LowCardinality(String)),
    order_reject_reason Array(LowCardinality(String)),
    ledger_user_id Array(UInt64),
    ledger_oid Array(UInt64),
    ledger_action Array(LowCardinality(String)),
    ledger_amount Array(UInt64),
    open_order_id Array(UInt64),
    open_user_id Array(UInt64),
    open_client_id Array(UInt64),
    open_outcome_id Array(UInt64),
    open_market_id Array(UInt64),
    open_created_at Array(DateTime64(3, 'UTC')),
    open_side Array(LowCardinality(String)),
    open_kind Array(LowCardinality(String)),
    open_tif Array(LowCardinality(String)),
    open_reduce_only Array(UInt8),
    open_price Array(UInt64),
    open_original_qty Array(UInt64),
    open_filled_qty Array(UInt64),
    open_is_open Array(UInt8),
    batch_first_offset Int64,
    batch_last_offset Int64,
    CONSTRAINT fill_roles_valid CHECK arrayAll(
        value -> value IN ('maker', 'taker', 'settlement'),
        fill_role
    ),
    CONSTRAINT fill_position_effects_valid CHECK arrayAll(
        (role, effect) -> effect IN (
                'Open Long', 'Add Long', 'Close Long',
                'Open Short', 'Add Short', 'Close Short',
                'Close Long → Open Short', 'Close Short → Open Long'
            ) OR (role = 'settlement' AND effect = 'Settlement'),
        fill_role,
        fill_position_effect
    ),
    CONSTRAINT fill_pnl_types_valid CHECK arrayAll(
        value -> value IN ('none', 'conditional', 'realized'),
        fill_pnl_type
    ),
    CONSTRAINT fill_pnl_classification_consistent CHECK arrayAll(
        (kind, closed, amount) -> (kind = 'none' AND closed = 0 AND amount = 0)
            OR (kind != 'none' AND closed > 0),
        fill_pnl_type,
        fill_closed_qty,
        fill_closed_pnl
    )
)
-- ReplacingMergeTree keyed on kafka_offset makes staging idempotent at the
-- row level: a delayed insert that lands after its client-side timeout can
-- only produce a duplicate of an identical row, which background merges
-- collapse. The deduplication window remains the first line of defense for
-- exact block retries.
ENGINE = ReplacingMergeTree
ORDER BY kafka_offset
SETTINGS non_replicated_deduplication_window = 4096;

ALTER TABLE if_market_logs.clearing_analytics_batches
    ADD CONSTRAINT IF NOT EXISTS fill_roles_valid CHECK arrayAll(
        value -> value IN ('maker', 'taker', 'settlement'),
        fill_role
    );

ALTER TABLE if_market_logs.clearing_analytics_batches
    ADD CONSTRAINT IF NOT EXISTS fill_position_effects_valid CHECK arrayAll(
        (role, effect) -> effect IN (
                'Open Long', 'Add Long', 'Close Long',
                'Open Short', 'Add Short', 'Close Short',
                'Close Long → Open Short', 'Close Short → Open Long'
            ) OR (role = 'settlement' AND effect = 'Settlement'),
        fill_role,
        fill_position_effect
    );

-- Progress marker for the worker-driven history fan-out. A staging range
-- [first_offset, last_offset] appears here only after every normalized
-- history table has acknowledged that range, so on restart the worker
-- re-projects everything after max(last_offset) before serving reads.
CREATE TABLE IF NOT EXISTS if_market_logs.clearing_analytics_projection_commits
(
    first_offset Int64,
    last_offset Int64,
    committed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY last_offset
SETTINGS non_replicated_deduplication_window = 4096;

-- Evidence locker for undecodable clearing records. The worker writes the
-- raw record here and then stops fatally: a malformed record on this topic
-- is an upstream incident, and skipping it would leave a silent gap in
-- financial history. ReplacingMergeTree keyed on kafka_offset keeps the
-- table clean across restart loops that re-encounter the same record.
CREATE TABLE IF NOT EXISTS if_market_logs.clearing_analytics_quarantine
(
    kafka_offset Int64,
    kafka_partition UInt8,
    kafka_key String,
    payload String,
    error String,
    quarantined_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY kafka_offset
SETTINGS non_replicated_deduplication_window = 1024;

CREATE TABLE IF NOT EXISTS if_market_logs.user_fills
(
    kafka_offset Int64,
    source_position Int64,
    event_index UInt16,
    fill_index UInt16,
    role LowCardinality(String),
    trade_id String,
    user_id UInt64,
    order_id UInt64,
    market_id UInt64,
    outcome_id UInt64,
    processed_at DateTime64(3, 'UTC'),
    side LowCardinality(String),
    position_effect LowCardinality(String),
    pnl_type LowCardinality(String),
    price UInt64,
    qty UInt64,
    closed_qty UInt64,
    opened_qty UInt64,
    closed_pnl Int128,
    fee Int128,
    realized_pnl Int128 MATERIALIZED if(pnl_type = 'realized', closed_pnl, 0),
    conditional_pnl Int128 MATERIALIZED if(pnl_type = 'conditional', closed_pnl, 0),
    net_realized_pnl Int128 MATERIALIZED realized_pnl - fee,
    volume UInt128 MATERIALIZED if(role = 'settlement', 0, toUInt128(price) * toUInt128(qty) / 1000000),
    crossed UInt8,
    CONSTRAINT role_valid CHECK role IN ('maker', 'taker', 'settlement'),
    CONSTRAINT position_effect_valid CHECK
        position_effect IN (
            'Open Long', 'Add Long', 'Close Long',
            'Open Short', 'Add Short', 'Close Short',
            'Close Long → Open Short', 'Close Short → Open Long'
        ) OR (role = 'settlement' AND position_effect = 'Settlement'),
    CONSTRAINT pnl_type_valid CHECK pnl_type IN ('none', 'conditional', 'realized'),
    CONSTRAINT pnl_classification_consistent CHECK
        (pnl_type = 'none' AND closed_qty = 0 AND closed_pnl = 0)
        OR (pnl_type != 'none' AND closed_qty > 0)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (user_id, processed_at, source_position, event_index, fill_index, role)
SETTINGS non_replicated_deduplication_window = 4096;

ALTER TABLE if_market_logs.user_fills
    ADD CONSTRAINT IF NOT EXISTS role_valid
    CHECK role IN ('maker', 'taker', 'settlement');

ALTER TABLE if_market_logs.user_fills
    ADD CONSTRAINT IF NOT EXISTS position_effect_valid CHECK
        position_effect IN (
            'Open Long', 'Add Long', 'Close Long',
            'Open Short', 'Add Short', 'Close Short',
            'Close Long → Open Short', 'Close Short → Open Long'
        ) OR (role = 'settlement' AND position_effect = 'Settlement');

CREATE TABLE IF NOT EXISTS if_market_logs.user_order_history
(
    kafka_offset Int64,
    source_position Int64,
    user_id UInt64,
    order_id UInt64,
    client_order_id UInt64,
    outcome_id UInt64,
    market_id UInt64,
    created_at DateTime64(3, 'UTC'),
    completed_at DateTime64(3, 'UTC'),
    side LowCardinality(String),
    kind LowCardinality(String),
    tif LowCardinality(String),
    reduce_only UInt8,
    price UInt64,
    original_qty UInt64,
    filled_qty UInt64,
    status LowCardinality(String),
    reject_reason LowCardinality(String)
)
ENGINE = ReplacingMergeTree(kafka_offset)
PARTITION BY toYYYYMM(created_at)
ORDER BY (user_id, order_id)
SETTINGS non_replicated_deduplication_window = 4096;

CREATE TABLE IF NOT EXISTS if_market_logs.user_ledger_events
(
    kafka_offset Int64,
    source_position Int64,
    user_id UInt64,
    oid UInt64,
    action LowCardinality(String),
    amount UInt64,
    processed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (user_id, processed_at, source_position)
SETTINGS non_replicated_deduplication_window = 4096;

CREATE TABLE IF NOT EXISTS if_market_logs.public_trades
(
    kafka_offset Int64,
    source_position Int64,
    event_index UInt16,
    fill_index UInt16,
    outcome_id UInt64,
    trade_id String,
    taker_side LowCardinality(String),
    price UInt64,
    qty UInt64,
    processed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (outcome_id, processed_at, source_position, event_index, fill_index)
SETTINGS non_replicated_deduplication_window = 4096;

-- Incremental public-volume aggregates. Ten-minute buckets retain enough
-- granularity for a trailing-24h window without rescanning the immutable trade
-- tape. Daily buckets retain all history at bounded cardinality. The serving
-- refresh below combines complete daily buckets with today's ten-minute
-- buckets so all-time and 24h values share the same hour-aligned upper bound.
CREATE MATERIALIZED VIEW IF NOT EXISTS if_market_logs.outcome_volume_10m
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (bucket_start, outcome_id)
TTL bucket_start + INTERVAL 2 DAY DELETE
SETTINGS non_replicated_deduplication_window = 4096
POPULATE
AS
SELECT
    toStartOfInterval(processed_at, INTERVAL 10 MINUTE) AS bucket_start,
    outcome_id,
    sum(intDiv(toUInt128(price) * toUInt128(qty), toUInt128(1000000))) AS volume
FROM if_market_logs.public_trades
WHERE processed_at >= now('UTC') - INTERVAL 2 DAY
GROUP BY bucket_start, outcome_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS if_market_logs.outcome_volume_daily
ENGINE = SummingMergeTree
PARTITION BY toYear(bucket_start)
ORDER BY (bucket_start, outcome_id)
SETTINGS non_replicated_deduplication_window = 4096
POPULATE
AS
SELECT
    toStartOfDay(processed_at) AS bucket_start,
    outcome_id,
    sum(intDiv(toUInt128(price) * toUInt128(qty), toUInt128(1000000))) AS volume
FROM if_market_logs.public_trades
GROUP BY bucket_start, outcome_id;

-- One atomic, hour-aligned serving generation for both published periods.
-- Marker rows (outcome_id = 0) preserve `as_of` when a period has no trades;
-- public outcome ids are strictly greater than zero.
CREATE TABLE IF NOT EXISTS if_market_logs.outcome_volume_cache
(
    period LowCardinality(String),
    as_of DateTime('UTC'),
    outcome_id UInt64,
    volume UInt128
)
ENGINE = MergeTree
ORDER BY (period, outcome_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS if_market_logs.outcome_volume_cache_refresh
REFRESH EVERY 1 HOUR OFFSET 2 MINUTE
TO if_market_logs.outcome_volume_cache
AS
WITH toStartOfHour(now('UTC')) AS snapshot_as_of
SELECT
    '24h' AS period,
    snapshot_as_of AS as_of,
    outcome_id,
    sum(volume) AS volume
FROM if_market_logs.outcome_volume_10m
WHERE bucket_start >= snapshot_as_of - INTERVAL 1 DAY
  AND bucket_start < snapshot_as_of
GROUP BY outcome_id
UNION ALL
SELECT
    'allTime' AS period,
    snapshot_as_of AS as_of,
    outcome_id,
    sum(volume) AS volume
FROM
(
    SELECT outcome_id, volume
    FROM if_market_logs.outcome_volume_daily
    WHERE bucket_start < toStartOfDay(snapshot_as_of)
    UNION ALL
    SELECT outcome_id, volume
    FROM if_market_logs.outcome_volume_10m
    WHERE bucket_start >= toStartOfDay(snapshot_as_of)
      AND bucket_start < snapshot_as_of
)
GROUP BY outcome_id
UNION ALL
SELECT
    period,
    snapshot_as_of AS as_of,
    toUInt64(0) AS outcome_id,
    toUInt128(0) AS volume
FROM (SELECT arrayJoin(['24h', 'allTime']) AS period);

CREATE TABLE IF NOT EXISTS if_market_logs.ingest_open_orders
(
    order_id UInt64,
    user_id UInt64,
    client_order_id UInt64,
    outcome_id UInt64,
    market_id UInt64,
    created_at DateTime64(3, 'UTC'),
    side LowCardinality(String),
    kind LowCardinality(String),
    tif LowCardinality(String),
    reduce_only UInt8,
    price UInt64,
    original_qty UInt64,
    filled_qty UInt64,
    is_open UInt8,
    kafka_offset Int64
)
ENGINE = ReplacingMergeTree(kafka_offset)
ORDER BY order_id
SETTINGS non_replicated_deduplication_window = 4096;

-- The legacy physical name is retained; buckets are 15 minutes wide after
-- upgrade so every quarter-hour generation ranks a freshly closed bucket.
CREATE TABLE IF NOT EXISTS if_market_logs.user_metrics_6h
(
    user_id UInt64,
    bucket_start DateTime('UTC'),
    realized_pnl Int128,
    conditional_pnl Int128,
    fee Int128,
    net_realized_pnl Int128,
    volume UInt128,
    fills UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(bucket_start)
ORDER BY (user_id, bucket_start)
SETTINGS non_replicated_deduplication_window = 4096;

CREATE MATERIALIZED VIEW IF NOT EXISTS if_market_logs.user_metrics_6h_mv
TO if_market_logs.user_metrics_6h
AS
SELECT
    user_id,
    toStartOfInterval(processed_at, INTERVAL 15 MINUTE) AS bucket_start,
    sum(realized_pnl) AS realized_pnl,
    sum(conditional_pnl) AS conditional_pnl,
    sum(fee) AS fee,
    sum(net_realized_pnl) AS net_realized_pnl,
    sum(volume) AS volume,
    countIf(role != 'settlement') AS fills
FROM if_market_logs.user_fills
GROUP BY user_id, bucket_start;

CREATE TABLE IF NOT EXISTS if_market_logs.account_equity_snapshots
(
    user_id UInt64,
    snapshot_hour DateTime('UTC'),
    captured_at DateTime64(3, 'UTC'),
    cadence_seconds UInt32 DEFAULT 0,
    equity UInt128,
    net_deposits Int128
)
ENGINE = ReplacingMergeTree(captured_at)
PARTITION BY toYYYYMM(snapshot_hour)
ORDER BY (user_id, snapshot_hour);

-- Serving table for rankings. A REPLACE-mode refreshable view atomically
-- swaps the whole table every 15 minutes, so the table always holds exactly
-- one consistent generation and needs no replacing/dedup machinery. Readers
-- derive the published asOf from max(as_of) here, which cannot race the swap.
CREATE TABLE IF NOT EXISTS if_market_logs.leaderboard_cache
(
    period LowCardinality(String),
    as_of DateTime('UTC'),
    rank UInt64,
    user_id UInt64,
    generated_at DateTime64(3, 'UTC'),
    participants UInt64,
    pnl Int128,
    volume UInt128,
    PROJECTION leaderboard_by_user
    (
        SELECT period, as_of, user_id, rank, participants, pnl, volume, generated_at
        ORDER BY (period, user_id, as_of)
    )
)
ENGINE = MergeTree
ORDER BY (period, as_of, rank, user_id);

-- Rankings use gross realized PnL only over the half-open window of complete
-- 15-minute buckets [asOf - period, asOf); the bucket that begins at asOf is
-- still accumulating and never enters a published generation. asOf is the
-- latest equity snapshot at or before the current quarter-hour, so each
-- refresh advances the window by one bucket when realtime captured on time.
-- The one-minute offset lets the snapshot insert and the worker's sub-second
-- insert buffering settle at each quarter-hour.
--
-- Every generation also writes one marker row per period (rank = 0,
-- user_id = 0). A quiet market would otherwise swap the table to empty, and
-- readers could not tell "refresh ran, nobody traded" from "refresh is
-- broken": max(as_of) always reflects the latest completed generation.
-- Ranked reads filter rank > 0.
CREATE MATERIALIZED VIEW IF NOT EXISTS if_market_logs.leaderboard_cache_refresh
REFRESH EVERY 15 MINUTE OFFSET 1 MINUTE
TO if_market_logs.leaderboard_cache
AS
WITH ifNull((
    SELECT max(snapshot_hour)
    FROM if_market_logs.account_equity_snapshots
    WHERE snapshot_hour <= toStartOfInterval(now('UTC'), INTERVAL 15 MINUTE)
), toDateTime(0, 'UTC')) AS generation_as_of
SELECT
    period,
    generation_as_of AS as_of,
    row_number() OVER (PARTITION BY period ORDER BY pnl DESC, volume DESC, user_id ASC) AS rank,
    user_id,
    now64(3, 'UTC') AS generated_at,
    count() OVER (PARTITION BY period) AS participants,
    pnl,
    volume
FROM
(
    SELECT '24h' AS period, user_id, sum(realized_pnl) AS pnl, sum(volume) AS volume
    FROM if_market_logs.user_metrics_6h
    WHERE bucket_start >= generation_as_of - INTERVAL 1 DAY
      AND bucket_start < generation_as_of
    GROUP BY user_id
    UNION ALL
    SELECT '7d' AS period, user_id, sum(realized_pnl) AS pnl, sum(volume) AS volume
    FROM if_market_logs.user_metrics_6h
    WHERE bucket_start >= generation_as_of - INTERVAL 7 DAY
      AND bucket_start < generation_as_of
    GROUP BY user_id
    UNION ALL
    SELECT '30d' AS period, user_id, sum(realized_pnl) AS pnl, sum(volume) AS volume
    FROM if_market_logs.user_metrics_6h
    WHERE bucket_start >= generation_as_of - INTERVAL 30 DAY
      AND bucket_start < generation_as_of
    GROUP BY user_id
    UNION ALL
    SELECT 'all' AS period, user_id, sum(realized_pnl) AS pnl, sum(volume) AS volume
    FROM if_market_logs.user_metrics_6h
    WHERE bucket_start < generation_as_of
    GROUP BY user_id
)
UNION ALL
SELECT
    period,
    generation_as_of AS as_of,
    toUInt64(0) AS rank,
    toUInt64(0) AS user_id,
    now64(3, 'UTC') AS generated_at,
    toUInt64(0) AS participants,
    toInt128(0) AS pnl,
    toUInt128(0) AS volume
FROM (SELECT arrayJoin(['24h', '7d', '30d', 'all']) AS period);

-- BEGIN SPOT HISTORY

CREATE TABLE IF NOT EXISTS if_market_logs.spot_price_updates
(
    kafka_offset Int64,
    kafka_timestamp DateTime64(3, 'UTC'),
    archive_recording_id Int64,
    archive_message_end_position Int64,
    archive_stream_id Int32,
    producer_instance FixedString(16),
    producer_sequence UInt64,
    observed_at DateTime64(3, 'UTC'),
    asset_id UInt64,
    available UInt8,
    price UInt64,
    source_mask UInt8,
    contributor_count UInt8,
    apply_status LowCardinality(String),
    payload String,
    batch_first_offset Int64,
    batch_last_offset Int64,
    CONSTRAINT spot_apply_status_valid CHECK apply_status IN
        ('applied_available', 'applied_unavailable', 'duplicate', 'late', 'sequence_gap', 'split_brain')
)
ENGINE = ReplacingMergeTree(kafka_offset)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (asset_id, observed_at, kafka_offset)
SETTINGS non_replicated_deduplication_window = 4096;

CREATE TABLE IF NOT EXISTS if_market_logs.spot_candles
(
    kafka_offset Int64,
    asset_id UInt64,
    interval LowCardinality(String),
    start_time DateTime64(3, 'UTC'),
    close_time DateTime64(3, 'UTC'),
    open UInt64,
    close UInt64,
    high UInt64,
    low UInt64,
    volume UInt64,
    trades UInt64,
    CONSTRAINT spot_interval_valid CHECK interval IN ('1m', '5m', '15m', '1h', '4h', '1d')
)
ENGINE = ReplacingMergeTree(kafka_offset)
PARTITION BY toYYYYMM(start_time)
ORDER BY (asset_id, interval, start_time)
SETTINGS non_replicated_deduplication_window = 4096;

CREATE TABLE IF NOT EXISTS if_market_logs.outcome_candles
(
    kafka_offset Int64,
    outcome_id UInt64,
    interval LowCardinality(String),
    start_time DateTime64(3, 'UTC'),
    close_time DateTime64(3, 'UTC'),
    open UInt64,
    close UInt64,
    high UInt64,
    low UInt64,
    volume UInt128,
    trades UInt64,
    CONSTRAINT outcome_interval_valid CHECK interval IN ('1m', '5m', '15m', '1h', '4h', '1d')
)
ENGINE = ReplacingMergeTree(kafka_offset)
PARTITION BY toYYYYMM(start_time)
ORDER BY (outcome_id, interval, start_time)
SETTINGS non_replicated_deduplication_window = 4096;

-- Kafka consumer-group commits now own spot projection progress and retained
-- Kafka records own integrity evidence. Remove the obsolete local metadata
-- tables when upgrading an environment that briefly ran the older design.
DROP TABLE IF EXISTS if_market_logs.spot_projection_commits;
DROP TABLE IF EXISTS if_market_logs.spot_analytics_quarantine;
