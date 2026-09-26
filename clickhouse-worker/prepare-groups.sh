#!/bin/sh
# Metadata-only preparation. No data copy, offset reset, table drop or activation.
set -eu
host="$(echo "$CLICKHOUSE_WORKER_CLICKHOUSE_URL" | sed -E 's#^https?://([^:/]+).*#\1#')"
test "$host" = clickhouse-clickhouse-0-0-0.clickhouse-clickhouse-headless.clickhouse.svc.cluster.local
test "$CLICKHOUSE_WORKER_CLICKHOUSE_DATABASE" = if_market_logs

query() {
  clickhouse-client --host "$host" --port 9000 \
    --user "$CLICKHOUSE_WORKER_CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_WORKER_CLICKHOUSE_PASSWORD" "$@"
}

# CREATE IF NOT EXISTS cannot update existing MV destination settings. Resolve
# their actual UUID-backed inner tables; never hard-code UUIDs from one cluster.
targets="$(query --query "SELECT concat('.inner_id.', toString(uuid)) FROM system.tables WHERE database='if_market_logs' AND engine='MaterializedView' AND name IN ('outcome_volume_10m','outcome_volume_daily') ORDER BY name FORMAT TSVRaw")"
test "$(printf '%s\n' "$targets" | wc -l | tr -d ' ')" = 2
for target in $targets; do
  # Metadata-derived identifiers must match the exact expected UUID form.
  printf '%s\n' "$target" | grep -Eq '^\.inner_id\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  engine="$(query --query "SELECT engine FROM system.tables WHERE database='if_market_logs' AND name='$target' FORMAT TSVRaw")"
  test "$engine" = SummingMergeTree
  # Do not lower a larger window set by a subsequent tuning change.
  window="$(query --query "SELECT if(extract(engine_full, '(?:SETTINGS |, )non_replicated_deduplication_window = ([0-9]+)') = '', (SELECT value FROM system.merge_tree_settings WHERE name='non_replicated_deduplication_window'), extract(engine_full, '(?:SETTINGS |, )non_replicated_deduplication_window = ([0-9]+)')) FROM system.tables WHERE database='if_market_logs' AND name='$target' FORMAT TSVRaw")"
  if [ "$window" -lt 4096 ]; then
    if [ "${PREPARE_GROUPS_DRY_RUN:-0}" = 1 ]; then
      echo "would raise deduplication window: $target ($window -> 4096)"
      continue
    fi
    query --query "ALTER TABLE if_market_logs.\`$target\` MODIFY SETTING non_replicated_deduplication_window = 4096"
  fi
  echo "deduplication prepared: $target (window >= 4096)"
done
