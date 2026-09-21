#!/usr/bin/env bash
# Compares Postgres against ClickHouse's current-state view with identical aggregates.
# Exits 0 on match, 1 on mismatch. Usage: scripts/verify.sh [--wait SECONDS]
# --wait retries until the two sides match (lets the pipeline drain) or the time runs out.
set -uo pipefail

PG="${PG_CONTAINER:-cdc-postgres}"
CH_URL="${CH_URL:-localhost:8123}"
CH_AUTH="user=default&password=clickhouse"
WAIT=0
[ "${1:-}" = "--wait" ] && WAIT="${2:-60}"

norm() { awk -F'|' '{printf "%s|%s|%.4f\n", $1, $2, $3}' | sort; }

postgres_agg() {
  docker exec "$PG" psql -U postgres -d inventory -tA -F'|' -c "
    SELECT 'team:'||coalesce(team,'<null>'), count(*), coalesce(sum(hourly_cost),0)
      FROM resource_inventory GROUP BY 1
    UNION ALL
    SELECT 'state:'||state, count(*), coalesce(sum(hourly_cost),0)
      FROM resource_inventory GROUP BY 1
    UNION ALL
    SELECT 'total', count(*), coalesce(sum(hourly_cost),0) FROM resource_inventory" | norm
}

clickhouse_agg() {
  curl -s "$CH_URL/?$CH_AUTH" --data-binary "
    SELECT 'team:'||ifNull(team,'<null>'), count(), ifNull(sum(hourly_cost),0)
      FROM cdc.resource_inventory_current GROUP BY 1
    UNION ALL
    SELECT 'state:'||state, count(), ifNull(sum(hourly_cost),0)
      FROM cdc.resource_inventory_current GROUP BY 1
    UNION ALL
    SELECT 'total', count(), ifNull(sum(hourly_cost),0) FROM cdc.resource_inventory_current
    FORMAT TSV" | tr '\t' '|' | norm
}

deadline=$((SECONDS + WAIT))
while true; do
  P=$(postgres_agg); C=$(clickhouse_agg)
  if [ "$P" = "$C" ] && [ -n "$P" ]; then
    echo "MATCH"; echo "$P" | sed 's/^/  /'
    dups=$(curl -s "$CH_URL/?$CH_AUTH" --data-binary "SELECT count() - uniqExact(resource_id, _version) FROM cdc.resource_inventory")
    raw=$(curl -s "$CH_URL/?$CH_AUTH" --data-binary "SELECT count() FROM cdc.resource_inventory")
    echo "raw rows: $raw, exact duplicates from replay: $dups"
    exit 0
  fi
  [ $SECONDS -ge $deadline ] && break
  sleep 3
done
echo "MISMATCH (postgres left, clickhouse right)"
diff <(echo "$P") <(echo "$C") | sed 's/^/  /'
exit 1
