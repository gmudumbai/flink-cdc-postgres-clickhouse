CREATE DATABASE IF NOT EXISTS cdc;

-- Analytical mirror of postgres resource_inventory.
-- Append-only: the sink only ever INSERTs. An update is a new row with a higher _version;
-- a delete is a new row with _is_deleted = 1. ReplacingMergeTree keeps the highest _version
-- per resource_id and drops deleted rows, but only when parts merge (use FINAL to be exact).
-- Nullability mirrors Postgres: team, instance_size and hourly_cost are nullable in the source
-- (untagged resources, resource types with no instance size, cost not yet known).
CREATE TABLE cdc.resource_inventory (
  resource_id    String,
  account_id     String,
  region         String,
  resource_type  String,
  instance_size  Nullable(String),
  team           Nullable(String),
  state          String,
  hourly_cost    Nullable(Decimal(10,4)),
  updated_at     DateTime64(3),
  _version       UInt64,     -- Postgres LSN; 0 for snapshot rows so any real change wins
  _is_deleted    UInt8
) ENGINE = ReplacingMergeTree(_version, _is_deleted)
ORDER BY resource_id;

-- Current state: latest version per key, tombstones removed.
CREATE VIEW cdc.resource_inventory_current AS
SELECT * FROM cdc.resource_inventory FINAL WHERE _is_deleted = 0;
