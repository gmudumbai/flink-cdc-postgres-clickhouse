-- Stretch exercise: two more tables, deliberately different shapes from resource_inventory
-- (booleans, integers, two independent timestamps, different nullability), sharing ONE
-- publication and ONE replication slot -- proving a slot serves many tables, not one-per-table.

CREATE TABLE network_interfaces (
  interface_id   TEXT PRIMARY KEY,
  resource_id    TEXT NOT NULL,
  mac_address    TEXT NOT NULL,
  private_ip     TEXT NOT NULL,
  public_ip      TEXT,                 -- nullable: not every interface has one
  is_primary     BOOLEAN NOT NULL DEFAULT false,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE network_interfaces REPLICA IDENTITY FULL;

CREATE TABLE snapshot_jobs (
  snapshot_id    TEXT PRIMARY KEY,
  resource_id    TEXT NOT NULL,
  snapshot_type  TEXT NOT NULL,        -- 'full', 'incremental'
  size_gb        INTEGER,              -- nullable: unknown until the job completes
  status         TEXT NOT NULL,        -- 'running', 'completed', 'failed'
  started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at   TIMESTAMPTZ           -- nullable: not done yet
);
ALTER TABLE snapshot_jobs REPLICA IDENTITY FULL;

-- Separate publication and slot from flink_pub/flink_slot: total isolation from the
-- Phase 1-6 pipeline. cdc_heartbeat is reused (a table can belong to more than one
-- publication) so this group gets its own heartbeat cadence independent of the other job.
CREATE PUBLICATION flink_pub_multi FOR TABLE network_interfaces, snapshot_jobs, cdc_heartbeat;

GRANT SELECT ON network_interfaces, snapshot_jobs TO cdc_user;

-- Seed data, deliberately including NULLs (public_ip, size_gb, completed_at).
INSERT INTO network_interfaces (interface_id, resource_id, mac_address, private_ip, public_ip, is_primary)
SELECT
  'eni-' || lpad(g::text, 4, '0'),
  'res-' || lpad(((g - 1) % 200 + 1)::text, 4, '0'),
  lpad(to_hex(g), 12, '0'),
  '10.0.' || (g % 256) || '.' || ((g * 7) % 256),
  CASE WHEN g % 3 = 0 THEN NULL ELSE format('54.%s.%s.%s', g % 256, (g*3) % 256, (g*5) % 256) END,
  (g % 4 = 0)
FROM generate_series(1, 60) AS g;

INSERT INTO snapshot_jobs (snapshot_id, resource_id, snapshot_type, size_gb, status, completed_at)
SELECT
  'snap-' || lpad(g::text, 4, '0'),
  'res-' || lpad(((g - 1) % 200 + 1)::text, 4, '0'),
  (ARRAY['full','incremental'])[1 + g % 2],
  CASE WHEN g % 5 = 0 THEN NULL ELSE 10 + (g % 200) END,
  (ARRAY['completed','completed','completed','failed','running'])[1 + g % 5],
  CASE WHEN g % 5 = 0 THEN NULL ELSE now() - (g || ' minutes')::interval END
FROM generate_series(1, 40) AS g;
