-- Operational cloud resource inventory: the source of truth that CDC reads from.
CREATE TABLE resource_inventory (
  resource_id   TEXT PRIMARY KEY,
  account_id    TEXT NOT NULL,
  region        TEXT NOT NULL,
  resource_type TEXT NOT NULL,     -- 'ec2', 'rds', 'ebs'
  instance_size TEXT,
  team          TEXT,
  state         TEXT NOT NULL,     -- 'running', 'stopped', 'terminated'
  hourly_cost   NUMERIC(10,4),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- UPDATE and DELETE events carry the full previous row, not just the primary key.
-- Costs extra WAL volume on every update; kept on so change events are legible.
ALTER TABLE resource_inventory REPLICA IDENTITY FULL;

-- Heartbeat: the Flink job updates this row so the replication slot keeps
-- advancing even when resource_inventory is quiet (used from Phase 1).
CREATE TABLE cdc_heartbeat (
  id INT PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL
);
INSERT INTO cdc_heartbeat VALUES (1, now());

-- Deliberately NOT published: demonstrates the quiet-table WAL retention problem.
CREATE TABLE unrelated_writes (
  id BIGSERIAL PRIMARY KEY,
  payload TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The heartbeat table must be published or its writes won't advance the slot.
CREATE PUBLICATION flink_pub FOR TABLE resource_inventory, cdc_heartbeat;

-- Dedicated CDC role instead of the superuser. The publication is pre-created
-- above precisely so cdc_user does not need CREATE on the database: the
-- least-privilege production pattern. Demo password only.
CREATE ROLE cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_pass';
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON resource_inventory, cdc_heartbeat TO cdc_user;
GRANT UPDATE ON cdc_heartbeat TO cdc_user;   -- heartbeat action query needs to write

-- Seed: 200 rows, 3 accounts, 3 teams, 2 regions, mixed types and states.
INSERT INTO resource_inventory
  (resource_id, account_id, region, resource_type, instance_size, team, state, hourly_cost)
SELECT
  'res-' || lpad(g::text, 4, '0'),
  (ARRAY['acct-1001','acct-1002','acct-1003'])[1 + g % 3],
  (ARRAY['us-east-1','eu-west-1'])[1 + g % 2],
  t.rtype,
  CASE t.rtype
    WHEN 'ec2' THEN (ARRAY['t3.micro','m5.large','c5.xlarge'])[1 + g % 3]
    WHEN 'rds' THEN (ARRAY['db.t3.medium','db.m5.large'])[1 + g % 2]
    ELSE (ARRAY['100GB','500GB'])[1 + g % 2]
  END,
  (ARRAY['platform','data','payments'])[1 + (g / 3) % 3],
  (ARRAY['running','running','running','stopped','terminated'])[1 + g % 5],
  round((0.01 + (g % 40) * 0.05)::numeric, 4)
FROM generate_series(1, 200) AS g
CROSS JOIN LATERAL (
  SELECT (ARRAY['ec2','ec2','rds','ebs'])[1 + g % 4] AS rtype
) AS t;
