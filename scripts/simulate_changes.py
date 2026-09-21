#!/usr/bin/env python3
"""Continuously applies a mix of inserts, updates and deletes to Postgres.

Every operation is appended to a local log (default sim_ops.log) with a timestamp.
Statements run as separate autocommit transactions, batched into one psql call per second.
Survives Postgres restarts: failed batches are logged and the id set is re-synced.

Usage: scripts/simulate_changes.py [--rate 20] [--seconds 120] [--log sim_ops.log] [--seed N]
"""
import argparse
import random
import subprocess
import time
from datetime import datetime, timezone

ACCOUNTS = ["acct-1001", "acct-1002", "acct-1003"]
REGIONS = ["us-east-1", "eu-west-1"]
TYPES = ["ec2", "rds", "ebs"]
SIZES = ["t3.micro", "m5.large", "c5.xlarge", "db.m5.large", "100GB", None]
TEAMS = ["platform", "data", "payments", None]      # None -> untagged resource (NULL)
STATES = ["running", "stopped", "terminated"]


def psql(container, sql, capture=False):
    cmd = ["docker", "exec", "-i", container, "psql", "-U", "postgres", "-d", "inventory",
           "-q", "-tA", "-v", "ON_ERROR_STOP=1"]
    return subprocess.run(cmd, input=sql, text=True, capture_output=True, check=True)


def lit(v):
    return "NULL" if v is None else "'" + str(v) + "'"


def cost():
    return "NULL" if random.random() < 0.05 else f"{random.uniform(0.01, 3):.4f}"


def load_ids(container):
    out = psql(container, "SELECT resource_id FROM resource_inventory").stdout
    return set(out.split())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=int, default=20, help="changes per second")
    ap.add_argument("--seconds", type=int, default=120)
    ap.add_argument("--log", default="sim_ops.log")
    ap.add_argument("--container", default="cdc-postgres")
    ap.add_argument("--seed", type=int, default=None)
    a = ap.parse_args()
    if a.seed is not None:
        random.seed(a.seed)

    ids = load_ids(a.container)
    run_id = format(random.SystemRandom().getrandbits(24), "06x")   # unique per run: ids never collide across runs
    counter = 0
    total = {"insert": 0, "update": 0, "delete": 0}
    end = time.time() + a.seconds
    with open(a.log, "a") as log:
        while time.time() < end:
            t0 = time.time()
            stmts, ops = [], []
            for _ in range(a.rate):
                r = random.random()
                if r < 0.25 or not ids:
                    counter += 1
                    rid = f"sim-{run_id}-{counter}"
                    stmts.append(
                        "INSERT INTO resource_inventory (resource_id,account_id,region,resource_type,"
                        "instance_size,team,state,hourly_cost) VALUES "
                        f"({lit(rid)},{lit(random.choice(ACCOUNTS))},{lit(random.choice(REGIONS))},"
                        f"{lit(random.choice(TYPES))},{lit(random.choice(SIZES))},{lit(random.choice(TEAMS))},"
                        f"{lit(random.choice(STATES))},{cost()});")
                    ops.append(("insert", rid))
                elif r < 0.85:
                    rid = random.choice(tuple(ids))
                    stmts.append(
                        f"UPDATE resource_inventory SET state={lit(random.choice(STATES))}, "
                        f"team={lit(random.choice(TEAMS))}, hourly_cost={cost()}, updated_at=now() "
                        f"WHERE resource_id={lit(rid)};")
                    ops.append(("update", rid))
                else:
                    rid = random.choice(tuple(ids))
                    stmts.append(f"DELETE FROM resource_inventory WHERE resource_id={lit(rid)};")
                    ops.append(("delete", rid))
            ts = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            try:
                psql(a.container, "\n".join(stmts))
                for kind, rid in ops:
                    log.write(f"{ts} {kind} {rid}\n")
                    total[kind] += 1
                    if kind == "insert":
                        ids.add(rid)
                    elif kind == "delete":
                        ids.discard(rid)
                log.flush()
            except subprocess.CalledProcessError as e:
                log.write(f"{ts} ERROR batch failed (postgres unavailable?): {e.stderr.strip()[:120]}\n")
                log.flush()
                try:
                    ids = load_ids(a.container)   # unknown how much of the batch applied
                except subprocess.CalledProcessError:
                    pass
            time.sleep(max(0, 1 - (time.time() - t0)))
    print(f"done: {total}")


if __name__ == "__main__":
    main()
