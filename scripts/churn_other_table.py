#!/usr/bin/env python3
"""Writes a few hundred rows/sec to unrelated_writes (NOT in the publication).

Used for the heartbeat demo: WAL grows from this table, but the CDC consumer
never sees it, so only the heartbeat can advance the replication slot.
Usage: scripts/churn_other_table.py [--seconds 120] [--rate 300]
"""
import argparse
import subprocess
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=120)
    ap.add_argument("--rate", type=int, default=300, help="rows per second")
    ap.add_argument("--container", default="cdc-postgres")
    a = ap.parse_args()

    end = time.time() + a.seconds
    total = 0
    while time.time() < end:
        t0 = time.time()
        sql = ("INSERT INTO unrelated_writes (payload) "
               f"SELECT repeat(md5(random()::text), 8) FROM generate_series(1, {a.rate});")
        subprocess.run(["docker", "exec", a.container, "psql", "-U", "postgres",
                        "-d", "inventory", "-qc", sql], check=True)
        total += a.rate
        time.sleep(max(0, 1 - (time.time() - t0)))
    print(f"inserted {total} rows into unrelated_writes")


if __name__ == "__main__":
    main()
