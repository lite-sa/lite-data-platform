"""Insert new merchants/business_entities into the local Postgres stand-in,
and optionally flip an existing merchant's status, to exercise
ingest_merchants.py / ingest_business_entities.py's full "replace"
disposition -- unlike incremental, replace should pick up in-place updates
too (an existing row changing), not just new rows, since it re-extracts the
whole table every run.

Usage:
    uv run --project apps/app_etl python scripts/local_source_test/seed_merchant_change.py
    uv run --project apps/app_etl python scripts/local_source_test/seed_merchant_change.py --count 2 --flip-status merch_0001
"""

from __future__ import annotations

import argparse
import uuid

import psycopg

from _common import load_env, psycopg_dsn


def run(count: int, flip_status: str | None) -> None:
    conn = psycopg.connect(psycopg_dsn(load_env()))
    with conn, conn.cursor() as cur:
        for _ in range(count):
            merchant_id = f"merch_{uuid.uuid4().hex[:8]}"
            internal_id = f"mch_{uuid.uuid4().hex[:8]}"
            be_id = f"be_{uuid.uuid4().hex[:8]}"
            cur.execute(
                """
                INSERT INTO "user".merchants (id, merchant_id, status, created_at, updated_at)
                VALUES (%s, %s, 'ACTIVE', now(), now())
                """,
                (internal_id, merchant_id),
            )
            cur.execute(
                """
                INSERT INTO business_management.business_entities
                    (id, business_id, status, cr_number, name, in_liquidation_process,
                     has_ecommerce, created_at, updated_at)
                VALUES (%s, %s, 'ACTIVE', %s, %s, false, false, now(), now())
                """,
                (be_id, merchant_id, f"30{uuid.uuid4().int % 10**8:08d}", f"Seed Co {merchant_id}"),
            )
            print(f"Inserted merchant {merchant_id} ({internal_id}) + business entity {be_id}")

        if flip_status:
            cur.execute(
                """
                UPDATE "user".merchants
                SET status = CASE WHEN status = 'ACTIVE' THEN 'SUSPENDED' ELSE 'ACTIVE' END,
                    updated_at = now()
                WHERE merchant_id = %s
                RETURNING status
                """,
                (flip_status,),
            )
            row = cur.fetchone()
            if row is None:
                raise SystemExit(f"No merchant with merchant_id={flip_status!r} found")
            print(f"Flipped {flip_status} status -> {row[0]}")
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--count", type=int, default=1, help="how many new merchants (+ business entities) to insert (default 1)"
    )
    parser.add_argument(
        "--flip-status",
        metavar="MERCHANT_ID",
        default=None,
        help="also toggle this merchant's status (e.g. merch_0001) between ACTIVE/SUSPENDED, "
        "to prove replace picks up in-place updates, not just new rows",
    )
    args = parser.parse_args()
    run(args.count, args.flip_status)


if __name__ == "__main__":
    main()
