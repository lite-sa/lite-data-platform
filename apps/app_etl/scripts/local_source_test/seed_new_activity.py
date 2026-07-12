"""Insert fresh payment_v2.payments/payment_operations rows into the local
Postgres stand-in, to exercise ingest_payments.py / ingest_payment_operations.py's
incremental (safety-lag) extraction across repeated runs.

Not idempotent on purpose -- run it, then run pipeline.py (or just the two
payment ingest files), and compare row counts / _dlt_pipeline_state before
and after in the notebook.

Usage:
    uv run --project apps/app_etl python scripts/local_source_test/seed_new_activity.py
    uv run --project apps/app_etl python scripts/local_source_test/seed_new_activity.py --count 5 --minutes-ago 15
"""

from __future__ import annotations

import argparse
import uuid

import psycopg

from _common import load_env, psycopg_dsn


def run(count: int, merchant_id: str, minutes_ago: float) -> None:
    seconds_ago = minutes_ago * 60
    conn = psycopg.connect(psycopg_dsn(load_env()))
    with conn, conn.cursor() as cur:
        for _ in range(count):
            payment_id = f"pay_{uuid.uuid4().hex[:12]}"
            cur.execute(
                """
                INSERT INTO payment_v2.payments
                    (id, merchant_id, amount, currency, status, payment_method,
                     channel_id, order_id, created_at, updated_at)
                VALUES
                    (%(id)s, %(merchant_id)s, %(amount)s, 'SAR', 'CAPTURED', 'CARD',
                     'default', %(order_id)s,
                     now() - make_interval(secs => %(seconds_ago)s),
                     now() - make_interval(secs => %(seconds_ago)s))
                """,
                {
                    "id": payment_id,
                    "merchant_id": merchant_id,
                    "amount": 1000 + (hash(payment_id) % 50_000),
                    "order_id": f"order_{payment_id}",
                    "seconds_ago": seconds_ago,
                },
            )
            cur.execute(
                """
                INSERT INTO payment_v2.payment_operations
                    (id, payment_id, operation_type, status, amount, currency,
                     created_at, updated_at)
                VALUES
                    (%(id)s, %(payment_id)s, 'AUTHORIZE', 'SUCCESS', %(amount)s, 'SAR',
                     now() - make_interval(secs => %(seconds_ago)s),
                     now() - make_interval(secs => %(seconds_ago)s))
                """,
                {
                    "id": f"op_{uuid.uuid4().hex[:12]}",
                    "payment_id": payment_id,
                    "amount": 1000,
                    "seconds_ago": seconds_ago,
                },
            )
    conn.close()
    print(
        f"Inserted {count} payment(s) + operation(s) for merchant {merchant_id}, "
        f"updated_at = now() - {minutes_ago} min"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--count", type=int, default=3, help="how many new payments to insert (default 3)")
    parser.add_argument(
        "--merchant-id", default="merch_0001", help="merchant_id to attribute new payments to"
    )
    parser.add_argument(
        "--minutes-ago",
        type=float,
        default=0.0,
        help=(
            "backdate created_at/updated_at by this many minutes (default 0 = right now). "
            "With the 10-minute SAFETY_LAG in _common.py, a run right after seeding with "
            "--minutes-ago 0 should NOT pick the row up yet -- rerun after 10 minutes (or "
            "temporarily lower SAFETY_LAG) to see it load. Pass e.g. --minutes-ago 15 to "
            "simulate a row already old enough to be outside the safety window."
        ),
    )
    args = parser.parse_args()
    run(args.count, args.merchant_id, args.minutes_ago)


if __name__ == "__main__":
    main()
