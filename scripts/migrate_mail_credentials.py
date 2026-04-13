#!/usr/bin/env python3
import argparse
import os
import sys

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from shared.mail_credential_crypto import (
    encrypt_mail_secret,
    is_encrypted_mail_secret,
)


def load_default_env_files():
    from dotenv import load_dotenv

    for rel_path in ("list-admin/.env", "list/.env"):
        env_path = os.path.join(BASE_DIR, rel_path)
        if os.path.isfile(env_path):
            load_dotenv(env_path, override=False)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description=(
            "Encrypt legacy plaintext IMAP/SMTP passwords stored in mailing_lists."
        )
    )
    parser.add_argument(
        "--env-file",
        action="append",
        default=[],
        help="Additional .env file to load before connecting to PostgreSQL.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without writing changes.",
    )
    return parser.parse_args(argv)


def get_db_config():
    required = {
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASS": os.getenv("DB_PASS"),
    }
    missing = [name for name, value in required.items() if not (value or "").strip()]
    if missing:
        joined = ", ".join(missing)
        raise RuntimeError(f"Missing required database settings: {joined}")

    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "5432")),
        "dbname": required["DB_NAME"],
        "user": required["DB_USER"],
        "password": required["DB_PASS"],
    }


def iter_target_rows(cur):
    cur.execute(
        """
        SELECT id, name, address, imap_pass, smtp_pass
        FROM mailing_lists
        ORDER BY id ASC
        """
    )
    return cur.fetchall()


def plan_updates(rows):
    updates = []
    stats = {
        "rows_seen": 0,
        "rows_to_update": 0,
        "imap_encrypted": 0,
        "smtp_encrypted": 0,
        "rows_already_encrypted": 0,
        "rows_without_credentials": 0,
    }

    for row in rows:
        stats["rows_seen"] += 1

        imap_pass = row["imap_pass"]
        smtp_pass = row["smtp_pass"]
        new_imap_pass = imap_pass
        new_smtp_pass = smtp_pass

        if imap_pass not in (None, "") and not is_encrypted_mail_secret(imap_pass):
            new_imap_pass = encrypt_mail_secret(
                imap_pass, field_name=f"IMAP password for {row['address']}"
            )
            stats["imap_encrypted"] += 1

        if smtp_pass not in (None, "") and not is_encrypted_mail_secret(smtp_pass):
            new_smtp_pass = encrypt_mail_secret(
                smtp_pass, field_name=f"SMTP password for {row['address']}"
            )
            stats["smtp_encrypted"] += 1

        if imap_pass in (None, "") and smtp_pass in (None, ""):
            stats["rows_without_credentials"] += 1
            continue

        if new_imap_pass == imap_pass and new_smtp_pass == smtp_pass:
            stats["rows_already_encrypted"] += 1
            continue

        stats["rows_to_update"] += 1
        updates.append(
            {
                "id": row["id"],
                "address": row["address"],
                "imap_pass": new_imap_pass,
                "smtp_pass": new_smtp_pass,
            }
        )

    return updates, stats


def apply_updates(conn, updates):
    if not updates:
        return

    import psycopg2.extras

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            UPDATE mailing_lists
            SET imap_pass = %(imap_pass)s,
                smtp_pass = %(smtp_pass)s
            WHERE id = %(id)s
            """,
            updates,
            page_size=100,
        )


def main(argv=None):
    args = parse_args(argv)

    import psycopg2
    import psycopg2.extras
    from dotenv import load_dotenv

    load_default_env_files()
    for env_file in args.env_file:
        load_dotenv(env_file, override=True)

    db_config = get_db_config()

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            rows = iter_target_rows(cur)

        updates, stats = plan_updates(rows)

        print(f"Rows scanned: {stats['rows_seen']}")
        print(f"Rows needing update: {stats['rows_to_update']}")
        print(f"IMAP passwords encrypted: {stats['imap_encrypted']}")
        print(f"SMTP passwords encrypted: {stats['smtp_encrypted']}")
        print(f"Rows already encrypted: {stats['rows_already_encrypted']}")
        print(f"Rows without credentials: {stats['rows_without_credentials']}")

        if updates:
            preview = ", ".join(
                f"{item['id']}:{item['address']}" for item in updates[:10]
            )
            suffix = " ..." if len(updates) > 10 else ""
            print(f"Rows to update: {preview}{suffix}")

        if args.dry_run:
            print("Dry run only. No database changes were written.")
            return 0

        apply_updates(conn, updates)
        conn.commit()
        print(f"Updated rows: {len(updates)}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
