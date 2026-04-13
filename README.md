# Mailing List Manager

Self-hosted mailing list software with two parts:

- `list/`: the mailer that fetches messages from IMAP and redistributes them via SMTP
- `list-admin/`: the web admin UI for managing lists, members, users, logs, and global settings

This app was initially designed to be run in a limited-access shared webhost server, hence the usage of cron jobs, pm2 etc.
If you have a webserver that is not shared and you have root/full access, then there are of course better ways to run this app.

The current deployment model uses:

- Python 3
- PostgreSQL
- PM2 for keeping the admin UI process alive
- cron for running the mailer and the PM2 watchdog

## What This Repository Contains

- The mailer runtime in [`list/mlist.py`](list/mlist.py)
- The admin UI in [`list-admin/app.py`](list-admin/app.py)
- The one-shot credential migration in [`scripts/migrate_mail_credentials.py`](scripts/migrate_mail_credentials.py)
- A portable mailer launcher in [`list/run_mlist.sh`](list/run_mlist.sh)
- A PM2 watchdog for the admin UI in [`list-admin/ensure-list-admin.sh`](list-admin/ensure-list-admin.sh)
- A clean bootstrap schema in [`list-admin/sql/schema.sql`](list-admin/sql/schema.sql)
- Example environment files [`list/.env.example`](list/.env.example) and [`list-admin/.env.example`](list-admin/.env.example)

## Requirements

- Python 3.11+ recommended
- PostgreSQL
- Node.js + PM2
- A reverse proxy such as Nginx or Apache for the admin UI

## Project Setup

### 1. Clone the repo

```bash
git clone https://github.com/henrimuldre/mailing-list-manager
cd mailing-list-manager
```

### 2. Create Python virtual environments

You can use one shared virtual environment if you prefer, but separate venvs keep the shell scripts simple.

```bash
python3 -m venv list/.venv
python3 -m venv list-admin/.venv
list/.venv/bin/pip install -r list/requirements.txt
list-admin/.venv/bin/pip install -r list-admin/requirements.txt
```

If installing dependencies fails on `cryptography` on platforms without a compatible
prebuilt wheel, such as some FreeBSD or shared-hosting environments, `pip` may fall back
to building from source. In that case you may need Rust installed before rerunning the
`pip install` commands.

<details>
<summary>Install Rust Only If <code>cryptography</code> Fails To Build</summary>

If `pip install` fails while building `cryptography`, install Rust with the official
`rustup` installer, reload your shell environment, and then rerun the dependency install:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
. "$HOME/.cargo/env"
rustc --version
cargo --version
```

Then retry:

```bash
list/.venv/bin/pip install -r list/requirements.txt
list-admin/.venv/bin/pip install -r list-admin/requirements.txt
```

</details>

### 3. Configure environment files

```bash
cp list/.env.example list/.env
cp list-admin/.env.example list-admin/.env
```

Then edit both `.env` files with your real values.

Important variables:

- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASS`
- `FLASK_SECRET` for the admin UI
- `MAIL_CREDENTIALS_KEY` shared by `list/` and `list-admin/`
- `LIST_ADMIN_HOST`, `LIST_ADMIN_PORT`

Generate `MAIL_CREDENTIALS_KEY` with:

```bash
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Once `MAIL_CREDENTIALS_KEY` is set, newly saved IMAP/SMTP passwords are stored in the
database as encrypted `enc:v1:...` values instead of raw text. Existing plaintext values
remain readable for backward compatibility and will be encrypted the next time that list is
saved through the admin UI.

Fresh install vs upgrade:

- Fresh install with a new database: no migration is needed. Set the same `MAIL_CREDENTIALS_KEY` in both `list/.env` and `list-admin/.env` before creating any lists.
- Upgrade of an existing deployment with old plaintext credentials already in `mailing_lists`: run the migration once to convert those legacy rows.

To migrate all existing plaintext credentials in one go:

```bash
list-admin/.venv/bin/python scripts/migrate_mail_credentials.py --dry-run
list-admin/.venv/bin/python scripts/migrate_mail_credentials.py
```

The script auto-loads `list-admin/.env` and `list/.env`, skips rows already stored as
`enc:v1:...`, and only updates rows that still contain legacy plaintext values.

### 4. Create the database schema

Import the canonical schema:

```bash
psql -U <db-user> -d <db-name> -f list-admin/sql/schema.sql
```

You will also need initial data in:

- `app_config`
- `mailing_lists`
- at least one admin user in `users`

The app does not currently ship with a full installer, so those initial rows must be created manually or with your own seed SQL.

### 5. Start the admin UI

Direct run:

```bash
cd list-admin
.venv/bin/python app.py --host 127.0.0.1 --port 9010 --no-debug --no-reload --no-debugger
```

PM2-managed run:

```bash
cd list-admin
./ensure-list-admin.sh
```

The watchdog script expects PM2 to be available in `PATH`.

Useful watchdog tuning env vars:

- `STARTUP_WAIT_SECONDS` to allow extra warm-up time before a fresh `pm2 start` or `pm2 restart` is treated as failed
- `HEALTH_TIMEOUT`, `HEALTH_RETRIES`, `HEALTH_CHECK_INTERVAL` to tune the HTTP health probe behavior

### 6. Run the mailer

Manual run:

```bash
cd list
./run_mlist.sh
```

The script will use:

- `LIST_PYTHON` if set
- otherwise `list/.venv/bin/python` if present
- otherwise plain `python3`

## Cron Setup

Example cron entries:

```cron
*/2 * * * * /path/to/repo/list/run_mlist.sh
0 */2 * * * /path/to/repo/list-admin/ensure-list-admin.sh
```

`*/2` for `run_mlist.sh` is a reasonable production default if you want mail delivery to stay responsive.
If you see occasional IMAP read timeouts but overall delivery is working, prefer increasing `SOCKET_TIMEOUT_SECONDS`
in `list/.env` to `90` or `120` before slowing the cron interval down.

Adjust the intervals to your needs.

## Reverse Proxy

The admin UI is designed to bind to a local address and port, then sit behind a reverse proxy.

Example target:

- `127.0.0.1:9010`

Expose it through your web server and TLS setup, for example:

- `https://your-domain.example`

## To-do in the future

- Add seed SQL or an installation script for `app_config`, `mailing_lists`, and the first admin user
- Replace Flask’s built-in server with Gunicorn or another WSGI server for production
- Add automated tests

## Notes

- The admin UI and mailer both rely on PostgreSQL as the shared source of truth.
- The mailer reads per-list IMAP/SMTP settings from the database.
