#!/usr/bin/env python3
import os

import time, json, logging, email, imaplib, smtplib, ssl, random, socket
from logging.handlers import RotatingFileHandler
import psycopg2, psycopg2.extras
from psycopg2 import errors
from email.message import EmailMessage
from email.header import decode_header, make_header
from dotenv import load_dotenv

# === Load environment (global settings) ===
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
)

DEFAULT_MAX_BYTES = 10 * 1024 * 1024
DEFAULT_IMAP_FOLDER = os.getenv(
    "DEFAULT_IMAP_FOLDER", os.getenv("IMAP_FOLDER", "INBOX")
)
DANGEROUS_EXT_FALLBACK = ".exe,.bat,.cmd,.scr,.js,.vbs,.ps1,.jar,.msi"

STATE_DIR = os.getenv("STATE_DIR", "state")

# === Logging ===
LOG_FILE = os.getenv("LOG_FILE", os.path.expanduser("logs/mlist.log"))
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)


def _safe_positive_int(value, default):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _normalize_extension(value):
    ext = (value or "").strip().lower()
    if not ext:
        return ""
    return ext if ext.startswith(".") else f".{ext}"


def _parse_ext_set(value):
    return {
        normalized
        for normalized in (_normalize_extension(part) for part in str(value).split(","))
        if normalized
    }


DEFAULT_MAX_SEEN_IDS = _safe_positive_int(os.getenv("MAX_SEEN_IDS"), 500)
DEFAULT_DANGEROUS_EXT = _parse_ext_set(
    os.getenv("DANGEROUS_EXT", DANGEROUS_EXT_FALLBACK)
) or _parse_ext_set(DANGEROUS_EXT_FALLBACK)


LOG_IMAP_CONNECTION_SUCCESS = False
LOG_MAX_BYTES = _safe_positive_int(os.getenv("LOG_MAX_BYTES"), 5 * 1024 * 1024)
LOG_BACKUP_COUNT = _safe_positive_int(os.getenv("LOG_BACKUP_COUNT"), 7)
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=LOG_MAX_BYTES,
    backupCount=LOG_BACKUP_COUNT,
    encoding="utf-8",
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        file_handler,
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("mlist")


# === DB helpers ===
def get_conn():
    return psycopg2.connect(**DB)


_SCHEMA_FLAGS = {"imap_folder_checked": False}


def ensure_imap_folder_column():
    if _SCHEMA_FLAGS["imap_folder_checked"]:
        return

    with get_conn() as c:
        try:
            with c.cursor() as cur:
                cur.execute("ALTER TABLE mailing_lists ADD COLUMN imap_folder TEXT")
        except errors.DuplicateColumn:
            c.rollback()
        except errors.InsufficientPrivilege:
            c.rollback()
        else:
            pass
        finally:
            if not c.closed:
                try:
                    with c.cursor() as cur:
                        cur.execute(
                            "ALTER TABLE mailing_lists ALTER COLUMN imap_folder SET DEFAULT %s",
                            (DEFAULT_IMAP_FOLDER,),
                        )
                except errors.UndefinedColumn:
                    c.rollback()
                try:
                    with c.cursor() as cur:
                        cur.execute(
                            "UPDATE mailing_lists SET imap_folder = %s WHERE imap_folder IS NULL",
                            (DEFAULT_IMAP_FOLDER,),
                        )
                except errors.UndefinedColumn:
                    c.rollback()
            _SCHEMA_FLAGS["imap_folder_checked"] = True


def load_runtime_settings():
    settings = {
        "max_bytes_limit": DEFAULT_MAX_BYTES,
        "max_seen_ids": DEFAULT_MAX_SEEN_IDS,
        "dangerous_ext": set(DEFAULT_DANGEROUS_EXT),
    }

    try:
        with get_conn() as c, c.cursor() as cur:
            cur.execute(
                "SELECT key, value FROM app_config WHERE key = ANY(%s)",
                (["MAX_BYTES", "MAX_SEEN_IDS", "DANGEROUS_EXT"],),
            )
            rows = {key: value for key, value in cur.fetchall()}
    except Exception as e:
        log.error(f"Failed to load runtime settings from DB: {e}")
        return settings

    raw_max_bytes = rows.get("MAX_BYTES")
    if raw_max_bytes not in (None, ""):
        try:
            value = int(raw_max_bytes)
        except (ValueError, TypeError):
            log.warning(f"Invalid MAX_BYTES value '{raw_max_bytes}', using default")
        else:
            settings["max_bytes_limit"] = None if value <= 0 else value

    raw_max_seen_ids = rows.get("MAX_SEEN_IDS")
    if raw_max_seen_ids not in (None, ""):
        try:
            value = int(raw_max_seen_ids)
        except (ValueError, TypeError):
            log.warning(
                f"Invalid MAX_SEEN_IDS value '{raw_max_seen_ids}', using default"
            )
        else:
            if value > 0:
                settings["max_seen_ids"] = value
            else:
                log.warning(
                    f"Invalid MAX_SEEN_IDS value '{raw_max_seen_ids}', using default"
                )

    raw_dangerous_ext = rows.get("DANGEROUS_EXT")
    if raw_dangerous_ext not in (None, ""):
        parsed_ext = _parse_ext_set(raw_dangerous_ext)
        if parsed_ext:
            settings["dangerous_ext"] = parsed_ext
        else:
            log.warning(
                f"Invalid DANGEROUS_EXT value '{raw_dangerous_ext}', using default"
            )

    return settings


def load_active_lists():
    ensure_imap_folder_column()
    with get_conn() as c:
        try:
            with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, name, address, subject_tag,
                           imap_user, imap_pass, imap_host, imap_port,
                           imap_folder,
                           smtp_user, smtp_pass, smtp_host, smtp_port,
                           open_posting
                    FROM mailing_lists
                    WHERE is_active = TRUE
                """
                )
                return cur.fetchall()
        except errors.UndefinedColumn:
            c.rollback()
            with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, name, address, subject_tag,
                           imap_user, imap_pass, imap_host, imap_port,
                           smtp_user, smtp_pass, smtp_host, smtp_port,
                           open_posting
                    FROM mailing_lists
                    WHERE is_active = TRUE
                """
                )
                rows = cur.fetchall()
                for row in rows:
                    row["imap_folder"] = DEFAULT_IMAP_FOLDER
                return rows


def load_members(list_id):
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            """
            SELECT email FROM list_members
            WHERE list_id=%s AND is_active=TRUE
        """,
            (list_id,),
        )
        return [row[0] for row in cur.fetchall()]


# === State handling ===
def state_path(list_id):
    os.makedirs(STATE_DIR, exist_ok=True)
    return os.path.join(STATE_DIR, f"state_list_{list_id}.json")


def load_state(list_id):
    path = state_path(list_id)
    try:
        with open(path, "r", encoding="utf-8") as f:
            state = json.load(f)
    except FileNotFoundError:
        state = {}

    state.setdefault("seen_ids", [])
    state.setdefault("backoff_until", 0)
    state.setdefault("imap_fail_count", 0)
    state.setdefault("last_backoff_logged_until", 0)
    state.setdefault("pending_recipients", {})
    return state


def save_state(list_id, state):
    path = state_path(list_id)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f)


# === Message processing ===
def is_dangerous(filename, dangerous_ext):
    if not filename:
        return False
    ext = os.path.splitext(filename)[1].lower()
    return ext in dangerous_ext


def clean_subject(subject, tag):
    try:
        decoded = str(make_header(decode_header(subject)))
    except Exception:
        decoded = subject or ""
    if tag and not decoded.startswith(tag):
        return f"[{tag}] {decoded}"
    return decoded


def _imap_backoff_seconds(state):
    """
    Backoff for IMAP failures to avoid hammering the provider.
    60s, 120s, 240s... capped at 30 minutes, with jitter.
    """
    base = _safe_positive_int(os.getenv("IMAP_BACKOFF_BASE_SECONDS"), 60)
    cap = _safe_positive_int(os.getenv("IMAP_BACKOFF_CAP_SECONDS"), 30 * 60)

    n = int(state.get("imap_fail_count", 0))
    n = min(n + 1, 10)
    state["imap_fail_count"] = n

    delay = min(cap, base * (2 ** (n - 1)))
    jitter = random.randint(0, max(1, delay // 5))  # up to ~20%
    return delay + jitter


def _remember_seen_id(state, msgid, max_seen_ids):
    seen_ids = state.setdefault("seen_ids", [])
    if msgid not in seen_ids:
        seen_ids.append(msgid)
        if len(seen_ids) > max_seen_ids:
            state["seen_ids"] = seen_ids[-max_seen_ids:]
    state.setdefault("pending_recipients", {}).pop(msgid, None)


def _mark_seen(M, msg_uid, list_name, reason):
    try:
        typ, _ = M.uid("store", msg_uid, "+FLAGS.SILENT", r"(\Seen)")
    except Exception as e:
        log.warning(f"List {list_name}: failed to mark message seen for {reason}: {e}")
        return False

    if typ != "OK":
        log.warning(f"List {list_name}: IMAP store failed while marking {reason} seen")
        return False

    return True


def process_list(list_row, runtime_settings):
    list_id = list_row["id"]
    list_name = list_row["name"]
    list_addr = list_row["address"]
    subject_tag = list_row["subject_tag"] or ""
    max_bytes_limit = runtime_settings["max_bytes_limit"]
    max_seen_ids = runtime_settings["max_seen_ids"]
    dangerous_ext = runtime_settings["dangerous_ext"]
    imap_user = list_row["imap_user"]
    imap_pass = list_row["imap_pass"]
    smtp_user = list_row["smtp_user"]
    smtp_pass = list_row["smtp_pass"]
    open_posting = list_row["open_posting"]

    state = load_state(list_id)
    now = time.time()
    if now < state.get("backoff_until", 0):
        backoff_until = state.get("backoff_until", 0)
        if state.get("last_backoff_logged_until") != backoff_until:
            remaining = max(0, int(backoff_until - now))
            log.info(
                f"List {list_name}: Backing off for another {remaining}s "
                f"(until {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(backoff_until))})"
            )
            state["last_backoff_logged_until"] = backoff_until
            save_state(list_id, state)
        return

    # Avoid long hangs on broken networks
    socket.setdefaulttimeout(
        _safe_positive_int(os.getenv("SOCKET_TIMEOUT_SECONDS"), 60)
    )

    M = None
    S = None

    try:
        # --- connect IMAP ---
        try:
            M = imaplib.IMAP4_SSL(
                list_row["imap_host"], int(list_row.get("imap_port", 993))
            )
            M.login(imap_user, imap_pass)
            folder = list_row.get("imap_folder") or DEFAULT_IMAP_FOLDER
            typ, _ = M.select(folder)
            if typ != "OK":
                raise RuntimeError(f"IMAP select failed for folder '{folder}'")

            # IMAP ok -> reset fail counter
            state["imap_fail_count"] = 0
            state["backoff_until"] = 0
            state["last_backoff_logged_until"] = 0
            save_state(list_id, state)

            if LOG_IMAP_CONNECTION_SUCCESS:
                log.info(f"List {list_name}: Connection success")

        except Exception as e:
            # IMPORTANT: close partial connection if login/select failed
            try:
                if M is not None:
                    M.logout()
            except Exception:
                pass

            delay = _imap_backoff_seconds(state)
            state["backoff_until"] = time.time() + delay
            save_state(list_id, state)

            log.error(f"List {list_name}: IMAP connect failed: {e} (backoff {delay}s)")
            return

        typ, data = M.uid("search", None, "UNSEEN")
        if typ != "OK":
            log.warning(f"List {list_name}: IMAP search failed")
            return

        msg_uids = (data[0] or b"").split()
        if not msg_uids:
            return

        members = load_members(list_id)
        if not members:
            log.warning(f"List {list_name}: No members, skipping")
            return
        member_set = {m.lower() for m in members if m}

        # --- connect SMTP ---
        try:
            context = ssl.create_default_context()
            S = smtplib.SMTP_SSL(
                list_row["smtp_host"],
                int(list_row.get("smtp_port", 465)),
                context=context,
            )
            S.login(smtp_user, smtp_pass)
        except Exception as e:
            log.error(f"List {list_name}: SMTP connect failed: {e}")
            return

        pending_recipients = state.setdefault("pending_recipients", {})

        for msg_uid in msg_uids:
            typ, data = M.uid("fetch", msg_uid, "(BODY.PEEK[])")
            if typ != "OK" or not data or not data[0]:
                continue

            raw = data[0][1]
            msg_uid_text = msg_uid.decode(errors="ignore")
            if max_bytes_limit is not None and len(raw) > max_bytes_limit:
                log.warning(f"List {list_name}: Message too large, skipped")
                msg = email.message_from_bytes(raw)
                msgid = msg.get("Message-ID") or f"NO-MSGID:{msg_uid_text}"
                _remember_seen_id(state, msgid, max_seen_ids)
                _mark_seen(M, msg_uid, list_name, msgid)
                save_state(list_id, state)
                continue

            msg = email.message_from_bytes(raw)

            # duplicate protection
            msgid = msg.get("Message-ID") or f"NO-MSGID:{msg_uid_text}"
            if msgid in state.get("seen_ids", []):
                _mark_seen(M, msg_uid, list_name, msgid)
                continue

            # sender restrictions
            from_addr = email.utils.parseaddr(msg.get("From"))[1]
            if not open_posting:
                sender = (from_addr or "").lower()
                if sender not in member_set:
                    log.warning(
                        f"List {list_name}: unauthorized sender {from_addr}, skipping"
                    )
                    _remember_seen_id(state, msgid, max_seen_ids)
                    _mark_seen(M, msg_uid, list_name, msgid)
                    save_state(list_id, state)
                    continue

            # subject tag (safe if Subject missing)
            subj = msg.get("Subject", "")
            new_subj = clean_subject(subj, subject_tag)
            if "Subject" in msg:
                msg.replace_header("Subject", new_subj)
            else:
                msg.add_header("Subject", new_subj)

            # attachment filtering
            safe = True
            for part in msg.walk():
                if part.get_filename() and is_dangerous(
                    part.get_filename(), dangerous_ext
                ):
                    log.warning(
                        f"{list_name}: dangerous attachment {part.get_filename()}, skipping"
                    )
                    safe = False
                    break
            if not safe:
                _remember_seen_id(state, msgid, max_seen_ids)
                _mark_seen(M, msg_uid, list_name, msgid)
                save_state(list_id, state)
                continue

            target_members = pending_recipients.get(msgid) or members
            target_members = [
                recipient
                for recipient in target_members
                if recipient and recipient.lower() in member_set
            ]
            if not target_members:
                log.warning(
                    f"List {list_name}: no retry recipients remain for {msgid}, marking seen"
                )
                _remember_seen_id(state, msgid, max_seen_ids)
                _mark_seen(M, msg_uid, list_name, msgid)
                save_state(list_id, state)
                continue

            # forward
            try:
                # force replies to the list (reply-to-list mode)
                if "Reply-To" in msg:
                    msg.replace_header("Reply-To", list_addr)
                else:
                    msg.add_header("Reply-To", list_addr)

                # always show list in To
                if "To" in msg:
                    msg.replace_header("To", f"{list_name} <{list_addr}>")
                else:
                    msg.add_header("To", f"{list_name} <{list_addr}>")

                # drop CC to avoid leaking addresses
                if "Cc" in msg:
                    del msg["Cc"]

                refused = S.sendmail(list_addr, target_members, msg.as_string())
                if refused:
                    pending_recipients[msgid] = list(refused.keys())
                    backoff = int(os.getenv("BACKOFF_MINUTES", 30)) * 60
                    state["backoff_until"] = time.time() + backoff
                    save_state(list_id, state)
                    log.error(
                        f"List {list_name}: SMTP accepted only "
                        f"{len(target_members) - len(refused)}/{len(target_members)} recipients "
                        f"for {msgid}; retrying refused recipients later"
                    )
                    break

                _remember_seen_id(state, msgid, max_seen_ids)
                _mark_seen(M, msg_uid, list_name, msgid)
                save_state(list_id, state)
                log.info(
                    f"List {list_name}: forwarded message from {from_addr} to {len(target_members)} members"
                )
            except Exception as e:
                pending_recipients[msgid] = target_members
                log.error(f"List {list_name}: SMTP send failed: {e}")
                backoff = int(os.getenv("BACKOFF_MINUTES", 30)) * 60
                state["backoff_until"] = time.time() + backoff
                save_state(list_id, state)
                break

        save_state(list_id, state)

    finally:
        # Always close both connections, even on unexpected exceptions
        try:
            if M is not None:
                M.logout()
        except Exception:
            pass
        try:
            if S is not None:
                S.quit()
        except Exception:
            pass


def main():
    runtime_settings = load_runtime_settings()
    lists = load_active_lists()
    if not lists:
        log.info("No active mailing lists found.")
        return
    for row in lists:
        try:
            process_list(row, runtime_settings)
        except Exception as e:
            log.exception(f"Error processing list {row['name']}: {e}")


if __name__ == "__main__":
    main()
