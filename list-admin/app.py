#!/usr/bin/env python3
import argparse, os, csv, io, functools, json, glob, time
from collections import deque
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    Response,
    g,
    session,
    make_response,
    has_request_context,
)
import psycopg2, psycopg2.extras
from psycopg2 import errors
from email_validator import validate_email, EmailNotValidError
from dotenv import load_dotenv
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import timedelta

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ==== App meta ====
APP_VERSION = os.getenv("APP_VERSION")
APP_AUTHOR = os.getenv("APP_AUTHOR")

_WEAK_FLASK_SECRETS = {
    "",
    "dev",
    "development",
    "secret",
    "changeme",
    "change-me",
    "default",
    "password",
    "test",
}


def _load_flask_secret():
    secret = (os.getenv("FLASK_SECRET") or "").strip()
    if len(secret) < 32 or secret.lower() in _WEAK_FLASK_SECRETS:
        raise RuntimeError(
            "FLASK_SECRET must be set to a strong random value of at least 32 characters"
        )
    return secret

# ==== Flask app ====
app = Flask(__name__, static_folder="static", template_folder="templates")
app.secret_key = _load_flask_secret()
app.config.update(
    SESSION_COOKIE_SECURE=True,      # only over HTTPS
    SESSION_COOKIE_HTTPONLY=True,    # not accessible to JS
    SESSION_COOKIE_SAMESITE="Lax",   # CSRF mitigation
)

# Absolute max lifetime of a login (cookie expiry)
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)
# Optional: don’t refresh expiry on every request (keeps it absolute)
# app.config['SESSION_REFRESH_EACH_REQUEST'] = False

DEFAULT_IDLE_TIMEOUT_SECONDS = int(os.getenv("IDLE_TIMEOUT_SECONDS", "1800"))  # 30 min
DEFAULT_ABSOLUTE_TIMEOUT_SECONDS = int(
    os.getenv("ABSOLUTE_TIMEOUT_SECONDS", str(12 * 3600))
)  # 12h

MB_FACTOR = Decimal(1024 * 1024)
DEFAULT_IMAP_FOLDER = os.getenv(
    "DEFAULT_IMAP_FOLDER", os.getenv("IMAP_FOLDER", "INBOX")
)
LOG_FILE_PATH = os.getenv(
    "MLIST_LOG_PATH",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "list", "logs", "mlist.log")),
)
ENSURE_LOG_FILE_PATH = os.getenv(
    "ENSURE_LOG_PATH",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "logs", "ensure-list-admin.log")),
)
LOG_TAIL_LINES = int(os.getenv("MLIST_LOG_TAIL", "400"))

_SCHEMA_FLAGS = {
    "imap_folder_checked": False,
    "users_password_reset_checked": False,
    "users_password_reset_available": None,
}


def _bytes_to_mb_display(raw_value):
    try:
        dec_value = Decimal(str(raw_value))
    except (InvalidOperation, TypeError, ValueError):
        return ""
    mb_value = dec_value / MB_FACTOR
    quantized = mb_value.quantize(Decimal("0.01"))
    text = format(quantized.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def _mb_to_bytes(value):
    dec = Decimal(value)
    bytes_value = (dec * MB_FACTOR).to_integral_value(rounding=ROUND_HALF_UP)
    return int(bytes_value)


def _safe_positive_int(value, default):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _parse_tcp_port(value, source):
    try:
        port = int(str(value).strip())
    except (TypeError, ValueError):
        raise RuntimeError(f"{source} must be an integer TCP port, got {value!r}")
    if not 1 <= port <= 65535:
        raise RuntimeError(f"{source} must be between 1 and 65535, got {port}")
    return port


def _env_flag(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _read_log_tail(path=LOG_FILE_PATH, limit=LOG_TAIL_LINES):
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return list(deque(fh, max(1, limit)))


@app.context_processor
def inject_globals():
    # Fetch once per request
    global_settings = {}
    try:
        global_settings = load_globals()
    except Exception:
        pass  # don’t block rendering if DB fails

    return dict(
        app_version=APP_VERSION,
        app_author=APP_AUTHOR,
        list_name=getattr(g, "list_name", None),
        list_address=getattr(g, "list_address", None),
        first_name=getattr(g, "first_name", None),
        globals=global_settings,
    )


# ==== i18n loader ====
I18N_DIR = os.getenv("I18N_DIR", os.path.join(os.path.dirname(__file__), "i18n"))
TRANSLATIONS, _TRANSLATION_MTIMES = {}, {}


def load_translations():
    """Load all *.json in I18N_DIR into TRANSLATIONS."""
    global TRANSLATIONS, _TRANSLATION_MTIMES
    merged, mtimes = {}, {}
    if not os.path.isdir(I18N_DIR):
        return
    for path in glob.glob(os.path.join(I18N_DIR, "*.json")):
        lang = os.path.splitext(os.path.basename(path))[0]
        try:
            with open(path, "r", encoding="utf-8") as f:
                merged[lang] = json.load(f)
            mtimes[lang] = os.path.getmtime(path)
        except Exception:
            # ignore broken files to avoid taking down the app
            continue
    if merged:
        TRANSLATIONS, _TRANSLATION_MTIMES = merged, mtimes


def _maybe_reload_translations():
    """Auto-reload on change when app.debug is True."""
    if not app.debug:
        return
    for lang, old_m in list(_TRANSLATION_MTIMES.items()):
        path = os.path.join(I18N_DIR, f"{lang}.json")
        try:
            new_m = os.path.getmtime(path)
        except FileNotFoundError:
            new_m = -1
        if new_m != old_m:
            load_translations()
            break


load_translations()


def t(key, **kwargs):
    """Translate key using current g.language, fallback to English, else key."""
    _maybe_reload_translations()
    lang = getattr(g, "language", "en")
    text = (TRANSLATIONS.get(lang) or {}).get(key)
    if text is None:
        text = (TRANSLATIONS.get("en") or {}).get(key, key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except Exception:
            pass
    return text


# expose in Jinja
app.jinja_env.globals.update(t=t)

# ==== DB config ====
DB = dict(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
)

def get_conn():
    return psycopg2.connect(**DB)


def _column_exists(cur, table_name, column_name, schema="public"):
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema=%s AND table_name=%s AND column_name=%s
        """,
        (schema, table_name, column_name),
    )
    return cur.fetchone() is not None


def load_globals():
    """Fetch global settings from app_config table as a dict."""
    if has_request_context() and hasattr(g, "_global_settings"):
        return g._global_settings

    with get_conn() as c, c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT key, value FROM app_config")
        settings = {row["key"]: row["value"] for row in cur.fetchall()}

    if has_request_context():
        g._global_settings = settings
    return settings


def load_auth_timeouts():
    settings = {}
    try:
        settings = load_globals()
    except Exception:
        pass

    idle_timeout = _safe_positive_int(
        settings.get("IDLE_TIMEOUT_SECONDS"), DEFAULT_IDLE_TIMEOUT_SECONDS
    )
    absolute_timeout = _safe_positive_int(
        settings.get("ABSOLUTE_TIMEOUT_SECONDS"),
        DEFAULT_ABSOLUTE_TIMEOUT_SECONDS,
    )
    return idle_timeout, absolute_timeout


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


def ensure_user_password_reset_column():
    if _SCHEMA_FLAGS["users_password_reset_checked"]:
        return bool(_SCHEMA_FLAGS["users_password_reset_available"])

    available = False
    with get_conn() as c:
        with c.cursor() as cur:
            available = _column_exists(cur, "users", "password_needs_reset")

        if not available:
            try:
                with c.cursor() as cur:
                    cur.execute(
                        "ALTER TABLE users ADD COLUMN password_needs_reset BOOLEAN NOT NULL DEFAULT FALSE"
                    )
            except errors.DuplicateColumn:
                c.rollback()
                available = True
            except errors.InsufficientPrivilege:
                c.rollback()
                app.logger.warning(
                    "users.password_needs_reset column is unavailable; "
                    "password reset enforcement is disabled until the schema is migrated"
                )
            else:
                available = True

        if not available:
            with c.cursor() as cur:
                available = _column_exists(cur, "users", "password_needs_reset")

    _SCHEMA_FLAGS["users_password_reset_checked"] = True
    _SCHEMA_FLAGS["users_password_reset_available"] = available
    return available


def _password_reset_select_sql():
    if ensure_user_password_reset_column():
        return "password_needs_reset AS password_needs_reset"
    return "FALSE AS password_needs_reset"


def _load_server_options(argv=None):
    env_host = (
        os.getenv("LIST_ADMIN_HOST")
        or os.getenv("HOST")
        or "127.0.0.1"
    ).strip() or "127.0.0.1"
    env_port = _parse_tcp_port(
        os.getenv("LIST_ADMIN_PORT") or os.getenv("PORT") or "9010",
        "LIST_ADMIN_PORT/PORT",
    )
    env_debug = _env_flag("FLASK_DEBUG", False)
    env_reload = _env_flag("FLASK_RELOAD", env_debug)
    env_debugger = _env_flag("FLASK_DEBUGGER", env_debug)

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--host", default=env_host)
    parser.add_argument("--port", type=int, default=env_port)
    parser.add_argument("--debug", dest="debug", action="store_true")
    parser.add_argument("--no-debug", dest="debug", action="store_false")
    parser.add_argument("--reload", dest="reload", action="store_true")
    parser.add_argument("--no-reload", dest="reload", action="store_false")
    parser.add_argument("--debugger", dest="debugger", action="store_true")
    parser.add_argument("--no-debugger", dest="debugger", action="store_false")
    parser.set_defaults(
        debug=env_debug,
        reload=env_reload,
        debugger=env_debugger,
    )
    options, _ = parser.parse_known_args(argv)
    options.host = (options.host or env_host).strip() or env_host
    options.port = _parse_tcp_port(options.port, "--port")
    return options


def fetch_list_settings(list_id):
    """Fetch list configuration, falling back if optional columns are missing."""
    ensure_imap_folder_column()
    query_with_folder = """
        SELECT id, name, address, is_active, subject_tag,
               open_posting, imap_host, imap_port, imap_user, imap_pass,
               imap_folder,
               smtp_host, smtp_port, smtp_user, smtp_pass
        FROM mailing_lists
        WHERE id=%s
    """

    query_without_folder = """
        SELECT id, name, address, is_active, subject_tag,
               open_posting, imap_host, imap_port, imap_user, imap_pass,
               smtp_host, smtp_port, smtp_user, smtp_pass
        FROM mailing_lists
        WHERE id=%s
    """

    with get_conn() as c:
        try:
            with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query_with_folder, (list_id,))
                row = cur.fetchone()
                return dict(row) if row else None
        except errors.UndefinedColumn:
            c.rollback()
            with c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(query_without_folder, (list_id,))
                row = cur.fetchone()
                if not row:
                    return None
                data = dict(row)
                data.setdefault("imap_folder", DEFAULT_IMAP_FOLDER)
                return data


# ==== Auth ====
def invalidate_session(message_key=None, category="error"):
    session.clear()
    if message_key:
        flash(t(message_key), category)

    endpoint = request.endpoint or ""
    if endpoint in {"login", "static"}:
        return None

    return redirect(url_for("login"))


def requires_login(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("login"))
        if getattr(g, "user_exists", None) is False:
            response = invalidate_session("session_expired_absolute")
            return response or redirect(url_for("login"))
        return fn(*args, **kwargs)

    return wrapper


# ==== Administrator ====
def requires_admin(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("login"))
        if getattr(g, "role", "user") != "admin":
            flash(t("not_authorized"), "error")
            return redirect(url_for("dashboard"))
        return fn(*args, **kwargs)

    return wrapper


# ---- Login / Logout ----
@app.get("/login")
def login():
    return render_template("login.html")


@app.post("/login")
def login_post():
    username = (request.form.get("username") or "").strip()
    password = request.form.get("password") or ""
    user = get_user(username)

    if user and check_password_hash(user["password_hash"], password):
        # reset session
        session.clear()
        session["user"] = username
        session.permanent = True  # cookie gets an expiry
        now = int(time.time())
        session["login_time"] = now
        session["last_active"] = now

        g.language = user["language"] or "en"
        first = user.get("first_name") if user else None

        # update last login time in DB
        try:
            with get_conn() as c, c.cursor() as cur:
                cur.execute(
                    "UPDATE users SET last_login = NOW() WHERE username=%s",
                    (username,),
                )
        except Exception:
            pass

        if user["password_needs_reset"]:
            flash(t("password_reset_required"), "warning")
            return redirect(url_for("password_reset"))

        flash(t("login_success", name=(first or username)), "success")
        return redirect(url_for("dashboard"))

    flash(t("login_invalid"), "error")
    return redirect(url_for("login"))


@app.get("/logout")
def logout():
    session.clear()
    flash(t("logout_success"), "success")
    return redirect(url_for("login"))


# ==== Helpers ====
def valid_email(addr: str) -> str:
    try:
        return validate_email(addr, check_deliverability=False).normalized
    except EmailNotValidError as e:
        raise ValueError(str(e))


def get_user(username):
    password_reset_sql = _password_reset_select_sql()
    with get_conn() as c, c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            f"""
            SELECT id, username, password_hash, theme, language,
                   first_name, last_name, role, {password_reset_sql}
            FROM users WHERE username=%s
        """,
            (username,),
        )
        return cur.fetchone()


def update_user(
    username,
    password=None,
    theme=None,
    language=None,
    first_name=None,
    last_name=None,
    password_needs_reset=None,
):
    supports_password_reset = ensure_user_password_reset_column()
    with get_conn() as c, c.cursor() as cur:
        fields, params = [], []
        if password:
            fields.append("password_hash=%s")
            params.append(generate_password_hash(password))
        if password_needs_reset is not None and supports_password_reset:
            fields.append("password_needs_reset=%s")
            params.append(bool(password_needs_reset))
        if theme is not None:
            fields.append("theme=%s")
            params.append(theme)
        if language is not None:
            fields.append("language=%s")
            params.append(language)
        if first_name is not None:
            fields.append("first_name=%s")
            params.append(first_name)
        if last_name is not None:
            fields.append("last_name=%s")
            params.append(last_name)
        if fields:
            params.append(username)
            cur.execute(
                f"UPDATE users SET {', '.join(fields)} WHERE username=%s",
                tuple(params),
            )


# ==== Loader (per-request globals) ====
@app.before_request
def load_user_prefs():
    # --- session timeout checks ---
    if session.get("user"):
        idle_timeout, absolute_timeout = load_auth_timeouts()
        now = int(time.time())
        last = session.get("last_active", now)
        login_time = session.get("login_time", now)

        # idle timeout
        if now - last > idle_timeout:
            session.clear()
            flash(t("session_expired_idle"), "error")
            return redirect(url_for("login"))

        # absolute timeout
        if now - login_time > absolute_timeout:
            session.clear()
            flash(t("session_expired_absolute"), "error")
            return redirect(url_for("login"))

        # update last activity timestamp
        session["last_active"] = now

    # --- defaults ---
    g.theme = "auto"
    g.language = session.get("language") or request.cookies.get("lang", "en")
    g.first_name = g.last_name = None
    g.role = "user"
    g.lists = []
    g.list_id = None
    g.list_name = None
    g.list_address = None
    g.password_needs_reset = False
    g.user_exists = None

    username = session.get("user")
    try:
        password_reset_sql = _password_reset_select_sql()
        with get_conn() as c, c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if username:
                cur.execute(
                    f"""
                    SELECT theme, language, first_name, last_name, role, {password_reset_sql}
                    FROM users WHERE username=%s
                """,
                    (username,),
                )
                row = cur.fetchone()
                if row:
                    g.user_exists = True
                    g.theme = row["theme"] or "auto"
                    g.language = row["language"] or "en"
                    g.first_name = row["first_name"] or None
                    g.last_name = row["last_name"] or None
                    g.role = row["role"] or "user"
                    g.password_needs_reset = bool(row["password_needs_reset"])
                else:
                    g.user_exists = False
                    response = invalidate_session("session_expired_absolute")
                    if response is not None:
                        return response
                    username = None
                    g.password_needs_reset = False
            else:
                g.password_needs_reset = False

            needs_reset = getattr(g, "password_needs_reset", False)
            if needs_reset:
                allowed_endpoints = {
                    "password_reset",
                    "logout",
                    "static",
                    "set_language",
                }
                endpoint = request.endpoint or ""
                if endpoint not in allowed_endpoints:
                    flash(t("password_reset_required"), "warning")
                    return redirect(url_for("password_reset"))

            # available lists
            cur.execute(
                """
                SELECT id, name, address, is_active
                FROM mailing_lists
                ORDER BY is_active DESC, id
            """
            )
            g.lists = cur.fetchall()

            # choose current list (session or first)
            sid = session.get("list_id")
            current = None
            if sid:
                current = next((L for L in g.lists if int(L["id"]) == int(sid)), None)
            if not current:
                current = next((L for L in g.lists if L["is_active"]), None)
            if not current and g.lists:
                current = g.lists[0]
            if current:
                g.list_id = int(current["id"])
                g.list_name = current["name"]
                g.list_address = current["address"]
                session["list_id"] = int(current["id"])
    except Exception:
        # Avoid blocking requests if a preference query fails
        pass


# ==== Root redirects ====
@app.get("/")
def root_redirect():
    return redirect(url_for("dashboard"))


@app.get("/index.html")
def index_html_redirect():
    return redirect(url_for("dashboard"))


# ==== Members ====
@app.get("/members")
@requires_login
def members():
    q = (request.args.get("q") or "").strip()
    only_active = request.args.get("active", "1") == "1"
    sort = request.args.get("sort", "email")
    direction = request.args.get("dir", "asc").lower()

    allowed_sorts = {"email", "display_name", "is_active", "created_at"}
    if sort not in allowed_sorts:
        sort = "email"
    if direction not in {"asc", "desc"}:
        direction = "asc"

    sql = """
        SELECT id, email, COALESCE(display_name,'') AS display_name,
               is_active, created_at
        FROM list_members
        WHERE list_id = %s
    """
    params = [g.list_id]

    if q:
        sql += (
            " AND (LOWER(email) LIKE LOWER(%s) OR LOWER(display_name) LIKE LOWER(%s))"
        )
        params += [f"%{q}%", f"%{q}%"]
    if only_active:
        sql += " AND is_active = TRUE"

    sql += f" ORDER BY {sort} {direction} LIMIT 1000"

    with get_conn() as c, c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    return render_template(
        "members.html",
        rows=rows,
        q=q,
        only_active=only_active,
        sort=sort,
        dir=direction,
        list_name=g.list_name,
        list_address=g.list_address,
    )


@app.post("/add")
@requires_login
def add():
    email = (request.form.get("email") or "").strip()
    display_name = (request.form.get("display_name") or "").strip() or None
    try:
        email = valid_email(email)
        with get_conn() as c, c.cursor() as cur:
            cur.execute(
                """
                INSERT INTO list_members(list_id, email, display_name, is_active)
                VALUES (%s, %s, %s, TRUE)
                ON CONFLICT (list_id, LOWER(email)) DO UPDATE
                  SET display_name = EXCLUDED.display_name,
                      is_active    = TRUE
            """,
                (g.list_id, email, display_name),
            )
        flash(t("member_added", email=email), "success")
    except Exception as e:
        flash(t("member_add_error", email=email, error=e), "error")
    return redirect(url_for("members"))


@app.post("/edit/<int:member_id>")
@requires_login
def edit_member(member_id):
    email = (request.form.get("email") or "").strip()
    display_name = (request.form.get("display_name") or "").strip() or None
    try:
        email = valid_email(email)
        with get_conn() as c, c.cursor() as cur:
            cur.execute(
                """
                UPDATE list_members
                   SET email = %s,
                       display_name = %s
                 WHERE id = %s AND list_id = %s
            """,
                (email, display_name, member_id, g.list_id),
            )
        flash(t("member_updated", email=email), "success")
    except Exception as e:
        flash(t("member_update_error", error=e), "error")
    return redirect(url_for("members"))


@app.post("/toggle/<int:member_id>")
@requires_login
def toggle(member_id):
    try:
        with get_conn() as c, c.cursor() as cur:
            cur.execute(
                """
                UPDATE list_members
                   SET is_active = NOT is_active
                 WHERE id=%s AND list_id=%s
             RETURNING is_active
            """,
                (member_id, g.list_id),
            )
            row = cur.fetchone()
            new_active = bool(row[0]) if row else True
        flash(t("member_toggled"), "success")
    except Exception as e:
        flash(t("member_toggle_error", error=e), "error")
        new_active = True
    q = request.form.get("q", "")
    active_filter = "1" if new_active else "0"
    return redirect(url_for("members", q=q, active=active_filter))


@app.post("/delete/<int:member_id>")
@requires_login
def delete(member_id):
    try:
        with get_conn() as c, c.cursor() as cur:
            cur.execute(
                "DELETE FROM list_members WHERE id=%s AND list_id=%s",
                (member_id, g.list_id),
            )
        flash(t("member_deleted"), "success")
    except Exception as e:
        flash(t("member_delete_error", error=e), "error")
    return redirect(url_for("members"))


@app.post("/bulk")
@requires_login
def bulk():
    mode = request.form.get("mode", "add")
    text = (request.form.get("bulk_text") or "").strip()
    file = request.files.get("bulk_file")

    entries = []
    if text:
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.split(",")]
            entries.append((parts[0], parts[1] if len(parts) > 1 else None))
    elif file and file.filename:
        data = file.read().decode("utf-8", "ignore")
        for row in csv.reader(io.StringIO(data)):
            if not row:
                continue
            entries.append((row[0].strip(), row[1].strip() if len(row) > 1 else None))
    else:
        flash(t("provide_bulk"), "error")
        return redirect(url_for("members"))

    added = deactivated = deleted = 0
    with get_conn() as c, c.cursor() as cur:
        for raw_email, name in entries:
            try:
                email = valid_email(raw_email)
            except Exception:
                continue
            if mode == "add":
                cur.execute(
                    """
                    INSERT INTO list_members(list_id, email, display_name, is_active)
                    VALUES (%s, %s, %s, TRUE)
                    ON CONFLICT (list_id, LOWER(email)) DO UPDATE
                    SET display_name = EXCLUDED.display_name,
                        is_active    = TRUE
                """,
                    (g.list_id, email, name),
                )
                added += 1
            elif mode == "deactivate":
                cur.execute(
                    """
                    UPDATE list_members SET is_active=FALSE
                     WHERE list_id=%s AND LOWER(email)=LOWER(%s)
                """,
                    (g.list_id, email),
                )
                deactivated += cur.rowcount
            elif mode == "delete":
                cur.execute(
                    """
                    DELETE FROM list_members
                     WHERE list_id=%s AND LOWER(email)=LOWER(%s)
                """,
                    (g.list_id, email),
                )
                deleted += cur.rowcount

    flash(
        t("bulk_done", added=added, deactivated=deactivated, deleted=deleted), "success"
    )
    return redirect(url_for("members"))


# ==== Exports ====
@app.get("/export.csv")
@requires_login
def export_csv():
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            """
            SELECT email, COALESCE(display_name,'')
              FROM list_members
             WHERE list_id=%s AND is_active=TRUE
          ORDER BY email
        """,
            (g.list_id,),
        )
        rows = cur.fetchall()
    sio = io.StringIO()
    w = csv.writer(sio)
    w.writerow(["email", "display_name"])
    for r in rows:
        w.writerow(r)
    return Response(
        sio.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=members.csv"},
    )


@app.get("/export.txt")
@requires_login
def export_txt():
    with get_conn() as c, c.cursor() as cur:
        cur.execute(
            """
            SELECT email, COALESCE(display_name,'')
              FROM list_members
             WHERE list_id=%s AND is_active=TRUE
          ORDER BY email
        """,
            (g.list_id,),
        )
        rows = cur.fetchall()
    sio = io.StringIO()
    for email, name in rows:
        sio.write(f"{email}, {name}\n" if name else f"{email}\n")
    return Response(
        sio.getvalue(),
        mimetype="text/plain",
        headers={"Content-Disposition": "attachment; filename=members.txt"},
    )


# ==== Dashboard ====
@app.get("/dashboard")
@requires_login
def dashboard():
    with get_conn() as c, c.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM list_members WHERE list_id=%s", (g.list_id,))
        total = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*) FROM list_members
             WHERE list_id=%s AND is_active=TRUE
        """,
            (g.list_id,),
        )
        active = cur.fetchone()[0]

        cur.execute(
            """
            SELECT COUNT(*) FROM list_members
             WHERE list_id=%s AND is_active=FALSE
        """,
            (g.list_id,),
        )
        inactive = cur.fetchone()[0]

        cur.execute(
            """
            SELECT date_trunc('month', created_at) AS month, COUNT(*)
              FROM list_members
             WHERE list_id=%s
          GROUP BY month
          ORDER BY month DESC
             LIMIT 6
        """,
            (g.list_id,),
        )
        stats = cur.fetchall()

    return render_template(
        "dashboard.html",
        total=total,
        active=active,
        inactive=inactive,
        stats=stats,
        list_name=g.list_name,
        list_address=g.list_address,
    )


# ==== Account Settings ====
@app.get("/account")
@requires_login
def account():
    username = session.get("user")
    user = get_user(username)
    return render_template(
        "account.html",
        username=username,
        first_name=(user["first_name"] if user else ""),
        last_name=(user["last_name"] if user else ""),
        theme=(user["theme"] if user else "auto"),
    )


@app.post("/account")
@requires_login
def account_post():
    username = session.get("user")
    new_pass = (request.form.get("password") or "").strip() or None
    theme = (request.form.get("theme") or "").strip() or None
    first_name = (request.form.get("first_name") or "").strip() or None
    last_name = (request.form.get("last_name") or "").strip() or None
    needs_reset = getattr(g, "password_needs_reset", False)
    current_user = get_user(username) if username else None

    if needs_reset and not new_pass:
        flash(t("password_reset_required"), "error")
        return redirect(url_for("password_reset"))

    if new_pass and current_user and check_password_hash(current_user["password_hash"], new_pass):
        flash(t("password_must_change"), "error")
        if needs_reset:
            return redirect(url_for("password_reset"))
        return redirect(url_for("account"))

    try:
        update_user(
            username,
            password=new_pass,
            theme=theme,
            first_name=first_name,
            last_name=last_name,
            password_needs_reset=False if new_pass else None,
        )
        flash(t("account_settings_updated"), "success")
    except Exception as e:
        flash(t("account_settings_error", error=e), "error")
    return redirect(url_for("account"))


@app.route("/password-reset", methods=["GET", "POST"])
@requires_login
def password_reset():
    ensure_user_password_reset_column()
    username = session.get("user")
    if not username:
        return redirect(url_for("login"))

    user = get_user(username)
    if not user or not user["password_needs_reset"]:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        new_pass = (request.form.get("password") or "").strip()
        confirm = (request.form.get("confirm_password") or "").strip()

        if not new_pass:
            flash(t("password_required"), "error")
        elif new_pass != confirm:
            flash(t("passwords_mismatch"), "error")
        elif check_password_hash(user["password_hash"], new_pass):
            flash(t("password_must_change"), "error")
        else:
            try:
                update_user(
                    username,
                    password=new_pass,
                    password_needs_reset=False,
                )
                flash(t("password_reset_success"), "success")
                return redirect(url_for("dashboard"))
            except Exception as e:
                flash(t("password_reset_error", error=e), "error")

    return render_template("password_reset.html")


@app.route("/set-language", methods=["POST", "GET"])
def set_language():
    lang = (
        request.values.get("language") or request.values.get("lang") or "en"
    ).strip()
    session["language"] = lang
    g.language = lang

    username = session.get("user")
    if username:
        try:
            update_user(username, language=lang)
            flash(t("language_updated"), "success")
        except Exception as e:
            flash(t("language_error", error=e), "error")

    resp = make_response(
        redirect(
            request.referrer or (url_for("dashboard") if username else url_for("login"))
        )
    )
    resp.set_cookie("lang", lang, max_age=60 * 60 * 24 * 365)  # 1 year
    return resp


# ==== Users (admin-only management) ====
@app.get("/users", endpoint="users")
@requires_login
@requires_admin
def users_page():
    sort = request.args.get("sort", "username")
    direction = request.args.get("dir", "asc").lower()

    allowed_sorts = {
        "username",
        "first_name",
        "last_name",
        "role",
        "language",
        "last_login",
        "password_needs_reset",
    }
    if sort not in allowed_sorts:
        sort = "username"
    if direction not in {"asc", "desc"}:
        direction = "asc"

    password_reset_sql = _password_reset_select_sql()
    with get_conn() as c, c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            f"""
            SELECT username, first_name, last_name, role, language, last_login,
                   {password_reset_sql}
            FROM users
            ORDER BY {sort} {direction}
        """
        )
        rows = cur.fetchall()

    return render_template(
        "users.html",
        users=rows,
        sort=sort,
        dir=direction,
    )


@app.template_filter("datetimeformat")
def datetimeformat(value, format="%Y-%m-%d %H:%M:%S"):
    return value.strftime(format) if value else ""


@app.post("/users/add")
@requires_login
@requires_admin
def users_add():
    username = (request.form.get("username") or "").strip()
    first_name = (request.form.get("first_name") or "").strip()
    last_name = (request.form.get("last_name") or "").strip()
    password = (request.form.get("password") or "").strip()
    role = (request.form.get("role") or "user").strip()
    language = (request.form.get("language") or "en").strip()
    needs_reset = (request.form.get("password_needs_reset") or "") in {"1", "on", "true", "True"}

    if not username or not password:
        flash(t("username_password_required"), "error")
        return redirect(url_for("users"))

    try:
        supports_password_reset = ensure_user_password_reset_column()
        if needs_reset and not supports_password_reset:
            raise RuntimeError(
                "password reset enforcement is unavailable until the users schema migration is applied"
            )
        with get_conn() as c, c.cursor() as cur:
            if supports_password_reset:
                cur.execute(
                    """
                    INSERT INTO users (username, password_hash, first_name, last_name, role, theme, language, password_needs_reset)
                    VALUES (%s, %s, %s, %s, %s, 'auto', %s, %s)
                """,
                    (
                        username,
                        generate_password_hash(password),
                        first_name,
                        last_name,
                        role,
                        language,
                        needs_reset,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO users (username, password_hash, first_name, last_name, role, theme, language)
                    VALUES (%s, %s, %s, %s, %s, 'auto', %s)
                """,
                    (
                        username,
                        generate_password_hash(password),
                        first_name,
                        last_name,
                        role,
                        language,
                    ),
                )
        flash(t("user_added", username=username), "success")
    except Exception as e:
        flash(t("user_add_error", error=e), "error")
    return redirect(url_for("users"))


@app.post("/users/edit/<username>")
@requires_login
@requires_admin
def users_edit(username):
    with get_conn() as c, c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT 1 FROM users WHERE username=%s", (username,))
        target = cur.fetchone()

    if not target:
        flash(t("user_not_found", username=username), "error")
        return redirect(url_for("users"))

    new_username = (request.form.get("username") or "").strip()
    first_name = (request.form.get("first_name") or "").strip() or None
    last_name = (request.form.get("last_name") or "").strip() or None
    password = (request.form.get("password") or "").strip() or None
    role = (request.form.get("role") or "user").strip()
    language = (request.form.get("language") or "").strip()
    needs_reset = (request.form.get("password_needs_reset") or "") in {"1", "on", "true", "True"}

    try:
        supports_password_reset = ensure_user_password_reset_column()
        if needs_reset and not supports_password_reset:
            raise RuntimeError(
                "password reset enforcement is unavailable until the users schema migration is applied"
            )
        with get_conn() as c, c.cursor() as cur:
            fields, params = [], []
            if new_username and new_username != username:
                fields.append("username=%s")
                params.append(new_username)
            if first_name is not None:
                fields.append("first_name=%s")
                params.append(first_name)
            if last_name is not None:
                fields.append("last_name=%s")
                params.append(last_name)
            if password:
                fields.append("password_hash=%s")
                params.append(generate_password_hash(password))
            if role:
                fields.append("role=%s")
                params.append(role)
            if language:
                fields.append("language=%s")
                params.append(language)
            if supports_password_reset:
                fields.append("password_needs_reset=%s")
                params.append(needs_reset)

            if fields:
                params.append(username)
                cur.execute(
                    f"UPDATE users SET {', '.join(fields)} WHERE username=%s",
                    tuple(params),
                )
        flash(t("user_updated", username=new_username or username), "success")
    except Exception as e:
        flash(t("user_update_error", error=e), "error")

    return redirect(url_for("users"))


@app.post("/users/delete/<username>")
@requires_login
@requires_admin
def users_delete(username):
    # Check if user exists
    with get_conn() as c, c.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("SELECT 1 FROM users WHERE username=%s", (username,))
        target = cur.fetchone()

    if not target:
        flash(t("user_not_found", username=username), "error")
        return redirect(url_for("users"))

    # prevent self-delete
    if username == session.get("user"):
        flash(t("cannot_delete_self"), "error")
        return redirect(url_for("users"))

    try:
        with get_conn() as c, c.cursor() as cur:
            cur.execute("DELETE FROM users WHERE username=%s", (username,))
        flash(t("user_deleted", username=username), "success")
    except Exception as e:
        flash(t("user_delete_error", error=e), "error")

    return redirect(url_for("users"))


# ==== Config page ====
@app.get("/config")
@requires_login
@requires_admin
def config_page():
    # Load global settings
    global_settings = {}
    try:
        global_settings = load_globals()
    except Exception:
        pass

    global_settings = dict(global_settings or {})
    default_max_bytes_display = _bytes_to_mb_display(10485760)
    max_bytes_display = _bytes_to_mb_display(global_settings.get("MAX_BYTES"))
    global_settings["MAX_BYTES_MB"] = max_bytes_display or default_max_bytes_display
    global_settings["MAX_BYTES_CHOICES"] = ["0", "5", "10", "15", "20", "25"]

    # Load list-specific settings
    list_settings = None
    if g.list_id:
        try:
            list_settings = fetch_list_settings(g.list_id)
            if list_settings:
                # Never send stored mail credentials back to the browser.
                list_settings["imap_pass"] = ""
                list_settings["smtp_pass"] = ""
        except Exception:
            pass

    return render_template(
        "config.html",
        globals=global_settings,
        list=list_settings,
        default_imap_folder=DEFAULT_IMAP_FOLDER,
    )


@app.post("/config/globals")
@requires_login
@requires_admin
def config_globals_update():
    updates = {
        k: request.form.get(k)
        for k in [
            "IDLE_TIMEOUT_SECONDS",
            "ABSOLUTE_TIMEOUT_SECONDS",
            "MAX_SEEN_IDS",
            "MAX_BYTES",
            "DANGEROUS_EXT",
        ]
    }

    max_bytes_mb = updates.get("MAX_BYTES")
    if max_bytes_mb not in (None, ""):
        try:
            max_bytes_value = _mb_to_bytes(max_bytes_mb)
        except (InvalidOperation, ValueError):
            flash(t("config_error", error=t("invalid_max_bytes")), "error")
            return redirect(url_for("config_page"))
        if max_bytes_value < 0:
            flash(t("config_error", error=t("invalid_max_bytes")), "error")
            return redirect(url_for("config_page"))
        updates["MAX_BYTES"] = str(max_bytes_value)

    try:
        with get_conn() as c, c.cursor() as cur:
            for k, v in updates.items():
                cur.execute("UPDATE app_config SET value=%s WHERE key=%s", (v, k))
        flash(t("config_updated"), "success")
    except Exception as e:
        flash(t("config_error", error=e), "error")
    return redirect(url_for("config_page"))


@app.post("/config/list")
@requires_login
@requires_admin
def config_list_update():
    if not g.list_id:
        flash(t("no_list_selected"), "error")
        return redirect(url_for("config_page"))

    name = (request.form.get("name") or "").strip()
    address = (request.form.get("address") or "").strip()
    is_active = request.form.get("is_active") == "1"
    subject_tag = (request.form.get("subject_tag") or "").strip() or None
    open_posting = request.form.get("open_posting") == "1"

    try:
        ensure_imap_folder_column()
        current_settings = fetch_list_settings(g.list_id)
        if not current_settings:
            flash(t("no_list_selected"), "error")
            return redirect(url_for("config_page"))

        imap_host = (request.form.get("imap_host") or "").strip()
        imap_port = int(request.form.get("imap_port") or 993)
        imap_user = (request.form.get("imap_user") or "").strip()
        imap_pass_raw = (request.form.get("imap_pass") or "").strip()
        imap_pass = (
            imap_pass_raw if imap_pass_raw else current_settings.get("imap_pass")
        )
        imap_folder_raw = (request.form.get("imap_folder") or "").strip()
        imap_folder = imap_folder_raw or DEFAULT_IMAP_FOLDER

        smtp_host = (request.form.get("smtp_host") or "").strip()
        smtp_port = int(request.form.get("smtp_port") or 465)
        smtp_user = (request.form.get("smtp_user") or "").strip()
        smtp_pass_raw = (request.form.get("smtp_pass") or "").strip()
        smtp_pass = (
            smtp_pass_raw if smtp_pass_raw else current_settings.get("smtp_pass")
        )

        with get_conn() as c:
            update_with_folder = """
                UPDATE mailing_lists
                   SET name=%s,
                       address=%s,
                       is_active=%s,
                       subject_tag=%s,
                       open_posting=%s,
                       imap_host=%s,
                       imap_port=%s,
                       imap_user=%s,
                       imap_pass=%s,
                       imap_folder=%s,
                       smtp_host=%s,
                       smtp_port=%s,
                       smtp_user=%s,
                       smtp_pass=%s
                 WHERE id=%s
            """

            params_with_folder = (
                name,
                address,
                is_active,
                subject_tag,
                open_posting,
                imap_host,
                imap_port,
                imap_user,
                imap_pass,
                imap_folder,
                smtp_host,
                smtp_port,
                smtp_user,
                smtp_pass,
                g.list_id,
            )

            try:
                with c.cursor() as cur:
                    cur.execute(update_with_folder, params_with_folder)
            except errors.UndefinedColumn:
                c.rollback()
                update_without_folder = """
                    UPDATE mailing_lists
                       SET name=%s,
                           address=%s,
                           is_active=%s,
                           subject_tag=%s,
                           open_posting=%s,
                           imap_host=%s,
                           imap_port=%s,
                           imap_user=%s,
                           imap_pass=%s,
                           smtp_host=%s,
                           smtp_port=%s,
                           smtp_user=%s,
                           smtp_pass=%s
                     WHERE id=%s
                """
                params_without_folder = (
                    name,
                    address,
                    is_active,
                    subject_tag,
                    open_posting,
                    imap_host,
                    imap_port,
                    imap_user,
                    imap_pass,
                    smtp_host,
                    smtp_port,
                    smtp_user,
                    smtp_pass,
                    g.list_id,
                )
                with c.cursor() as cur:
                    cur.execute(update_without_folder, params_without_folder)
        flash(t("list_settings_updated"), "success")
    except Exception as e:
        flash(t("list_settings_error", error=e), "error")

    return redirect(url_for("config_page"))


@app.get("/logs")
@requires_login
@requires_admin
def logs_page():
    sections = []
    for label, path in (
        ("mlist.log", LOG_FILE_PATH),
        ("ensure-list-admin.log", ENSURE_LOG_FILE_PATH),
    ):
        try:
            lines = _read_log_tail(path=path)
            sections.append(dict(label=label, lines=lines, path=path, error=None))
        except FileNotFoundError:
            sections.append(dict(label=label, lines=[], path=path, error=t("log_missing")))
        except Exception as e:
            sections.append(dict(label=label, lines=[], path=path, error=t("log_read_error", error=e)))

    return render_template("logs.html", log_sections=sections)


# ==== List switcher ====
@app.post("/set-list")
@requires_login
def set_list():
    list_id = request.form.get("list_id")
    try:
        if list_id:
            session["list_id"] = int(list_id)
            flash(t("list_switched"), "success")
    except Exception as e:
        flash(t("list_switch_error", error=e), "error")
    return redirect(request.referrer or url_for("dashboard"))


# ==== Run ====
if __name__ == "__main__":
    server_options = _load_server_options()
    app.run(
        host=server_options.host,
        port=server_options.port,
        debug=server_options.debug,
        use_reloader=server_options.reload,
        use_debugger=server_options.debug and server_options.debugger,
    )
