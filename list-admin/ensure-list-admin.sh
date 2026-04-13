#!/usr/bin/env bash
set -eo pipefail
set -o errtrace
set +u

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
DEFAULT_HOME="${HOME:-$SCRIPT_DIR}"
HOME="${LIST_ADMIN_HOME:-$DEFAULT_HOME}"
APP_DIR="${LIST_ADMIN_APP_DIR:-$SCRIPT_DIR}"
VENV="${LIST_ADMIN_VENV:-$APP_DIR/.venv}"
APP_NAME="${LIST_ADMIN_APP_NAME:-list-admin}"
PY="${LIST_ADMIN_PYTHON:-$VENV/bin/python}"
HOST="${LIST_ADMIN_HOST:-127.0.0.1}"
PORT="${LIST_ADMIN_PORT:-9010}"
HEALTH_URL="${LIST_ADMIN_HEALTH_URL:-http://$HOST:$PORT/}"

cd "$APP_DIR"

LOG_ENABLE=1
LOG_ALL=0

for arg in "$@"; do
	case "$arg" in
	--log)
		LOG_ENABLE=1
		;;
	--log-all)
		LOG_ENABLE=1
		LOG_ALL=1
		;;
	*) ;;
	esac
done

export HOME
export APP_DIR
export APP_NAME
export VENV
export PY
export HOST
export PORT
export LIST_ADMIN_HOST="$HOST"
export LIST_ADMIN_PORT="$PORT"
export HEALTH_URL
export LOG_ENABLE
export LOG_ALL
export PM2_HOME="$HOME/.pm2"
export PATH="$VENV/bin:$HOME/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

PM2_TIMEOUT=${PM2_TIMEOUT:-20}
HEALTH_TIMEOUT=${HEALTH_TIMEOUT:-5}
HEALTH_RETRIES=${HEALTH_RETRIES:-2}
STARTUP_WAIT_SECONDS=${STARTUP_WAIT_SECONDS:-30}
HEALTH_CHECK_INTERVAL=${HEALTH_CHECK_INTERVAL:-1}
PM2_AUTO_UPDATE=${PM2_AUTO_UPDATE:-1}

LOG_DIR="$APP_DIR/logs"
LOG_FILE="$LOG_DIR/ensure-list-admin.log"
LOG_MAX_ARCHIVES=${LOG_MAX_ARCHIVES:-7}

export LOG_DIR
export LOG_FILE
export LOG_MAX_ARCHIVES

_ts() {
	date '+%Y-%m-%d %H:%M:%S%z'
}

log_to_file() {
	[[ ${LOG_ENABLE:-0} -eq 1 ]] || return 0
	{ printf '[%s] %s\n' "$(_ts)" "$*"; } >>"$LOG_FILE" 2>/dev/null || true
}

log_info() {
	log_to_file "INFO: $*"
	return 0
}

log_warn() {
	log_to_file "WARN: $*"
	return 0
}

log_error() {
	log_to_file "ERROR: $*"
	return 0
}

log_debug() {
	if [[ ${LOG_ALL:-0} -eq 1 ]]; then
		log_to_file "DEBUG: $*"
	fi
	return 0
}

say() {
	local msg="$*"
	log_info "$msg"
	if [[ -t 1 ]]; then
		printf '%s\n' "$msg"
	fi
}

say_warn() {
	local msg="$*"
	log_warn "$msg"
	if [[ -t 1 ]]; then
		printf '%s\n' "$msg"
	fi
}

say_error() {
	local msg="$*"
	log_error "$msg"
	if [[ -t 1 ]]; then
		printf '%s\n' "$msg"
	fi
}

trap 'rc=$?;
  msg="ensure-list-admin failed (exit $rc) at $0:$LINENO: ${BASH_COMMAND}"
  log_error "$msg"
  printf "%s\n" "$msg" >&2
  exit $rc' ERR

trap 'rc=$?; [[ $rc -eq 0 ]] || log_warn "Run finished with error (exit $rc)"' EXIT

_prune_old_archives() {
	local keep="$1"
	[[ "$keep" =~ ^[0-9]+$ ]] || return 0
	((keep > 0)) || return 0
	if compgen -G "${LOG_FILE}".* >/dev/null; then
		local -a archives=()
		mapfile -t archives < <(ls -1t "${LOG_FILE}".* 2>/dev/null)
		local total=${#archives[@]}
		if ((total > keep)); then
			for old in "${archives[@]:$keep}"; do
				rm -f -- "$old"
			done
		fi
	fi
}

if [[ ${LOG_ENABLE:-0} -eq 1 ]]; then
	mkdir -p "$LOG_DIR"
	[[ -f "$LOG_FILE" ]] || touch "$LOG_FILE"

	if [[ -f "$LOG_FILE" ]] && [[ $(wc -c <"$LOG_FILE") -ge 1048576 ]]; then
		mv -f "$LOG_FILE" "$LOG_FILE.$(date +%Y%m%d-%H%M%S)"
		: >"$LOG_FILE" 2>/dev/null || true
	fi

	_prune_old_archives "$LOG_MAX_ARCHIVES"

	if [[ ${LOG_ALL:-0} -eq 1 ]]; then
		exec >>"$LOG_FILE" 2>&1
		echo
		echo "[$(_ts)] ===== ensure run start (log-all) ====="
	fi
fi

if command -v timeout >/dev/null 2>&1; then
	HAVE_TIMEOUT=1
else
	HAVE_TIMEOUT=0
fi

CURL_CAN_RETRY_CONNREFUSED=0
if command -v curl >/dev/null 2>&1; then
	curl_help_output="$({ curl --help; } 2>&1 || true)"
	if [[ "$curl_help_output" == *"--retry-connrefused"* ]]; then
		CURL_CAN_RETRY_CONNREFUSED=1
	fi
fi

with_timeout() {
	local timeout_secs="$1"
	shift
	if ((timeout_secs > 0)) && [[ ${HAVE_TIMEOUT:-0} -eq 1 ]]; then
		timeout "$timeout_secs" "$@"
	else
		"$@"
	fi
}

run_pm2_capture() {
	local action="$1"
	shift
	local -a args=("$@")
	local output=""
	if output=$(with_timeout "$PM2_TIMEOUT" pm2 "$action" "${args[@]}" 2>&1); then
		printf '%s' "$output"
		return 0
	fi
	local rc=$?
	printf '%s' "$output"
	return $rc
}

run_pm2() {
	local action="$1"
	shift
	local -a args=("$@")
	local output=""
	if output=$(run_pm2_capture "$action" "${args[@]}"); then
		local pretty=""
		if ((${#args[@]})); then
			pretty=" ${args[*]}"
		fi
		log_debug "pm2 $action${pretty} output: $output"
		return 0
	fi

	local rc=$?
	local pretty=""
	if ((${#args[@]})); then
		pretty=" ${args[*]}"
	fi
	if ((rc == 124)); then
		log_error "pm2 $action${pretty} timed out after ${PM2_TIMEOUT}s"
	else
		log_error "pm2 $action${pretty} failed (rc=$rc${output:+, output: $output})"
	fi
	return $rc
}

pm2_pid_safe() {
	local output=""
	if output=$(run_pm2_capture pid "$APP_NAME"); then
		log_debug "pm2 pid output: $output"
		printf '%s\n' "$output"
		return 0
	fi
	local rc=$?
	if ((rc == 124)); then
		log_warn "pm2 pid $APP_NAME timed out after ${PM2_TIMEOUT}s"
	else
		log_warn "pm2 pid $APP_NAME failed (rc=$rc${output:+, output: $output})"
	fi
	return $rc
}

trim() {
	local s="$*"
	s="${s#"${s%%[![:space:]]*}"}"
	s="${s%"${s##*[![:space:]]}"}"
	printf '%s' "$s"
}

version_ge() {
	local a="$1" b="$2"
	[[ -z "$a" || -z "$b" ]] && return 1
	[[ "$a" == "$b" ]] && return 0

	local first
	first=$(
		{
			printf '%s\n' "$a"
			printf '%s\n' "$b"
		} | sort -V | sed -n '1p'
	) || return 1

	[[ "$first" == "$b" ]]
}

pm2_versions() {
	local out mem_ver local_ver
	out=$(with_timeout "$PM2_TIMEOUT" pm2 list 2>&1 || true)

	mem_ver=$(printf '%s\n' "$out" | sed -n 's/^In memory PM2 version:[[:space:]]*//p' | sed -n '1p')
	local_ver=$(printf '%s\n' "$out" | sed -n 's/^Local PM2 version:[[:space:]]*//p' | sed -n '1p')

	mem_ver=$(trim "$mem_ver")
	local_ver=$(trim "$local_ver")

	printf '%s|%s\n' "$mem_ver" "$local_ver"
}

ensure_pm2_daemon_current() {
	[[ "${PM2_AUTO_UPDATE:-1}" == "1" ]] || return 0

	local versions mem_ver local_ver
	versions="$(pm2_versions || true)"
	mem_ver="${versions%%|*}"
	local_ver="${versions##*|}"

	if [[ -z "$mem_ver" || -z "$local_ver" ]]; then
		log_debug "Could not determine PM2 versions automatically; skipping auto-update check"
		return 0
	fi

	log_debug "PM2 version check: in-memory=$mem_ver local=$local_ver"

	if [[ "$local_ver" != "$mem_ver" ]] && version_ge "$local_ver" "$mem_ver"; then
		say "PM2 daemon is outdated ($mem_ver < $local_ver); refreshing..."
		if run_pm2 update; then
			log_info "pm2 update succeeded"
			if ! run_pm2 save --force; then
				log_warn "pm2 save --force failed after pm2 update"
			fi
		else
			log_warn "pm2 update failed; continuing with existing daemon"
		fi
	fi

	return 0
}

health_ok() {
	if command -v curl >/dev/null 2>&1; then
		local -a curl_args=(-fsS --max-time "$HEALTH_TIMEOUT")
		if ((HEALTH_RETRIES > 0)); then
			curl_args+=(--retry "$HEALTH_RETRIES" --retry-delay 1)
			((CURL_CAN_RETRY_CONNREFUSED == 1)) && curl_args+=(--retry-connrefused)
		fi
		curl "${curl_args[@]}" "$HEALTH_URL" >/dev/null
	else
		if command -v sockstat >/dev/null 2>&1; then
			sockstat -4 -6 | grep -q "$PORT"
		else
			ss -ltn 2>/dev/null | grep -q ":$PORT "
		fi
	fi
}

wait_for_health() {
	local wait_secs="${1:-$STARTUP_WAIT_SECONDS}"
	local interval="${HEALTH_CHECK_INTERVAL:-1}"
	local deadline=0

	if health_ok; then
		return 0
	fi

	[[ "$wait_secs" =~ ^[0-9]+$ ]] || wait_secs=30
	[[ "$interval" =~ ^[0-9]+$ ]] || interval=1
	((wait_secs > 0)) || return 1
	((interval > 0)) || interval=1

	deadline=$((SECONDS + wait_secs))
	while ((SECONDS < deadline)); do
		sleep "$interval"
		if health_ok; then
			return 0
		fi
	done

	return 1
}

if [[ ! -x "$PY" ]]; then
	say "!! Virtualenv Python not found at $PY"
	log_error "venv Python not found at $PY"
	exit 1
fi

if [[ ! -f "$VENV/bin/activate" ]]; then
	say "!! Activate script not found at $VENV/bin/activate"
	log_error "activate script not found at $VENV/bin/activate"
	exit 1
fi

if ! command -v pm2 >/dev/null 2>&1; then
	say "!! pm2 not found in PATH"
	log_error "pm2 not found in PATH"
	exit 1
fi

# shellcheck disable=SC1091
source "$VENV/bin/activate"

ensure_pm2_daemon_current

PID=""
if PID=$(pm2_pid_safe); then
	PID=${PID//$'\r'/}
	PID=${PID//$'\n'/}
else
	PID=""
fi
[[ "$PID" =~ ^[0-9]+$ ]] || PID=""

if [[ -n "$PID" && "$PID" != "0" ]]; then
	if health_ok; then
		log_info "Healthy (PID $PID). Nothing to do."
		exit 0
	fi

	log_warn "Running (PID $PID) but not healthy yet; waiting up to ${STARTUP_WAIT_SECONDS}s before restart."
	if wait_for_health; then
		say "OK: process became healthy without restart."
		log_info "Healthy (PID $PID) after startup grace period."
		exit 0
	fi

	say_warn "WARN: running but unhealthy; restarting…"
	log_warn "Running (PID $PID) still unhealthy after grace period; restarting with pm2."

	if ! run_pm2 restart "$APP_NAME" --update-env; then
		say_error "ERROR: pm2 restart failed."
		exit 1
	fi
	log_info "pm2 restart $APP_NAME succeeded"

	if ! run_pm2 save; then
		log_warn "pm2 save failed after restart"
	fi

	log_info "Waiting up to ${STARTUP_WAIT_SECONDS}s for health check after restart."
	if wait_for_health; then
		say "OK after restart."
		exit 0
	fi

	say_warn "WARN: still not responding on $HEALTH_URL"
	log_error "Health check failed after restart"
	exit 1
fi

say "-> $APP_NAME not running; starting…"
log_info "Not running; starting via pm2…"

if ! run_pm2 start "$APP_DIR/app.py" \
	--name "$APP_NAME" \
	--cwd "$APP_DIR" \
	--interpreter "$PY" \
	-- --no-debug --no-reload --no-debugger; then
	say_error "ERROR: pm2 start failed."
	exit 1
fi
log_info "pm2 start $APP_NAME succeeded"

if ! run_pm2 save; then
	log_warn "pm2 save failed after start"
fi

log_info "Waiting up to ${STARTUP_WAIT_SECONDS}s for health check after fresh start."
if wait_for_health; then
	say "OK: started and responding."
	exit 0
fi

say_warn "WARN: started but not responding on $HEALTH_URL"
log_error "Health check failed after fresh start"
exit 1
