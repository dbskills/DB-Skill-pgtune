#!/usr/bin/env python3
"""Long-lived HTTP server exposing rule-based PostgreSQL knob recommendations."""

import json
import os
import re
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg2
from psycopg2.extras import RealDictCursor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
JOURNAL_DIR = os.path.join(BASE_DIR, "skill_state")
RULE_SCRIPT = os.path.join(BASE_DIR, "pgtune_calc.mjs")
LOADER_SCRIPT = os.path.join(BASE_DIR, "pgtune_loader.mjs")
NODE_BIN = os.environ.get("NODE_BIN", "node")

DB_TYPES = {"web", "oltp", "dw", "desktop", "mixed"}
OS_TYPES = {"linux", "windows", "mac"}
HD_TYPES = {"ssd", "hdd", "san", "nvme"}
DB_SIZES = {"less_ram", "mid_ram", "greater_ram"}

BUDGET_KEYS = {"time_budget_s", "max_rounds", "max_restarts"}
CONFIG_KEYS = {
    "db_type", "os_type", "hd_type", "db_size", "cpu_num",
    "total_memory_mb", "connection_num", "db_version", "tuning_scope",
    "restart_command", "restart_wait_s", "concurrency", "duration_s",
    "seed", "state_dir",
}

DEFAULT_STATE_DIR = os.environ.get(
    "PGTUNE_STATE_DIR", os.path.join(BASE_DIR, "skill_state")
)
DEFAULTS = {
    "db_type": "web",
    "os_type": "linux",
    "hd_type": "ssd",
    "db_size": "mid_ram",
    "cpu_num": None,
    "total_memory_mb": None,
    "connection_num": None,
    "db_version": None,
    "tuning_scope": None,
    "restart_command": None,
    "restart_wait_s": 60,
    "concurrency": 1,
    "duration_s": None,
    "seed": None,
    "state_dir": "skill_state",
}

SEMANTIC_PREFIXES = ("lc_", "default_transaction_")
SEMANTIC_KNOBS = {"timezone", "datestyle", "intervalstyle", "search_path"}
BYTES = {"b": 1, "kb": 1024, "mb": 1024 ** 2, "gb": 1024 ** 3, "tb": 1024 ** 4, "pb": 1024 ** 5}
TIME_MS = {"us": 0.001, "ms": 1, "s": 1000, "min": 60000, "h": 3600000, "d": 86400000}
KNOB_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")

STATE_LOCK = threading.RLock()
STATE_CACHE = {}
JOURNAL_LOCK = threading.Lock()
SHUTDOWN_FLAG = threading.Event()
SERVER_REF = {"server": None}


class BadRequest(Exception):
    pass


def _as_dict(value, name):
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            raise BadRequest(f"{name} must be a JSON object")
        if not isinstance(parsed, dict):
            raise BadRequest(f"{name} must be a JSON object")
        return parsed
    raise BadRequest(f"{name} must be a JSON object")


def _to_int(value, name, minimum=None):
    if isinstance(value, bool):
        raise BadRequest(f"{name} must be an integer")
    if isinstance(value, int):
        result = value
    elif isinstance(value, float):
        if not value.is_integer():
            raise BadRequest(f"{name} must be an integer")
        result = int(value)
    elif isinstance(value, str):
        text = value.strip()
        if not re.fullmatch(r"[+-]?\d+", text):
            raise BadRequest(f"{name} must be an integer")
        result = int(text)
    else:
        raise BadRequest(f"{name} must be an integer")
    if minimum is not None and result < minimum:
        raise BadRequest(f"{name} must be >= {minimum}")
    return result


def _to_float(value, name, minimum=None):
    if isinstance(value, bool):
        raise BadRequest(f"{name} must be a number")
    if isinstance(value, (int, float)):
        result = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", text):
            raise BadRequest(f"{name} must be a number")
        result = float(text)
    else:
        raise BadRequest(f"{name} must be a number")
    if minimum is not None and result < minimum:
        raise BadRequest(f"{name} must be >= {minimum}")
    return result


def _parse_scope(value):
    if not isinstance(value, list):
        raise BadRequest("tuning_scope must be a list")
    result = []
    seen = set()
    for item in value:
        if isinstance(item, str):
            knob = item.strip()
            low = None
            high = None
        elif isinstance(item, dict):
            knob = item.get("knob")
            if not isinstance(knob, str):
                raise BadRequest("each tuning_scope object needs a string knob")
            knob = knob.strip()
            low = item.get("min")
            high = item.get("max")
            if low is not None:
                low = _to_float(low, "tuning_scope.min")
            if high is not None:
                high = _to_float(high, "tuning_scope.max")
            if low is not None and high is not None and low > high:
                raise BadRequest("tuning_scope min must not exceed max")
        else:
            raise BadRequest("each tuning_scope item must be a knob name or object")
        if not KNOB_RE.fullmatch(knob):
            raise BadRequest(f"invalid knob name in tuning_scope: {knob!r}")
        key = knob.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append({"knob": key, "min": low, "max": high})
    return result


def _parse_config(raw):
    cfg = dict(DEFAULTS)
    data = _as_dict(raw, "config")
    for key in data:
        if key in BUDGET_KEYS:
            raise BadRequest(f"budget key '{key}' must be supplied via budget")
    for key, value in data.items():
        if key not in CONFIG_KEYS:
            continue
        if key in ("db_type", "os_type", "hd_type", "db_size"):
            if not isinstance(value, str) or value not in {
                "db_type": DB_TYPES, "os_type": OS_TYPES,
                "hd_type": HD_TYPES, "db_size": DB_SIZES,
            }[key]:
                raise BadRequest(f"invalid {key}")
            cfg[key] = value
        elif key in ("cpu_num", "total_memory_mb", "connection_num", "db_version"):
            if value is None:
                cfg[key] = None
            elif key == "db_version":
                cfg[key] = _to_float(value, key, 9.5)
            elif key == "cpu_num":
                cfg[key] = _to_int(value, key, 1)
            elif key == "total_memory_mb":
                cfg[key] = _to_int(value, key, 512)
            else:
                cfg[key] = _to_int(value, key, 20)
        elif key == "tuning_scope":
            cfg[key] = None if value is None else _parse_scope(value)
        elif key == "restart_command":
            if value is not None and not isinstance(value, str):
                raise BadRequest("restart_command must be a string")
            cfg[key] = value
        elif key == "restart_wait_s":
            cfg[key] = _to_int(value, key, 1)
        elif key == "concurrency":
            cfg[key] = _to_int(value, key, 1)
        elif key == "duration_s":
            cfg[key] = None if value is None else _to_float(value, key, 0.0)
        elif key == "seed":
            cfg[key] = None if value is None else _to_int(value, key)
        elif key == "state_dir":
            if not isinstance(value, str) or not value.strip():
                raise BadRequest("state_dir must be a non-empty string")
            cfg[key] = value.strip()
    return cfg


def _parse_budget(raw):
    budget = {"time_budget_s": None, "max_rounds": None, "max_restarts": None}
    data = _as_dict(raw, "budget")
    for key in data:
        if key in CONFIG_KEYS:
            raise BadRequest(f"config key '{key}' must be supplied via config")
    for key, value in data.items():
        if key not in BUDGET_KEYS or value is None:
            continue
        if key == "time_budget_s":
            budget[key] = _to_float(value, key, 0.0)
        else:
            budget[key] = _to_int(value, key, 0)
    return budget


def _connect(dsn, readonly=False):
    conn = psycopg2.connect(dsn, connect_timeout=5)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = 30000")
        if readonly:
            cur.execute("SET default_transaction_read_only = on")
    return conn


def _read_settings(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT name, setting, unit, context, vartype, min_val, max_val, "
            "enumvals, pending_restart FROM pg_settings WHERE context <> 'internal'"
        )
        return {row["name"]: dict(row) for row in cur.fetchall()}


def _server_version(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT current_setting('server_version_num')::int")
        num = int(cur.fetchone()[0])
    major, minor = num // 10000, (num // 100) % 100
    return float(major) if major >= 10 else float(f"{major}.{minor}")


def _host_memory_mb():
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        pass
    return 1024


def _is_semantic(name):
    return name in SEMANTIC_KNOBS or name.startswith(SEMANTIC_PREFIXES)


def _unit_bytes(unit):
    text = (unit or "").strip()
    match = re.fullmatch(r"(\d+)\s*(kB|MB|GB|TB|PB)", text)
    if match:
        return int(match.group(1)) * BYTES[match.group(2).lower()]
    return BYTES.get(text.lower())


def _setting_number(value, row):
    text = str(value).strip()
    match = re.fullmatch(r"([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*([A-Za-z]+)?", text)
    if not match:
        return None
    number = float(match.group(1))
    suffix = (match.group(2) or "").lower()
    unit = (row.get("unit") or "").strip()
    if unit:
        unit_bytes = _unit_bytes(unit)
        if unit_bytes is not None:
            if not suffix:
                return number
            value_bytes = BYTES.get(suffix)
            return None if value_bytes is None else number * value_bytes / unit_bytes
        unit_ms = TIME_MS.get(unit.lower())
        if unit_ms is not None:
            if not suffix:
                return number
            value_ms = TIME_MS.get(suffix)
            return None if value_ms is None else number * value_ms / unit_ms
        return None
    return None if suffix else number


def _value_valid(row, value):
    vartype = (row.get("vartype") or "").lower()
    if vartype == "bool":
        if str(value).strip().lower() in {"on", "off", "true", "false", "yes", "no", "1", "0"}:
            return True, None
        return False, "not a boolean"
    if vartype == "enum":
        allowed = [str(item) for item in (row.get("enumvals") or [])]
        if not allowed or str(value) in allowed:
            return True, None
        return False, "not in enumvals"
    if vartype in ("integer", "real"):
        number = _setting_number(value, row)
        if number is None:
            return False, "not numeric for this unit"
        low = _to_float(row.get("min_val"), "min_val") if row.get("min_val") else None
        high = _to_float(row.get("max_val"), "max_val") if row.get("max_val") else None
        if low is not None and number < low:
            return False, "below min_val"
        if high is not None and number > high:
            return False, "above max_val"
        return True, None
    return True, None


def _ddl(name, value):
    return "ALTER SYSTEM SET %s = '%s';" % (name, str(value).replace("'", "''"))


def _run_rules(hardware):
    payload = json.dumps(hardware)
    proc = subprocess.run(
        [NODE_BIN, "--no-warnings", "--experimental-loader", LOADER_SCRIPT, RULE_SCRIPT],
        input=payload, capture_output=True, text=True, timeout=30,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "rule engine failed").strip().splitlines()
        raise RuntimeError(detail[-1] if detail else "rule engine failed")
    return json.loads(proc.stdout)


def _require_str(req, key):
    value = req.get(key)
    if not isinstance(value, str) or not value.strip():
        raise BadRequest(f"missing required parameter: {key}")
    return value.strip()


def _resolve_state_dir(path):
    return path if os.path.isabs(path) else os.path.join(BASE_DIR, path)


def _state_path(state_dir):
    return os.path.join(state_dir, "state.json")


def _load_state(state_dir):
    with STATE_LOCK:
        if state_dir in STATE_CACHE:
            return dict(STATE_CACHE[state_dir])
        state = {
            "requests": 0,
            "last_objective": None,
            "active_scope": None,
            "last_recommendation_count": 0,
            "last_eval_mode": None,
        }
        try:
            with open(_state_path(state_dir), "r", encoding="utf-8") as handle:
                stored = json.load(handle)
            if isinstance(stored, dict):
                state.update({key: stored[key] for key in state if key in stored})
        except (OSError, ValueError):
            pass
        STATE_CACHE[state_dir] = state
        return dict(state)


def _save_state(state_dir, state):
    path = _state_path(state_dir)
    temp = path + ".tmp"
    try:
        os.makedirs(state_dir, exist_ok=True)
        with open(temp, "w", encoding="utf-8") as handle:
            json.dump(state, handle, sort_keys=True)
        os.replace(temp, path)
    except OSError:
        pass


LAST_STATE = {"dir": DEFAULT_STATE_DIR}


def _record_state(state_dir, objective, scope, count, eval_mode):
    with STATE_LOCK:
        state = _load_state(state_dir)
        state["requests"] = int(state.get("requests", 0)) + 1
        state["last_objective"] = objective
        state["active_scope"] = scope
        state["last_recommendation_count"] = count
        state["last_eval_mode"] = eval_mode
        STATE_CACHE[state_dir] = dict(state)
        _save_state(state_dir, state)
        LAST_STATE["dir"] = state_dir


def _state_summary():
    with STATE_LOCK:
        state_dir = LAST_STATE["dir"]
    state = _load_state(state_dir)
    summary = {
        "idle": True,
        "mode": "inference-only",
        "state_dir": state_dir,
    }
    summary.update({key: state.get(key) for key in (
        "requests", "last_objective", "active_scope",
        "last_recommendation_count", "last_eval_mode",
    )})
    return summary


def _journal_path(state_dir):
    return os.path.join(JOURNAL_DIR, "restore_journal.json")


def _write_journal(state_dir, dsn, settings):
    payload = {
        "dsn": dsn,
        "completed": False,
        "touched": [],
        "snapshot": {name: row.get("setting") for name, row in settings.items()},
    }
    path = _journal_path(state_dir)
    temp = path + ".tmp"
    with JOURNAL_LOCK:
        try:
            os.makedirs(JOURNAL_DIR, exist_ok=True)
            with open(temp, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
            os.replace(temp, path)
        except OSError:
            pass


def _complete_journal(state_dir):
    path = _journal_path(state_dir)
    with JOURNAL_LOCK:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, ValueError):
            return
        payload["completed"] = True
        temp = path + ".tmp"
        try:
            with open(temp, "w", encoding="utf-8") as handle:
                json.dump(payload, handle)
            os.replace(temp, path)
        except OSError:
            pass


def _replay_journal(state_dir):
    path = _journal_path(state_dir)
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError):
        return
    if not payload.get("completed"):
        touched = payload.get("touched") or []
        dsn = payload.get("dsn")
        if touched and dsn:
            try:
                conn = _connect(dsn)
                try:
                    with conn.cursor() as cur:
                        for item in touched:
                            cur.execute(_ddl(item["knob"], item["value"]))
                        cur.execute("SELECT pg_reload_conf()")
                finally:
                    conn.close()
            except Exception as exc:
                print(f"[journal] restore failed: {type(exc).__name__}: {exc}", flush=True)
                return
    try:
        os.remove(path)
    except OSError:
        pass


def _degrade_payload(objective, eval_mode, budget, config, note):
    return {
        "recommended_knobs": [],
        "metadata": {
            "strategy_type": "rule-based",
            "optimization_time": 0.0,
            "objective": objective,
            "num_rounds": 0,
            "budget_used": dict(budget, time_used_s=0.0, rounds_used=0, restarts_used=0),
            "eval_mode": eval_mode,
            "excluded_restart_knobs": [],
            "mode": "inference-only",
            "config": config,
            "degrade_note": note,
        },
    }


def _build_entries(rules, settings, config, eval_mode, budget):
    scope = config["tuning_scope"]
    scope_names = {item["knob"] for item in scope} if scope is not None else None
    rule_names = set()
    entries = []
    skipped = []
    for item in rules.get("settings") or []:
        name = item.get("key")
        if not isinstance(name, str):
            continue
        rule_names.add(name)
        row = settings.get(name)
        if row is None:
            skipped.append({"knob": name, "reason": "not present on target"})
            continue
        if _is_semantic(name):
            skipped.append({"knob": name, "reason": "result-semantic setting"})
            continue
        valid, reason = _value_valid(row, item.get("value"))
        if not valid:
            skipped.append({"knob": name, "reason": reason})
            continue
        if scope_names is not None and name.lower() not in scope_names:
            continue
        entries.append({
            "knob": name,
            "value": str(item.get("value")),
            "ddl": _ddl(name, item.get("value")),
            "restart_required": row.get("context") == "postmaster",
            "current_value": row.get("setting"),
        })

    restart_allowed = eval_mode == "dedicated" and (
        budget["max_restarts"] is None or budget["max_restarts"] > 0
    ) and bool(config["restart_command"])
    excluded_restart = []
    if not restart_allowed:
        kept = []
        for entry in entries:
            if entry["restart_required"]:
                excluded_restart.append(entry["knob"])
            else:
                kept.append(entry)
        entries = kept
    return entries, skipped, excluded_restart, scope_names, rule_names


def _degrade_note(entries, scope, scope_names, rule_names, excluded_restart):
    if scope is not None and not scope:
        return "tuning_scope is empty: no tunable knobs"
    if scope_names is not None and not (scope_names & rule_names):
        return "no recommended knob matches tuning_scope"
    if not entries and excluded_restart:
        return "all recommended knobs in scope require a server restart and were excluded"
    if not entries:
        return "no applicable knob recommendation for this target"
    return None


def _parse_apply_scope(value):
    if value is None or value == "":
        return "dynamic"
    if not isinstance(value, str):
        raise BadRequest("apply_scope must be a string")
    text = value.strip()
    if text in ("dynamic", "all"):
        return text
    names = []
    for part in text.split(","):
        name = part.strip()
        if not name:
            continue
        if not KNOB_RE.fullmatch(name):
            raise BadRequest(f"invalid knob name in apply_scope: {name!r}")
        names.append(name.lower())
    if not names:
        raise BadRequest("apply_scope is empty")
    return names


def _apply(conn, entries, apply_scope):
    if apply_scope == "dynamic":
        selected = [entry for entry in entries if not entry["restart_required"]]
    elif apply_scope == "all":
        selected = list(entries)
    else:
        names = set(apply_scope)
        selected = [entry for entry in entries if entry["knob"].lower() in names]

    results = []
    applied = False
    for entry in selected:
        try:
            with conn.cursor() as cur:
                cur.execute(entry["ddl"])
            applied = True
            results.append({
                "knob": entry["knob"],
                "status": "applied",
                "error": None,
                "restart_pending": entry["restart_required"],
            })
        except Exception as exc:
            results.append({
                "knob": entry["knob"],
                "status": "failed",
                "error": str(exc)[:200],
                "restart_pending": entry["restart_required"],
            })
    if applied:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_reload_conf()")
        except Exception:
            pass
    return results


def _tune(req):
    started = time.time()
    dsn = _require_str(req, "dsn")
    if "workload" not in req or req.get("workload") is None:
        raise BadRequest("missing required parameter: workload")
    objective = _require_str(req, "objective")
    config = _parse_config(req.get("config"))
    budget = _parse_budget(req.get("budget"))

    eval_dsn = req.get("eval_dsn")
    dedicated = isinstance(eval_dsn, str) and bool(eval_dsn.strip()) and eval_dsn.strip() != dsn
    eval_mode = "dedicated" if dedicated else "in-place"

    apply_flag = req.get("apply", False)
    if not isinstance(apply_flag, bool):
        raise BadRequest("apply must be a boolean")
    apply_scope = _parse_apply_scope(req.get("apply_scope")) if apply_flag else None
    state_dir = _resolve_state_dir(config["state_dir"])

    try:
        conn = _connect(dsn)
    except Exception as exc:
        note = f"database connection failed: {type(exc).__name__}: {exc}"
        payload = _degrade_payload(objective, eval_mode, budget, config, note[:300])
        _record_state(state_dir, objective, None, 0, eval_mode)
        return payload

    try:
        settings = _read_settings(conn)
        _write_journal(state_dir, dsn, settings)
        db_version = config["db_version"]
        if db_version is None:
            db_version = _server_version(conn)
        connection_num = config["connection_num"]
        if connection_num is None:
            try:
                connection_num = int(settings["max_connections"]["setting"])
            except (KeyError, TypeError, ValueError):
                connection_num = None
        hardware = {
            "dbVersion": db_version,
            "osType": config["os_type"],
            "dbType": config["db_type"],
            "cpuNum": config["cpu_num"] if config["cpu_num"] is not None else (os.cpu_count() or 1),
            "totalMemoryMb": config["total_memory_mb"] if config["total_memory_mb"] is not None else _host_memory_mb(),
            "connectionNum": connection_num,
            "hdType": config["hd_type"],
            "dbSize": config["db_size"],
        }
        rules = _run_rules(hardware)
        entries, skipped, excluded_restart, scope_names, rule_names = _build_entries(
            rules, settings, config, eval_mode, budget
        )
        note = _degrade_note(entries, config["tuning_scope"], scope_names, rule_names, excluded_restart)
        scope_unknown = sorted(scope_names - set(settings)) if scope_names is not None else []

        apply_results = None
        if apply_flag:
            apply_results = _apply(conn, entries, apply_scope)

        elapsed = time.time() - started
        scope_list = [item["knob"] for item in config["tuning_scope"]] if config["tuning_scope"] is not None else None
        metadata = {
            "strategy_type": "rule-based",
            "optimization_time": round(elapsed, 4),
            "objective": objective,
            "num_rounds": 0,
            "budget_used": dict(budget, time_used_s=round(elapsed, 4), rounds_used=0, restarts_used=0),
            "eval_mode": eval_mode,
            "excluded_restart_knobs": excluded_restart,
            "mode": "inference-only",
            "config": config,
            "hardware": hardware,
            "warnings": rules.get("warnings") or [],
            "skipped_knobs": skipped[:100],
            "scope_unknown_knobs": scope_unknown,
        }
        if note:
            metadata["degrade_note"] = note
        payload = {"recommended_knobs": entries, "metadata": metadata}
        if apply_results is not None:
            payload["apply_results"] = apply_results
        _record_state(state_dir, objective, scope_list, len(entries), eval_mode)
        return payload
    finally:
        try:
            conn.close()
        except Exception:
            pass
        _complete_journal(state_dir)


def _send_json(handler, status, payload):
    data = json.dumps(payload).encode("utf-8")
    try:
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(data)))
        handler.end_headers()
        handler.wfile.write(data)
    except (BrokenPipeError, ConnectionResetError):
        pass


class Handler(BaseHTTPRequestHandler):
    server_version = "KnobTuning/1.0"

    def log_message(self, format, *args):
        return

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            raise BadRequest("invalid Content-Length")
        if length < 0 or length > 10_000_000:
            raise BadRequest("request body too large")
        raw = self.rfile.read(length) if length else b""
        if not raw:
            raise BadRequest("request body is empty")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            raise BadRequest("invalid JSON body")
        if not isinstance(payload, dict):
            raise BadRequest("request body must be a JSON object")
        return payload

    def do_GET(self):
        started = time.time()
        path = self.path.split("?", 1)[0]
        if path == "/health":
            _send_json(self, 200, {"status": "ok"})
        elif path == "/state":
            _send_json(self, 200, _state_summary())
        else:
            _send_json(self, 404, {"error": "unknown endpoint"})
        print(f"[req] {path} status={200 if path in ('/health', '/state') else 404} "
              f"duration={time.time() - started:.3f}s", flush=True)

    def do_POST(self):
        started = time.time()
        path = self.path.split("?", 1)[0]
        if path == "/shutdown":
            with STATE_LOCK:
                for state_dir in list(STATE_CACHE):
                    _save_state(state_dir, STATE_CACHE[state_dir])
            _send_json(self, 200, {"status": "shutting down"})
            SHUTDOWN_FLAG.set()
            server = SERVER_REF.get("server")
            if server is not None:
                threading.Thread(target=server.shutdown, daemon=True).start()
            print(f"[req] /shutdown status=200 duration={time.time() - started:.3f}s", flush=True)
            return
        if path != "/tune":
            _send_json(self, 404, {"error": "unknown endpoint"})
            print(f"[req] {path} status=404 duration={time.time() - started:.3f}s", flush=True)
            return

        req = {}
        try:
            req = self._read_json()
            if SHUTDOWN_FLAG.is_set():
                fallback = {"time_budget_s": None, "max_rounds": None, "max_restarts": None}
                _send_json(self, 503, _degrade_payload("", "in-place", fallback, {}, "server is shutting down"))
                return
            payload = _tune(req)
            _send_json(self, 200, payload)
            metadata = payload.get("metadata") or {}
            print(f"[req] /tune status=200 knobs={len(payload.get('recommended_knobs') or [])} "
                  f"degrade={'yes' if metadata.get('degrade_note') else 'no'} "
                  f"duration={time.time() - started:.3f}s", flush=True)
        except BadRequest as exc:
            _send_json(self, 400, {"error": str(exc)})
            print(f"[req] /tune status=400 error={exc} duration={time.time() - started:.3f}s", flush=True)
        except Exception:
            fallback = {"time_budget_s": None, "max_rounds": None, "max_restarts": None}
            objective = req.get("objective") if isinstance(req, dict) else ""
            payload = _degrade_payload(objective or "", "in-place", fallback, {},
                                       "internal recommendation failure")
            _send_json(self, 200, payload)
            print(f"[req] /tune status=200 degrade=yes duration={time.time() - started:.3f}s", flush=True)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    _replay_journal(DEFAULT_STATE_DIR)
    port = int(os.environ.get("PORT", "8765"))
    server = Server(("0.0.0.0", port), Handler)
    SERVER_REF["server"] = server
    print(f"[start] listening on 0.0.0.0:{port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        with STATE_LOCK:
            for state_dir in list(STATE_CACHE):
                _save_state(state_dir, STATE_CACHE[state_dir])
        server.server_close()


if __name__ == "__main__":
    main()