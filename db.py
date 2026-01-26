# db.py
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import os

DB_PATH = Path(os.getenv("DB_PATH", "/app/storage/bot.db"))


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db() -> None:
    with connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS admin_settings (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS masters (
                master_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                about TEXT NOT NULL DEFAULT '',
                contacts_json TEXT NOT NULL DEFAULT '{}',
                schedule_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS master_overrides (
                master_id INTEGER PRIMARY KEY,
                name TEXT,
                enabled INTEGER,
                FOREIGN KEY(master_id) REFERENCES masters(master_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS services (
                master_id INTEGER NOT NULL,
                service_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                price INTEGER NOT NULL DEFAULT 0,
                duration INTEGER NOT NULL DEFAULT 0,
                is_custom INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(master_id, service_id),
                FOREIGN KEY(master_id) REFERENCES masters(master_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS service_overrides (
                master_id INTEGER NOT NULL,
                service_id INTEGER NOT NULL,
                price INTEGER,
                duration INTEGER,
                enabled INTEGER,
                PRIMARY KEY(master_id, service_id),
                FOREIGN KEY(master_id, service_id) REFERENCES services(master_id, service_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS bookings (
                id INTEGER PRIMARY KEY,
                master_id INTEGER NOT NULL,
                client_id INTEGER NOT NULL,
                client_username TEXT,
                client_full_name TEXT,

                service_id INTEGER,
                service_ids_json TEXT,

                service_name TEXT,
                service_price INTEGER,
                service_duration INTEGER,

                date TEXT NOT NULL,
                time TEXT NOT NULL,
                status TEXT NOT NULL,

                cancelled_by TEXT,
                cancel_reason TEXT,

                followup_sent INTEGER,
                client_rating INTEGER,
                rating_at TEXT,

                created_at TEXT,
                extra_json TEXT NOT NULL DEFAULT '{}',

                FOREIGN KEY(master_id) REFERENCES masters(master_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_bookings_master_date ON bookings(master_id, date);
            CREATE INDEX IF NOT EXISTS idx_bookings_status ON bookings(status);

            CREATE TABLE IF NOT EXISTS blocked_slots (
                master_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                time TEXT,
                reason TEXT,
                PRIMARY KEY(master_id, date, time),
                FOREIGN KEY(master_id) REFERENCES masters(master_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                first_seen TEXT,
                last_seen TEXT,
                active INTEGER NOT NULL DEFAULT 1,
                last_error TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_users_active ON users(active);
            """
        )
        conn.execute(
            "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("2",),
        )
        conn.commit()

def _j(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))

def _jl(text: str, default: Any):
    try:
        return json.loads(text)
    except Exception:
        return default

def _rows(conn: sqlite3.Connection, sql: str, params: Tuple[Any, ...] = ()) -> List[sqlite3.Row]:
    return list(conn.execute(sql, params).fetchall())

def load_state() -> Dict[str, Any]:
    init_db()
    with connect() as conn:
        admin_settings: Dict[str, Any] = {}
        for r in _rows(conn, "SELECT key, value_json FROM admin_settings"):
            admin_settings[r["key"]] = _jl(r["value_json"], None)

        masters_custom: Dict[str, dict] = {}
        for r in _rows(conn, "SELECT * FROM masters"):
            mid = str(r["master_id"])
            masters_custom[mid] = {
                "name": r["name"],
                "enabled": bool(r["enabled"]),
                "about": r["about"],
                "contacts": _jl(r["contacts_json"], {}),
                "schedule": _jl(r["schedule_json"], {}),
                "services": [],
            }

        services_custom: Dict[str, list] = {}
        for r in _rows(conn, "SELECT * FROM services ORDER BY master_id, service_id"):
            mid = str(r["master_id"])
            svc = {
                "id": int(r["service_id"]),
                "name": r["name"],
                "price": int(r["price"] or 0),
                "duration": int(r["duration"] or 0),
            }
            if int(r["is_custom"]) == 1:
                services_custom.setdefault(mid, []).append(svc)
            else:
                masters_custom.setdefault(mid, {"services": []})
                masters_custom[mid].setdefault("services", []).append(svc)

        master_overrides: Dict[str, dict] = {}
        for r in _rows(conn, "SELECT * FROM master_overrides"):
            mid = str(r["master_id"])
            d: dict = {}
            if r["name"] is not None:
                d["name"] = r["name"]
            if r["enabled"] is not None:
                d["enabled"] = bool(r["enabled"])
            master_overrides[mid] = d

        service_overrides: Dict[str, dict] = {}
        for r in _rows(conn, "SELECT * FROM service_overrides"):
            mid = str(r["master_id"])
            sid = str(r["service_id"])
            service_overrides.setdefault(mid, {}).setdefault(sid, {})
            d = service_overrides[mid][sid]
            if r["price"] is not None:
                d["price"] = int(r["price"])
            if r["duration"] is not None:
                d["duration"] = int(r["duration"])
            if r["enabled"] is not None:
                d["enabled"] = bool(r["enabled"])

        bookings: List[dict] = []
        for r in _rows(conn, "SELECT * FROM bookings ORDER BY id"):
            b = {
                "id": int(r["id"]),
                "master_id": int(r["master_id"]),
                "client_id": int(r["client_id"]),
                "client_username": r["client_username"],
                "client_full_name": r["client_full_name"],
                "service_id": r["service_id"],
                "service_ids": _jl(r["service_ids_json"] or "[]", []),
                "service_name": r["service_name"],
                "service_price": r["service_price"],
                "service_duration": r["service_duration"],
                "date": r["date"],
                "time": r["time"],
                "status": r["status"],
                "cancelled_by": r["cancelled_by"],
                "cancel_reason": r["cancel_reason"],
                "followup_sent": (bool(r["followup_sent"]) if r["followup_sent"] is not None else None),
                "client_rating": r["client_rating"],
                "rating_at": r["rating_at"],
                "created_at": r["created_at"],
            }
            extra = _jl(r["extra_json"] or "{}", {})
            if isinstance(extra, dict):
                b.update({k: v for k, v in extra.items() if k not in b})
            bookings.append(b)

        blocked_slots: Dict[str, list] = {}
        for r in _rows(conn, "SELECT * FROM blocked_slots ORDER BY master_id, date, time"):
            mid = str(r["master_id"])
            blocked_slots.setdefault(mid, []).append(
                {"date": r["date"], "time": r["time"], "reason": r["reason"]}
            )

        return {
            "admin_settings": admin_settings,
            "masters_custom": masters_custom,
            "master_overrides": master_overrides,
            "bookings": bookings,
            "blocked_slots": blocked_slots,
            "service_overrides": service_overrides,
            "services_custom": services_custom,
        }

def save_state(
    *,
    admin_settings: Dict[str, Any],
    masters_custom: Dict[str, dict],
    master_overrides: Dict[str, dict],
    services_custom: Dict[str, list],
    service_overrides: Dict[str, dict],
    bookings: List[dict],
    blocked_slots: Dict[str, list],
) -> None:
    init_db()
    with connect() as conn:
        cur = conn.cursor()

        cur.execute("DELETE FROM admin_settings")
        for k, v in (admin_settings or {}).items():
            cur.execute("INSERT INTO admin_settings(key, value_json) VALUES(?, ?)", (str(k), _j(v)))

        cur.execute("DELETE FROM bookings")
        cur.execute("DELETE FROM blocked_slots")
        cur.execute("DELETE FROM service_overrides")
        cur.execute("DELETE FROM services")
        cur.execute("DELETE FROM master_overrides")
        cur.execute("DELETE FROM masters")

        inserted_master_ids = set()

        def ensure_master(mid: int) -> None:
            if mid in inserted_master_ids:
                return
            cur.execute(
                "INSERT OR IGNORE INTO masters(master_id, name, enabled, about, contacts_json, schedule_json) "
                "VALUES(?, ?, 1, '', '{}', '{}')",
                (mid, str(mid)),
            )
            inserted_master_ids.add(mid)

        # masters + base services
        for mid_s, m in (masters_custom or {}).items():
            try:
                mid = int(mid_s)
            except Exception:
                continue
            if not isinstance(m, dict):
                continue

            name = str(m.get("name") or mid)
            enabled = 1 if bool(m.get("enabled", True)) else 0
            about = str(m.get("about") or "")
            contacts = m.get("contacts") if isinstance(m.get("contacts"), dict) else {}
            schedule = m.get("schedule") if isinstance(m.get("schedule"), dict) else {}

            cur.execute(
                "INSERT INTO masters(master_id, name, enabled, about, contacts_json, schedule_json) VALUES(?, ?, ?, ?, ?, ?)",
                (mid, name, enabled, about, _j(contacts), _j(schedule)),
            )
            inserted_master_ids.add(mid)

            base_services = m.get("services", [])
            if isinstance(base_services, list):
                for s in base_services:
                    if not isinstance(s, dict):
                        continue
                    sid = s.get("id")
                    if sid is None:
                        continue
                    try:
                        sid = int(sid)
                    except Exception:
                        continue
                    cur.execute(
                        "INSERT OR REPLACE INTO services(master_id, service_id, name, price, duration, is_custom) VALUES(?, ?, ?, ?, ?, 0)",
                        (
                            mid,
                            sid,
                            str(s.get("name") or f"service_{sid}"),
                            int(s.get("price") or 0),
                            int(s.get("duration") or 0),
                        ),
                    )

        # Ensure stub masters for any referenced IDs (JSON can be inconsistent)
        for mid_s in (master_overrides or {}).keys():
            try: ensure_master(int(mid_s))
            except Exception: pass

        for mid_s in (services_custom or {}).keys():
            try: ensure_master(int(mid_s))
            except Exception: pass

        for mid_s in (service_overrides or {}).keys():
            try: ensure_master(int(mid_s))
            except Exception: pass

        for b in (bookings or []):
            if not isinstance(b, dict):
                continue
            try: ensure_master(int(b.get("master_id")))
            except Exception: pass

        for mid_s in (blocked_slots or {}).keys():
            try: ensure_master(int(mid_s))
            except Exception: pass

        # master_overrides
        for mid_s, ov in (master_overrides or {}).items():
            try:
                mid = int(mid_s)
            except Exception:
                continue
            if not isinstance(ov, dict):
                continue
            name = ov.get("name")
            enabled = ov.get("enabled")
            enabled_i = None if enabled is None else (1 if bool(enabled) else 0)
            cur.execute(
                "INSERT OR REPLACE INTO master_overrides(master_id, name, enabled) VALUES(?, ?, ?)",
                (mid, name, enabled_i),
            )

        # services_custom
        for mid_s, svcs in (services_custom or {}).items():
            try:
                mid = int(mid_s)
            except Exception:
                continue
            ensure_master(mid)
            if not isinstance(svcs, list):
                continue
            for s in svcs:
                if not isinstance(s, dict):
                    continue
                sid = s.get("id")
                if sid is None:
                    continue
                try:
                    sid = int(sid)
                except Exception:
                    continue
                cur.execute(
                    "INSERT OR REPLACE INTO services(master_id, service_id, name, price, duration, is_custom) VALUES(?, ?, ?, ?, ?, 1)",
                    (
                        mid,
                        sid,
                        str(s.get("name") or f"service_{sid}"),
                        int(s.get("price") or 0),
                        int(s.get("duration") or 0),
                    ),
                )

        # Ensure stub services for FK safety (service_overrides -> services)
        existing_services = set()
        cur.execute("SELECT master_id, service_id FROM services")
        existing_services |= {(int(r[0]), int(r[1])) for r in cur.fetchall()}

        def ensure_service(mid: int, sid: int) -> None:
            key = (mid, sid)
            if key in existing_services:
                return
            cur.execute(
                "INSERT OR IGNORE INTO services(master_id, service_id, name, price, duration, is_custom) "
                "VALUES(?, ?, ?, 0, 0, 0)",
                (mid, sid, f"service_{sid}"),
            )
            existing_services.add(key)

        # service_overrides
        for mid_s, per_master in (service_overrides or {}).items():
            try:
                mid = int(mid_s)
            except Exception:
                continue
            ensure_master(mid)
            if not isinstance(per_master, dict):
                continue
            for sid_s, ov in per_master.items():
                try:
                    sid = int(sid_s)
                except Exception:
                    continue
                if not isinstance(ov, dict):
                    continue

                ensure_service(mid, sid)

                price = ov.get("price")
                duration = ov.get("duration")
                enabled = ov.get("enabled")
                enabled_i = None if enabled is None else (1 if bool(enabled) else 0)

                cur.execute(
                    "INSERT OR REPLACE INTO service_overrides(master_id, service_id, price, duration, enabled) VALUES(?, ?, ?, ?, ?)",
                    (
                        mid,
                        sid,
                        None if price is None else int(price),
                        None if duration is None else int(duration),
                        enabled_i,
                    ),
                )

        # bookings
        for b in (bookings or []):
            if not isinstance(b, dict):
                continue
            try:
                bid = int(b.get("id"))
                master_id = int(b.get("master_id"))
                client_id = int(b.get("client_id"))
            except Exception:
                continue

            ensure_master(master_id)

            date = str(b.get("date") or "")
            time = str(b.get("time") or "")
            status = str(b.get("status") or "PENDING")
            if not date or not time:
                continue

            known_keys = {
                "id","master_id","client_id","client_username","client_full_name",
                "service_id","service_ids","service_name","service_price","service_duration",
                "date","time","status","cancelled_by","cancel_reason","followup_sent","client_rating","rating_at","created_at",
            }
            extra = {k: v for k, v in b.items() if k not in known_keys}

            service_id = b.get("service_id")
            if service_id is not None:
                try:
                    service_id = int(service_id)
                except Exception:
                    service_id = None

            service_ids = b.get("service_ids")
            if not isinstance(service_ids, list):
                service_ids = [service_id] if service_id is not None else []

            cur.execute(
                """
                INSERT OR REPLACE INTO bookings(
                    id, master_id, client_id, client_username, client_full_name,
                    service_id, service_ids_json,
                    service_name, service_price, service_duration,
                    date, time, status,
                    cancelled_by, cancel_reason,
                    followup_sent, client_rating, rating_at,
                    created_at, extra_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    bid,
                    master_id,
                    client_id,
                    b.get("client_username"),
                    b.get("client_full_name"),
                    service_id,
                    _j(service_ids),
                    b.get("service_name"),
                    None if b.get("service_price") is None else int(b.get("service_price") or 0),
                    None if b.get("service_duration") is None else int(b.get("service_duration") or 0),
                    date,
                    time,
                    status,
                    b.get("cancelled_by"),
                    b.get("cancel_reason"),
                    None if b.get("followup_sent") is None else (1 if bool(b.get("followup_sent")) else 0),
                    b.get("client_rating"),
                    b.get("rating_at"),
                    b.get("created_at"),
                    _j(extra),
                ),
            )

        # blocked_slots
        for mid_s, arr in (blocked_slots or {}).items():
            try:
                mid = int(mid_s)
            except Exception:
                continue
            ensure_master(mid)
            if not isinstance(arr, list):
                continue
            for it in arr:
                if not isinstance(it, dict):
                    continue
                date = it.get("date")
                time = it.get("time")
                reason = it.get("reason")
                if not date:
                    continue
                cur.execute(
                    "INSERT OR REPLACE INTO blocked_slots(master_id, date, time, reason) VALUES(?, ?, ?, ?)",
                    (mid, str(date), (None if time is None else str(time)), (None if reason is None else str(reason))),
                )

        conn.commit()

def get_db():
    return connect()



# -----------------------------------------------------------------------------
# USERS: аудитория для рассылки
# -----------------------------------------------------------------------------

def upsert_user(user_id: int, username: str | None, full_name: str | None, ts: str | None = None) -> None:
    """Регистрирует пользователя, который взаимодействовал с ботом.

    Используется для рассылок и общей статистики аудитории.
    """
    if ts is None:
        ts = datetime.utcnow().isoformat(timespec="seconds")

    with connect() as conn:
        conn.execute(
            """
            INSERT INTO users(user_id, username, full_name, first_seen, last_seen, active, last_error)
            VALUES(?, ?, ?, ?, ?, 1, NULL)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name,
                last_seen=excluded.last_seen,
                active=1
            """,
            (int(user_id), (username or None), (full_name or None), ts, ts),
        )
        conn.commit()


def set_user_inactive(user_id: int, error: str | None = None) -> None:
    ts = datetime.utcnow().isoformat(timespec="seconds")
    with connect() as conn:
        conn.execute(
            "UPDATE users SET active=0, last_seen=?, last_error=? WHERE user_id=?",
            (ts, (error or None), int(user_id)),
        )
        conn.commit()


def get_active_user_ids() -> List[int]:
    init_db()
    with connect() as conn:
        rows = conn.execute("SELECT user_id FROM users WHERE active=1 ORDER BY user_id").fetchall()
        return [int(r[0]) for r in rows]
