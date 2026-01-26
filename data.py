# data.py (SQLite-backed)
from __future__ import annotations

from typing import Any, Dict, List

from db import init_db, load_state, save_state


# --- загрузка состояния из SQLite при старте процесса ---
init_db()
_state = load_state()

admin_settings: Dict[str, Any] = _state.get("admin_settings", {}) or {}
masters_custom: Dict[str, dict] = _state.get("masters_custom", {}) or {}
master_overrides: Dict[str, dict] = _state.get("master_overrides", {}) or {}

bookings: List[dict] = _state.get("bookings", []) or []
blocked_slots: Dict[str, list] = _state.get("blocked_slots", {}) or {}

service_overrides: Dict[str, dict] = _state.get("service_overrides", {}) or {}
services_custom: Dict[str, list] = _state.get("services_custom", {}) or {}


# ----------------------------
# СХЕМА МАСТЕРА (оставляем, т.к. main.py её вызывает)
# ----------------------------
def ensure_master_schema(master_id: int | str) -> dict:
    mid = str(master_id)
    m = masters_custom.get(mid)
    if not isinstance(m, dict):
        m = {}

    if "name" not in m:
        m["name"] = mid
    if "enabled" not in m:
        m["enabled"] = True
    if "services" not in m or not isinstance(m.get("services"), list):
        m["services"] = []

    if "about" not in m:
        m["about"] = ""
    if "contacts" not in m or not isinstance(m.get("contacts"), dict):
        m["contacts"] = {"phone": "", "instagram": "", "address": "", "telegram": ""}

    if "schedule" not in m or not isinstance(m.get("schedule"), dict):
        m["schedule"] = {}
    sch = m["schedule"]
    if "days" not in sch:
        sch["days"] = []
    if "start" not in sch:
        sch["start"] = ""
    if "end" not in sch:
        sch["end"] = ""
    if "daily_limit" not in sch:
        sch["daily_limit"] = 0

    masters_custom[mid] = m
    return m


# ----------------------------
# SAVE: сохраняем ВЕСЬ state в SQLite
# (потому что db.save_state принимает весь набор структур сразу)
# ----------------------------
def _save_all() -> None:
    save_state(
        admin_settings=admin_settings,
        masters_custom=masters_custom,
        master_overrides=master_overrides,
        services_custom=services_custom,
        service_overrides=service_overrides,
        bookings=bookings,
        blocked_slots=blocked_slots,
    )


def save_admin_settings() -> None:
    _save_all()


def save_masters_custom() -> None:
    _save_all()


def save_master_overrides() -> None:
    _save_all()


def save_services_custom() -> None:
    _save_all()


def save_service_overrides() -> None:
    _save_all()


def save_bookings() -> None:
    _save_all()


def save_blocked() -> None:
    _save_all()
