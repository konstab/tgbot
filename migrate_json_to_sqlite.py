# migrate_json_to_sqlite.py
"""
Одноразовый перенос JSON -> SQLite (bot.db).

Запуск:
    python migrate_json_to_sqlite.py

Что делает:
- создаёт bot.db рядом с этим файлом
- читает ваши JSON (если существуют) и заливает их в таблицы
- печатает краткую статистику

Важно:
- Скрипт НЕ удаляет JSON.
- Если bot.db уже содержит мастеров (не пустая), миграцию не выполняет.
"""
from __future__ import annotations

import json
from pathlib import Path

import db

BASE_DIR = Path(__file__).resolve().parent

FILES = {
    "admin_settings": BASE_DIR / "admin_settings.json",
    "masters_custom": BASE_DIR / "masters_custom.json",
    "master_overrides": BASE_DIR / "master_overrides.json",
    "bookings": BASE_DIR / "bookings.json",
    "blocked_slots": BASE_DIR / "blocked_slots.json",
    "service_overrides": BASE_DIR / "service_overrides.json",
    "services_custom": BASE_DIR / "services_custom.json",
}

def load_json(p: Path, default):
    try:
        if not p.exists():
            return default
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[WARN] не удалось прочитать {p.name}: {e!r}")
        return default

def db_is_empty() -> bool:
    db.init_db()
    with db.connect() as conn:
        r = conn.execute("SELECT COUNT(*) AS c FROM masters").fetchone()
        return int(r["c"]) == 0

def main():
    db.init_db()
    if not db_is_empty():
        print("[SKIP] bot.db уже не пустая (таблица masters содержит данные). Миграция не выполнена.")
        return

    admin_settings = load_json(FILES["admin_settings"], {})
    masters_custom = load_json(FILES["masters_custom"], {})
    master_overrides = load_json(FILES["master_overrides"], {})
    bookings = load_json(FILES["bookings"], [])
    blocked_slots = load_json(FILES["blocked_slots"], {})
    service_overrides = load_json(FILES["service_overrides"], {})
    services_custom = load_json(FILES["services_custom"], {})

    db.save_state(
        admin_settings=admin_settings,
        masters_custom=masters_custom,
        master_overrides=master_overrides,
        services_custom=services_custom,
        service_overrides=service_overrides,
        bookings=bookings,
        blocked_slots=blocked_slots,
    )

    st = db.load_state()
    print("[OK] миграция завершена.")
    print(f"  masters: {len(st['masters_custom'])}")
    base_services = sum(len(m.get('services', []) or []) for m in st["masters_custom"].values() if isinstance(m, dict))
    custom_services = sum(len(v or []) for v in st["services_custom"].values())
    print(f"  services: base={base_services}, custom={custom_services}")
    print(f"  service_overrides: {sum(len(v or {}) for v in st['service_overrides'].values())}")
    print(f"  master_overrides: {len(st['master_overrides'])}")
    print(f"  bookings: {len(st['bookings'])}")
    print(f"  blocked_slots: {sum(len(v or []) for v in st['blocked_slots'].values())}")
    print(f"  admin_settings keys: {len(st['admin_settings'])}")
    print(f"  db file: {db.DB_PATH}")

if __name__ == "__main__":
    main()
