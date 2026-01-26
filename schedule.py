from __future__ import annotations

from datetime import datetime, timedelta, time
from typing import Optional

from config import DATE_FORMAT, TIME_FORMAT, TIME_STEP, PAUSE_MINUTES
import data

MIN_ADVANCE_MINUTES = 120  # не показывать слоты раньше чем через 2 часа

def _parse_hhmm(s: str) -> Optional[time]:
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.strptime(s.strip(), "%H:%M").time()
    except Exception:
        return None


def _get_master_schedule(master_id: int) -> Optional[dict]:
    m = data.ensure_master_schema(master_id)  # <-- гарантирует поля
    ov = data.master_overrides.get(str(master_id), {})

    enabled = True
    if isinstance(ov, dict) and "enabled" in ov:
        enabled = bool(ov.get("enabled"))
    else:
        enabled = bool(m.get("enabled", True))

    if not enabled:
        return None

    sch = m.get("schedule")
    if not isinstance(sch, dict):
        return None

    days = sch.get("days")
    st = _parse_hhmm(sch.get("start"))
    en = _parse_hhmm(sch.get("end"))

    if not isinstance(days, list) or st is None or en is None:
        return None

    try:
        daily_limit = int(sch.get("daily_limit") or 0)
    except Exception:
        daily_limit = 0

    return {"days": days, "start": st, "end": en, "daily_limit": daily_limit}

def ceil_to_step(dt: datetime, step_minutes: int) -> datetime:
    m = dt.minute
    r = m % step_minutes
    if r == 0:
        return dt.replace(second=0, microsecond=0)
    add = step_minutes - r
    return (dt + timedelta(minutes=add)).replace(second=0, microsecond=0)


def _hhmm_to_min(hhmm: str) -> int | None:
    if not hhmm or not isinstance(hhmm, str):
        return None
    try:
        h, m = hhmm.strip().split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return None


def _ceil_to_step_min(mins: int, step: int) -> int:
    r = mins % step
    return mins if r == 0 else mins + (step - r)


def _get_service_duration(master_id: int, service_id: int) -> int | None:
    # 1) услуга (base + custom)
    base_services = data.masters_custom.get(str(master_id), {}).get("services", [])
    custom_services = data.services_custom.get(str(master_id), [])
    all_services = list(base_services) + list(custom_services)

    base_service = next((s for s in all_services if s.get("id") == service_id), None)
    if not base_service:
        return None

    # 2) overrides (длительность + включенность)
    ov = data.service_overrides.get(str(master_id), {}).get(str(service_id), {})
    if ov.get("enabled") is False:
        return None

    try:
        duration = int(ov.get("duration", base_service.get("duration", 0)) or 0)
    except Exception:
        duration = 0

    return duration if duration > 0 else None


def get_available_slots(
    master_id: int,
    date_str: str,
    service_id: int | list[int],
    ignore_min_advance: bool = False,
    ignore_booking_id: int | None = None,
) -> list[str]:

    # service_id может быть int или list[int]
    service_ids = service_id if isinstance(service_id, list) else [service_id]
    if not service_ids:
        return []

    durations = []
    for sid in service_ids:
        d = _get_service_duration(master_id, int(sid))
        if d is None:
            return []
        durations.append(int(d))

    duration = sum(durations)
    if duration <= 0:
        return []

    pause = int(PAUSE_MINUTES or 0)
    if pause < 0:
        pause = 0

    # 3) график мастера
    work = _get_master_schedule(master_id)
    if not work:
        return []

    # дата + weekday
    try:
        day = datetime.strptime(date_str, DATE_FORMAT).date()
    except Exception:
        return []

    weekday = day.weekday()
    if weekday not in work["days"]:
        return []

    # 7) блокировки
    master_blocked = data.blocked_slots.get(str(master_id), [])
    if any(b.get("date") == date_str and b.get("time") is None for b in master_blocked):
        return []

    blocked_times = {
        b.get("time")
        for b in master_blocked
        if b.get("date") == date_str and b.get("time")
    }
    # блокировки считаем интервалами длиной TIME_STEP (чтобы ловить пересечения)
    blocked_intervals: list[tuple[int, int]] = []
    for t in blocked_times:
        m = _hhmm_to_min(t)
        if m is None:
            continue
        blocked_intervals.append((m, m + int(TIME_STEP or 0)))


    # 4) границы дня в минутах
    start_min = work["start"].hour * 60 + work["start"].minute
    end_min = work["end"].hour * 60 + work["end"].minute
    start_min = _ceil_to_step_min(start_min, TIME_STEP)

    if start_min >= end_min:
        return []

    # 6) бронь-интервалы (PENDING+CONFIRMED)
    booked_intervals: list[tuple[int, int]] = []
    day_count = 0

    for b in data.bookings:
        if ignore_booking_id is not None and b.get("id") == ignore_booking_id:
            continue
        if b.get("master_id") != master_id:
            continue
        if b.get("date") != date_str:
            continue
        if b.get("status") not in ("PENDING", "CONFIRMED"):
            continue

        bt = b.get("time")
        if not bt:
            continue

        b_start = _hhmm_to_min(bt)
        if b_start is None:
            continue

        try:
            b_dur = int(b.get("service_duration", 0) or 0)
        except Exception:
            b_dur = 0
        if b_dur <= 0:
            # fallback (если старые записи без длительности)
            b_dur = duration

        b_end = b_start + b_dur + pause
        booked_intervals.append((b_start, b_end))
        day_count += 1

    # daily_limit
    daily_limit = int(work.get("daily_limit") or 0)
    if daily_limit > 0 and day_count >= daily_limit:
        return []

    # now-фильтр (минимум 2 часа вперёд), если не игнорируем
    now = datetime.now()
    if day < now.date():
        return []

    min_start_min = None
    if not ignore_min_advance and day == now.date():
        min_start_min = now.hour * 60 + now.minute + MIN_ADVANCE_MINUTES
        min_start_min = _ceil_to_step_min(min_start_min, TIME_STEP)

    available: list[str] = []

    # последний старт, чтобы услуга целиком влезла (с паузой)
    last_start = end_min - (duration + pause)
    if last_start < start_min:
        return []

    cur = start_min
    while cur <= last_start:
        if min_start_min is not None and cur < min_start_min:
            cur += TIME_STEP
            continue

        hh = cur // 60
        mm = cur % 60
        slot_str = f"{hh:02d}:{mm:02d}"

        slot_end = cur + duration + pause

        # если интервал услуги пересекается с любой блокировкой — слот нельзя
        if any(cur < b_end and slot_end > b_start for b_start, b_end in blocked_intervals):
            cur += TIME_STEP
            continue

        conflict = False
        for b_start, b_end in booked_intervals:
            if cur < b_end and slot_end > b_start:
                conflict = True
                break

        if not conflict:
            available.append(slot_str)

        cur += TIME_STEP

    return available


def can_book_at_time(
    master_id: int,
    date_str: str,
    time_str: str,
    service_id: int | list[int],
    ignore_min_advance: bool = False,
    ignore_booking_id: int | None = None,
) -> bool:
    # Проверяем возможность поставить запись ровно в time_str с учетом длительности (и паузы)
    service_ids = service_id if isinstance(service_id, list) else [service_id]
    if not service_ids:
        return False

    durations = []
    for sid in service_ids:
        d = _get_service_duration(master_id, int(sid))
        if d is None:
            return False
        durations.append(int(d))
    duration = sum(durations)
    if duration <= 0:
        return False

    pause = int(PAUSE_MINUTES or 0)
    if pause < 0:
        pause = 0

    work = _get_master_schedule(master_id)
    if not work:
        return False

    try:
        day = datetime.strptime(date_str, DATE_FORMAT).date()
    except Exception:
        return False

    weekday = day.weekday()
    if weekday not in work["days"]:
        return False

    # целый день закрыт
    master_blocked = data.blocked_slots.get(str(master_id), [])
    if any(b.get("date") == date_str and b.get("time") is None for b in master_blocked):
        return False

    # точечная блокировка
    blocked_times = {
        b.get("time")
        for b in master_blocked
        if b.get("date") == date_str and b.get("time")
    }
    # блокировки считаем интервалами длиной TIME_STEP (чтобы ловить пересечения)
    blocked_intervals: list[tuple[int, int]] = []
    for t in blocked_times:
        m = _hhmm_to_min(t)
        if m is None:
            continue
        blocked_intervals.append((m, m + int(TIME_STEP or 0)))


    start_min = work["start"].hour * 60 + work["start"].minute
    end_min = work["end"].hour * 60 + work["end"].minute
    start_min = _ceil_to_step_min(start_min, TIME_STEP)
    if start_min >= end_min:
        return False

    cur = _hhmm_to_min(time_str)
    if cur is None:
        return False

    # влезаем в рабочее окно
    last_start = end_min - (duration + pause)
    if cur < start_min or cur > last_start:
        return False

    # min advance (если нужно)
    now = datetime.now()
    if day < now.date():
        return False
    if not ignore_min_advance and day == now.date():
        min_start = now.hour * 60 + now.minute + MIN_ADVANCE_MINUTES
        min_start = _ceil_to_step_min(min_start, TIME_STEP)
        if cur < min_start:
            return False

    slot_end = cur + duration + pause
    # пересечение с блокировками
    if any(cur < b_end and slot_end > b_start for b_start, b_end in blocked_intervals):
        return False

    # пересечения с занятыми
    for b in data.bookings:
        if ignore_booking_id is not None and b.get("id") == ignore_booking_id:
            continue
        if b.get("master_id") != master_id:
            continue
        if b.get("date") != date_str:
            continue
        if b.get("status") not in ("PENDING", "CONFIRMED"):
            continue

        bt = b.get("time")
        if not bt:
            continue
        b_start = _hhmm_to_min(bt)
        if b_start is None:
            continue

        try:
            b_dur = int(b.get("service_duration", 0) or 0)
        except Exception:
            b_dur = 0
        if b_dur <= 0:
            b_dur = duration

        b_end = b_start + b_dur + pause

        if cur < b_end and slot_end > b_start:
            return False

    return True


