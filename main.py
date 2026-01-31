from db import get_db, init_db, upsert_user
from datetime import datetime, date, timedelta, timezone
import calendar
import asyncio
import logging
from datetime import datetime, timedelta
import data
import telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    JobQueue,
    MessageHandler,
    TypeHandler,
    filters,
)
from telegram import Update
from schedule import get_available_slots, can_book_at_time
from states import States
from config import (
    TOKEN,
    DATE_FORMAT,
    TIME_FORMAT,
    DAYS_PER_PAGE,
    REMINDERS,
    TIME_STEP,
    ADMIN_IDS,
    MASTER_DAYS_PER_PAGE,
)

from data import (
    bookings,
    blocked_slots,
    save_bookings,
    save_blocked,
    service_overrides,
    save_service_overrides,
    services_custom,
    save_services_custom,
    masters_custom,
    save_masters_custom,
    master_overrides,
    save_master_overrides,
    admin_settings as ADMIN_SETTINGS,
    save_admin_settings as save_ADMIN_SETTINGS,
)

import zipfile
from pathlib import Path
BASE_DIR = Path(__file__).resolve().parent
BACKUP_DIR = Path(BASE_DIR) / "backups"
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

BACKUP_FILES = [
    "bot.db",
    "bookings.json",
    "blocked_slots.json",
    "masters_custom.json",
    "master_overrides.json",
    "services_custom.json",
    "service_overrides.json",
    "admin_settings.json",
    "clients_custom.json",
]

def make_backup_zip() -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = BACKUP_DIR / f"backup_{ts}.zip"
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for fname in BACKUP_FILES:
            p = Path(BASE_DIR) / fname
            if p.exists():
                z.write(p, arcname=fname)
    return out
# -----------------------------------------------------------------------------
# ЛОГИ
# -----------------------------------------------------------------------------
logger = logging.getLogger(__name__)

async def track_user_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Трекаем всех пользователей, которые хоть раз взаимодействовали с ботом (для рассылок)."""
    u = update.effective_user
    if not u:
        return
    try:
        u = update.effective_user
        if not u:
            return
        ts = datetime.utcnow().isoformat(timespec="seconds")
        await asyncio.to_thread(upsert_user, int(u.id), (u.username or None), (u.full_name or None), ts)
    except Exception:
        return


logging.basicConfig(level=logging.INFO)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Exception while handling an update:", exc_info=context.error)

# -----------------------------------------------------------------------------
# ГЛОБАЛЬНОЕ СОСТОЯНИЕ (in-memory)
# -----------------------------------------------------------------------------
DATA_LOCK = asyncio.Lock()

# user_id -> dict (состояние "визарда" клиента)
user_context: dict[int, dict] = {}

# booking_id -> {"client_id": int, "master_id": int}
active_chats: dict[int, dict] = {}
# user_id -> booking_id
active_chat_by_user: dict[int, int] = {}

ADMIN_BOOKINGS_PER_PAGE = 10

BACK_MAPPING = {
    States.MASTER: States.START,
    States.SERVICE: States.MASTER,
    States.DATE: States.SERVICE,
    States.TIME: States.DATE,
    States.CONFIRM: States.TIME,
}

CLIENT_CANCEL_REASONS = {
    "changed_mind": "Передумал(а)",
    "cant_time": "Не получается по времени",
    "other_master": "Записался(лась) к другому",
    "other": "Другое",
}

# -----------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНОЕ: доступ/guards
# -----------------------------------------------------------------------------
def get_dynamic_admin_ids() -> set[int]:
    raw = ADMIN_SETTINGS.get("admins", [])
    ids: set[int] = set()
    if isinstance(raw, list):
        for x in raw:
            try:
                ids.add(int(x))
            except Exception:
                pass
    return ids

def is_admin(user_id: int) -> bool:
    # базовые + динамические
    return user_id in ADMIN_IDS or user_id in get_dynamic_admin_ids()


def is_master(user_id: int) -> bool:
    return user_id in get_all_masters()

async def guard_admin(q) -> bool:
    if not is_admin(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return False
    return True

async def guard_master(q) -> bool:
    if not is_master(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return False
    return True

# -----------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНОЕ: безопасная отправка/редактирование
# -----------------------------------------------------------------------------
async def safe_edit_text(message, text, reply_markup=None):
    """
    Универсально редактирует:
    - обычные текстовые сообщения через edit_text
    - медиа-сообщения (фото/видео/док) через edit_caption
    Если отредактировать нельзя — отправляет новое сообщение.
    """
    try:
        # 1) обычное текстовое сообщение
        if getattr(message, "text", None) is not None:
            return await message.edit_text(text=text, reply_markup=reply_markup, parse_mode=None)

        # 2) фото/медиа сообщение — редактируем подпись (caption)
        # Telegram допускает caption даже если ранее ее не было
        if getattr(message, "caption", None) is not None or getattr(message, "photo", None) or getattr(message, "video", None) or getattr(message, "document", None):
            return await message.edit_caption(caption=text, reply_markup=reply_markup, parse_mode=None)

        # 3) если не понятно что это — просто шлём новое
        return await message.get_bot().send_message(chat_id=message.chat_id, text=text, reply_markup=reply_markup)

    except Exception:
        # если редактирование невозможно (например сообщение старое/удалено) — шлём новое
        try:
            return await message.get_bot().send_message(chat_id=message.chat_id, text=text, reply_markup=reply_markup)
        except Exception:
            return None

async def safe_send(chat_id: int, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None):
    try:
        return await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
    except telegram.error.TimedOut:
        await asyncio.sleep(1)
        return await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)

# -----------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНОЕ: lock-safe сохранения
# -----------------------------------------------------------------------------
async def locked_save(fn):
    async with DATA_LOCK:
        await asyncio.to_thread(fn)   # ✅ запись файлов уйдёт в отдельный поток

async def save_bookings_locked():
    await locked_save(save_bookings)

async def save_blocked_locked():
    await locked_save(save_blocked)

async def save_service_overrides_locked():
    await locked_save(save_service_overrides)

async def save_services_custom_locked():
    await locked_save(save_services_custom)

async def save_masters_custom_locked():
    await locked_save(save_masters_custom)

async def save_master_overrides_locked():
    await locked_save(save_master_overrides)

async def save_admin_settings_locked():
    await locked_save(save_ADMIN_SETTINGS)

# -----------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНОЕ: masters / services
# -----------------------------------------------------------------------------
def fmt_duration(mins: int) -> str:
    try:
        mins = int(mins or 0)
    except Exception:
        mins = 0

    if mins <= 0:
        return "0 мин"

    h = mins // 60
    m = mins % 60

    if h <= 0:
        return f"{m} мин"
    if m == 0:
        return f"{h} ч"
    return f"{h} ч {m} мин"

def _parse_hhmm(s: str):
    try:
        return datetime.strptime(s.strip(), "%H:%M").time()
    except Exception:
        return None

def get_master_schedule_from_data(master_id: int) -> dict | None:
    m = masters_custom.get(str(master_id))
    if not isinstance(m, dict):
        return None
    sch = m.get("schedule")
    if not isinstance(sch, dict):
        return None

    days = sch.get("days")
    st = _parse_hhmm(sch.get("start", ""))
    en = _parse_hhmm(sch.get("end", ""))

    if not isinstance(days, list) or st is None or en is None:
        return None

    try:
        daily_limit = int(sch.get("daily_limit") or 0)
    except Exception:
        daily_limit = 0

    return {"days": days, "start": st, "end": en, "daily_limit": daily_limit}

def get_all_masters() -> dict[int, dict]:
    """
    Теперь мастера берутся только из data.masters_custom (masters_custom.json).
    Применяются overrides (name/enabled) из master_overrides.json.
    """
    all_m: dict[int, dict] = {}

    # берём из masters_custom.json
    for k, v in masters_custom.items():
        try:
            mid = int(k)
        except Exception:
            continue
        if isinstance(v, dict):
            all_m[mid] = dict(v)

    # overrides: имя/вкл-выкл
    for mid, m in list(all_m.items()):
        ov = master_overrides.get(str(mid), {})
        mm = dict(m)

        if ov.get("name"):
            mm["name"] = ov["name"]

        # enabled может быть и в masters_custom, и в overrides
        # overrides приоритетнее
        if "enabled" in ov:
            mm["enabled"] = ov["enabled"]

        all_m[mid] = mm

    return all_m


def master_enabled(master_id: int) -> bool:
    # приоритет: overrides, затем masters_custom.enabled
    ov = master_overrides.get(str(master_id), {})
    if "enabled" in ov:
        return bool(ov.get("enabled"))
    m = masters_custom.get(str(master_id), {})
    if isinstance(m, dict) and "enabled" in m:
        return bool(m.get("enabled"))
    return True


def list_services_for_master(master_id: int) -> list[dict]:
    base = masters_custom.get(str(master_id), {}).get("services", [])
    custom = services_custom.get(str(master_id), [])
    return list(base) + list(custom)


def next_service_id(master_id: int) -> int:
    all_ids = [s["id"] for s in list_services_for_master(master_id)]
    return max(all_ids, default=999) + 1

def get_service_for_master(master_id: int, service_id: int) -> dict | None:
    base = next((s for s in list_services_for_master(master_id) if s.get("id") == service_id), None)
    if not base:
        return None

    ov = service_overrides.get(str(master_id), {}).get(str(service_id), {})
    merged = dict(base)

    if "price" in ov:
        merged["price"] = ov["price"]
    if "duration" in ov:
        merged["duration"] = ov["duration"]

    merged["enabled"] = ov.get("enabled", True)
    return merged


async def set_service_override(master_id: int, service_id: int, **fields):
    mkey, skey = str(master_id), str(service_id)
    service_overrides.setdefault(mkey, {})
    service_overrides[mkey].setdefault(skey, {})
    service_overrides[mkey][skey].update(fields)
    await save_service_overrides_locked()


def format_service_line(svc: dict) -> str:
    status = "✅" if svc.get("enabled", True) else "🚫"
    return f"{status} {svc['name']} — {svc.get('price', 0)}₽ / {fmt_duration(svc.get('duration', 0))}"

# -----------------------------------------------------------------------------
# ВСПОМОГАТЕЛЬНОЕ: время/слоты/booking
# -----------------------------------------------------------------------------
def ceil_to_step(dt: datetime, step_min: int) -> datetime:
    m = dt.hour * 60 + dt.minute
    rem = m % step_min
    if rem == 0:
        return dt
    add = step_min - rem
    return dt + timedelta(minutes=add)

def next_booking_id() -> int:
    return max((b.get("id", 0) for b in bookings), default=0) + 1

def get_booking(bid: int) -> dict | None:
    return next((b for b in bookings if b.get("id") == bid), None)

def parse_booking_dt(b: dict) -> datetime | None:
    try:
        return datetime.strptime(f"{b['date']} {b['time']}", f"{DATE_FORMAT} {TIME_FORMAT}")
    except Exception:
        return None

def booking_label(b: dict) -> str:
    st = b.get("status", "?")
    ico = {"PENDING": "⏳", "CONFIRMED": "✅", "CANCELLED": "❌"}.get(st, "•")
    return f"{ico} #{b.get('id')} {b.get('date','-')} {b.get('time','-')} — {b.get('service_name','-')}"

def sort_booking_key(b: dict):
    dt = parse_booking_dt(b)
    return (dt is None, dt or datetime.max, b.get("id", 0))

def get_days_page(offset: int, days_per_page: int = DAYS_PER_PAGE) -> list[str]:
    today = datetime.now().date()
    start = today + timedelta(days=offset)
    return [(start + timedelta(days=i)).strftime(DATE_FORMAT) for i in range(days_per_page)]

def scan_available_days(master_id: int, start_offset: int, svc_ids, limit: int = DAYS_PER_PAGE, scan_cap: int = 365 * 2):
    """
    Возвращает (available_days, end_offset).
    available_days — ближайшие limit дней, где есть свободные слоты.
    end_offset — смещение (offset), на котором закончился скан (последний проверенный день).
    """
    today = datetime.now().date()
    master_blocked = blocked_slots.get(str(master_id), [])

    available = []
    offset = start_offset
    scanned = 0

    while len(available) < limit and scanned < scan_cap:
        d = (today + timedelta(days=offset)).strftime(DATE_FORMAT)

        # день закрыт целиком
        if any(b.get("date") == d and b.get("time") is None for b in master_blocked):
            offset += 1
            scanned += 1
            continue

        try:
            slots = get_available_slots(master_id, d, svc_ids)
        except Exception:
            slots = []

        if slots:
            available.append(d)

        offset += 1
        scanned += 1

    end_offset = offset - 1
    return available, end_offset


def get_next_days(n=100) -> list[str]:
    today = datetime.now().date()
    return [(today + timedelta(days=i)).strftime(DATE_FORMAT) for i in range(n)]

def format_client(booking: dict) -> str:
    u = booking.get("client_username")
    if u:
        return f"@{u}"

    # Если нет @username — показываем имя + телефон (если есть), чтобы мастер мог связаться
    name = (booking.get("client_contact_name") or booking.get("client_full_name") or "").strip()
    phone = (booking.get("client_phone") or booking.get("phone") or "").strip()

    if name and phone:
        return f"{name} ({phone})"
    if phone:
        return f"{phone} (ID: {booking.get('client_id')})"
    if name:
        return f"{name} (ID: {booking.get('client_id')})"
    return f"ID: {booking.get('client_id')}"




def _normalize_phone(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    # оставляем + и цифры
    out = []
    for ch in s:
        if ch.isdigit() or (ch == "+" and not out):
            out.append(ch)
    return "".join(out)

async def _finalize_pending_booking(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    pb = context.user_data.get("pending_booking")
    if not isinstance(pb, dict):
        return

    # повторно проверяем слот — пока клиент вводил контакты, слот мог занять другой
    master_id = int(pb.get("master_id"))
    date_s = str(pb.get("date") or "")
    time_s = str(pb.get("time") or "")

    already_taken = any(
        isinstance(b, dict)
        and int(b.get("master_id", -1)) == master_id
        and str(b.get("date") or "") == date_s
        and str(b.get("time") or "") == time_s
        and b.get("status") in ("PENDING", "CONFIRMED")
        for b in bookings
    )
    if already_taken:
        context.user_data.pop("pending_booking", None)
        context.user_data.pop("pending_booking_step", None)
        await update.message.reply_text(
            "Похоже, выбранное время уже занято. Пожалуйста, создайте запись заново.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return

    booking = dict(pb)
    bookings.append(booking)
    await save_bookings_locked()

    keyboard = [
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_{booking['id']}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"cancel_booking_{booking['id']}"),
        ],
        [InlineKeyboardButton("💬 Связаться с клиентом", callback_data=f"chat_{booking['id']}")],
    ]

    await context.bot.send_message(
        chat_id=master_id,
        text=(
            f"Новая заявка #{booking['id']}\n"
            f"👤 Клиент: {format_client(booking)}\n"
            f"Услуга: {booking.get('service_name')}\n"
            f"Дата: {date_s}\n"
            f"Время: {time_s}"
        ),
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

    context.user_data.pop("pending_booking", None)
    context.user_data.pop("pending_booking_step", None)

    await update.message.reply_text(
        "Заявка отправлена мастеру. Ожидайте подтверждения.",
        reply_markup=ReplyKeyboardRemove(),
    )




async def pending_contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    pb = context.user_data.get("pending_booking")
    step = context.user_data.get("pending_booking_step")
    if not isinstance(pb, dict) or step != "phone":
        return

    c = update.message.contact
    if not c:
        return

    # если прислали не свой контакт — попросим свой
    if c.user_id and int(c.user_id) != int(user_id):
        await update.message.reply_text("Пожалуйста, отправьте свой номер (контакт должен принадлежать вам).")
        return

    phone = _normalize_phone(c.phone_number or "")
    digits = "".join(ch for ch in phone if ch.isdigit())
    if len(digits) < 10:
        await update.message.reply_text("Не смог распознать номер. Попробуйте ещё раз или напишите его текстом.")
        return

    pb["client_phone"] = phone
    context.user_data["pending_booking"] = pb
    context.user_data["pending_booking_step"] = "name"

    await update.message.reply_text("Теперь напишите, пожалуйста, как к вам обращаться (имя):", reply_markup=ReplyKeyboardRemove())


def ensure_master_profile(mid: int) -> dict:
    # гарантируем схему мастера в masters_custom
    data.ensure_master_schema(mid)
    m = masters_custom.get(str(mid), {})
    return m if isinstance(m, dict) else {}

def get_master_address(master_id: int) -> str:
    m = masters_custom.get(str(master_id), {})
    addr = ""
    if isinstance(m, dict):
        c = m.get("contacts", {})
        if isinstance(c, dict):
            addr = (c.get("address") or "").strip()
    return addr or "—"

def format_contacts(c: dict) -> str:
    if not isinstance(c, dict):
        return "—"
    parts = []
    if c.get("phone"): parts.append(f"📞 {c['phone']}")
    if c.get("instagram"): parts.append(f"📷 {c['instagram']}")
    if c.get("telegram"): parts.append(f"✈️ {c['telegram']}")
    if c.get("address"): parts.append(f"📍 {c['address']}")
    return "\n".join(parts) if parts else "—"

def format_schedule(s: dict) -> str:
    if not isinstance(s, dict):
        return "—"
    days = s.get("days") or []
    start = s.get("start") or ""
    end = s.get("end") or ""
    lim = s.get("daily_limit") or 0
    days_txt = ", ".join(WEEKDAYS[d] for d in days if isinstance(d, int) and 0 <= d <= 6) or "—"
    lim_txt = "без лимита" if not lim else str(lim)
    if not start or not end or not days:
        return "—"
    return f"{days_txt}\n⏰ {start}–{end}\n📌 Лимит/день: {lim_txt}"

def master_card_text(mid: int) -> str:
    m = ensure_master_profile(mid)
    name = m.get("name", str(mid))
    about = (m.get("about") or "").strip() or "—"
    contacts = format_contacts(m.get("contacts", {}))
    schedule = format_schedule(m.get("schedule", {}))
    return (
        f"👤 Мастер: {name}\nID: {mid}\n\n"
        f"📝 Описание:\n{about}\n\n"
        f"📞 Контакты:\n{contacts}\n\n"
        f"🗓 График:\n{schedule}"
    )

def _is_hhmm(text: str) -> bool:
    try:
        datetime.strptime(text.strip(), "%H:%M")
        return True
    except Exception:
        return False

def check_state(user_id: int, expected_state) -> bool:
    ctx = user_context.get(user_id)
    return bool(ctx and ctx.get("state") == expected_state)

def clear_user_context(user_id: int):
    user_context.pop(user_id, None)

# -----------------------------------------------------------------------------
# Блокировки мастера (слоты)
# -----------------------------------------------------------------------------
def block_slot(master_id: int, date: str, time: str | None = None, reason: str = "отпуск/личное время"):
    blocked_slots.setdefault(str(master_id), [])
    exists = any(b["date"] == date and b["time"] == time for b in blocked_slots[str(master_id)])
    if not exists:
        blocked_slots[str(master_id)].append({"date": date, "time": time, "reason": reason})
        # ❌ НЕ сохраняем тут

def block_time(master_id: int, date: str, time: str, reason: str = "личное время"):
    block_slot(master_id, date, time, reason)

def unblock_day(master_id: int, date: str):
    arr = blocked_slots.get(str(master_id), [])
    blocked_slots[str(master_id)] = [
        b for b in arr if not (b.get("date") == date and b.get("time") is None)
    ]

def unblock_time(master_id: int, date: str, time: str):
    arr = blocked_slots.get(str(master_id), [])
    blocked_slots[str(master_id)] = [
        b for b in arr if not (b.get("date") == date and b.get("time") == time)
    ]

# -----------------------------------------------------------------------------
# НАПОМИНАНИЯ
# -----------------------------------------------------------------------------
def get_reminders_cfg():
    try:
        r = ADMIN_SETTINGS.get("reminders", {})
        return {
            "client": r.get("client", REMINDERS["client"]),
            "master": r.get("master", REMINDERS["master"]),
        }
    except Exception:
        return REMINDERS

def reminder_delta(cfg: dict) -> timedelta:
    return timedelta(**cfg)

async def send_reminder(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    booking_id = job.data["booking_id"]
    target = job.data["target"]

    booking = get_booking(booking_id)
    if not booking or booking.get("status") != "CONFIRMED":
        return

    reply_markup = None

    if target == "client":
        chat_id = booking["client_id"]
        addr = get_master_address(booking["master_id"])

        text = (
            "⏰ Напоминание о записи!\n\n"
            f"📅 {booking['date']}\n"
            f"⏰ {booking['time']}\n"
            f"💅 {booking['service_name']}\n"
            f"📍 Адрес: {addr}\n"
        )

        # кнопки подтверждения/отмены
        reply_markup = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Я приду", callback_data=f"remind_yes_{booking_id}"),
                InlineKeyboardButton("❌ Отменить / Перенести", callback_data=f"client_cancel_{booking_id}"),
            ]
        ])

    else:
        chat_id = booking["master_id"]
        text = (
            "⏰ Напоминание о клиенте!\n\n"
            f"👤 Клиент: {format_client(booking)}\n"
            f"📅 {booking['date']}\n"
            f"⏰ {booking['time']}\n"
            f"💅 {booking['service_name']}"
        )

    try:
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
    except Exception as e:
        print(f"[REMINDER ERROR] booking_id={booking_id} target={target} chat_id={chat_id} err={e}")

async def remind_yes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    # remind_yes_<booking_id>
    parts = (q.data or "").split("_")
    if len(parts) < 3:
        return

    try:
        booking_id = int(parts[2])
    except Exception:
        return

    booking = get_booking(booking_id)
    if not booking:
        await q.answer("Запись не найдена", show_alert=True)
        return

    # защита: нажимать может только клиент этой записи
    if q.from_user.id != booking.get("client_id"):
        await q.answer("Нет доступа", show_alert=True)
        return

    # защита от повторных нажатий
    if booking.get("client_confirmed") is True:
        await q.answer("Вы уже подтвердили ✅", show_alert=True)
        return

    booking["client_confirmed"] = True
    booking["client_confirmed_at"] = datetime.now().isoformat(timespec="seconds")
    await save_bookings_locked()

    # уведомляем мастера
    try:
        await context.bot.send_message(
            chat_id=booking["master_id"],
            text=(
                "✅ Клиент подтвердил запись (нажал «Я приду»)\n\n"
                f"👤 Клиент: {format_client(booking)}\n"
                f"📅 {booking.get('date')}\n"
                f"⏰ {booking.get('time')}\n"
                f"💅 {booking.get('service_name')}\n"
                f"🆔 Запись: #{booking_id}"
            ),
        )
    except Exception:
        pass

    # обновим сообщение клиенту и уберём кнопки
    new_text = (q.message.text or "").strip()
    if new_text:
        new_text += "\n\n✅ Отмечено: вы придёте."
        await safe_edit_text(q.message, new_text, reply_markup=None)
    else:
        # на всякий случай (если вдруг нет текста)
        await context.bot.send_message(
            chat_id=booking["client_id"],
            text="✅ Отмечено: вы придёте.",
        )

def remove_reminders(job_queue: JobQueue, booking_id: int):
    for name in (f"client_{booking_id}", f"master_{booking_id}", f"expire_{booking_id}", f"followup_{booking_id}"):
        for job in job_queue.get_jobs_by_name(name):
            job.schedule_removal()

def schedule_reminders_for_booking(job_queue: JobQueue, booking: dict):
    if booking.get("status") != "CONFIRMED":
        return

    booking_id = booking["id"]
    remove_reminders(job_queue, booking_id)  # ✅ защита от дублей

    rcfg = get_reminders_cfg()
    now = datetime.now()
    booking_dt = datetime.strptime(f"{booking['date']} {booking['time']}", f"{DATE_FORMAT} {TIME_FORMAT}")

    client_delay = (booking_dt - reminder_delta(rcfg["client"]) - now).total_seconds()
    if client_delay > 0:
        job_queue.run_once(
            send_reminder,
            when=client_delay,
            data={"booking_id": booking_id, "target": "client"},
            name=f"client_{booking_id}",
        )

    master_delay = (booking_dt - reminder_delta(rcfg["master"]) - now).total_seconds()
    if master_delay > 0:
        job_queue.run_once(
            send_reminder,
            when=master_delay,
            data={"booking_id": booking_id, "target": "master"},
            name=f"master_{booking_id}",
        )

def restore_reminders(job_queue: JobQueue):
    now = datetime.now()
    rcfg = get_reminders_cfg()

    for booking in bookings:
        if booking.get("status") != "CONFIRMED":
            continue

        try:
            booking_dt = datetime.strptime(f"{booking['date']} {booking['time']}", f"{DATE_FORMAT} {TIME_FORMAT}")
        except Exception:
            continue

        client_delay = (booking_dt - reminder_delta(rcfg["client"]) - now).total_seconds()
        if client_delay > 0:
            remove_reminders(job_queue, booking["id"])
            job_queue.run_once(
                send_reminder,
                when=client_delay,
                data={"booking_id": booking["id"], "target": "client"},
                name=f"client_{booking['id']}",
            )

        master_delay = (booking_dt - reminder_delta(rcfg["master"]) - now).total_seconds()
        if master_delay > 0:
            job_queue.run_once(
                send_reminder,
                when=master_delay,
                data={"booking_id": booking["id"], "target": "master"},
                name=f"master_{booking['id']}",
            )

def restore_followups(job_queue: JobQueue):
    """
    После перезапуска восстанавливаем followup-задачи тем записям, которым ещё надо отправить оценку.
    """
    cfg = get_followup_cfg()
    if not cfg["enabled"]:
        return

    now = datetime.now()

    for b in bookings:
        if b.get("status") != "CONFIRMED":
            continue
        if b.get("followup_sent") is True:
            continue
        if b.get("client_rating") is not None:
            continue

        end_dt = _booking_end_dt(b)
        if not end_dt:
            continue

        run_at = end_dt + timedelta(hours=int(cfg["after_hours"] or 0))
        delay = (run_at - now).total_seconds()
        if delay <= 0:
            # если уже “поздно” — ничего не шлём автоматически (чтобы не спамить)
            continue

        booking_id = b.get("id")
        if not isinstance(booking_id, int):
            continue

        remove_followup(job_queue, booking_id)  # защита от дублей
        job_queue.run_once(
            send_followup_after_visit,
            when=delay,
            data={"booking_id": booking_id},
            name=f"followup_{booking_id}",
        )

def cancel_cleanup_for_booking(booking_id: int, context: ContextTypes.DEFAULT_TYPE):
    # убираем напоминания/таймеры
    remove_reminders(context.job_queue, booking_id)

    # закрываем чат если был
    chat = active_chats.get(booking_id)
    if chat:
        active_chats.pop(booking_id, None)
        active_chat_by_user.pop(chat.get("client_id"), None)
        active_chat_by_user.pop(chat.get("master_id"), None)

# -----------------------------------------------------------------------------
# POST-FOLLOWUP: благодарность + оценка + 2ГИС
# -----------------------------------------------------------------------------

def get_followup_cfg():
    """
    ADMIN_SETTINGS["followup"] пример:
    {
      "enabled": true,
      "after_hours": 12,
      "two_gis_url": "https://...",
      "ask_text": "...{name}...",
      "thanks_text": "...{rating}..."
    }
    """
    f = ADMIN_SETTINGS.get("followup", {}) if isinstance(ADMIN_SETTINGS, dict) else {}
    enabled = f.get("enabled", True)
    after_hours = int(f.get("after_hours", 12) or 12)
    two_gis_url = (f.get("two_gis_url") or "").strip()

    ask_text = f.get("ask_text") or (
        "{name}, искренне благодарим вас за визит! 🙏\n\n"
        "Пожалуйста, оцените работу мастера по шкале 1–5 ⭐️"
    )
    thanks_text = f.get("thanks_text") or (
        "Спасибо за оценку ⭐️{rating}!\n\n"
        "Если вам не сложно, оставьте короткий отзыв в 2ГИС — это очень помогает 🙏"
    )

    return {
        "enabled": bool(enabled),
        "after_hours": max(0, after_hours),
        "two_gis_url": two_gis_url,
        "ask_text": ask_text,
        "thanks_text": thanks_text,
    }


def _booking_end_dt(booking: dict) -> datetime | None:
    """
    Конец сеанса = старт + service_duration минут.
    """
    try:
        start_dt = datetime.strptime(
            f"{booking['date']} {booking['time']}",
            f"{DATE_FORMAT} {TIME_FORMAT}"
        )
    except Exception:
        return None

    dur = int(booking.get("service_duration") or 0)
    if dur <= 0:
        dur = 0
    return start_dt + timedelta(minutes=dur)


def remove_followup(job_queue: JobQueue, booking_id: int):
    for job in job_queue.get_jobs_by_name(f"followup_{booking_id}"):
        job.schedule_removal()


def schedule_followup_for_booking(job_queue: JobQueue, booking: dict):
    cfg = get_followup_cfg()
    if not cfg["enabled"]:
        return
    if booking.get("status") != "CONFIRMED":
        return
    if booking.get("followup_sent") is True:
        return
    if booking.get("client_rating") is not None:
        return

    end_dt = _booking_end_dt(booking)
    if not end_dt:
        return

    run_at = end_dt + timedelta(hours=cfg["after_hours"])
    delay = (run_at - datetime.now()).total_seconds()
    if delay <= 0:
        return

    booking_id = booking["id"]
    remove_followup(job_queue, booking_id)  # защита от дублей

    job_queue.run_once(
        send_followup_after_visit,
        when=delay,
        data={"booking_id": booking_id},
        name=f"followup_{booking_id}",
    )


async def send_followup_after_visit(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    booking_id = job.data["booking_id"]

    booking = get_booking(booking_id)
    if not booking:
        return
    if booking.get("status") != "CONFIRMED":
        return
    if booking.get("followup_sent") is True:
        return
    if booking.get("client_rating") is not None:
        return

    cfg = get_followup_cfg()
    if not cfg["enabled"]:
        return

    # имя клиента (первое слово)
    full = (booking.get("client_full_name") or "").strip()
    name = full.split()[0] if full else "Здравствуйте"

    row = [
        InlineKeyboardButton("1⭐️", callback_data=f"rate_{booking_id}_1"),
        InlineKeyboardButton("2⭐️", callback_data=f"rate_{booking_id}_2"),
        InlineKeyboardButton("3⭐️", callback_data=f"rate_{booking_id}_3"),
        InlineKeyboardButton("4⭐️", callback_data=f"rate_{booking_id}_4"),
        InlineKeyboardButton("5⭐️", callback_data=f"rate_{booking_id}_5"),
    ]

    text = cfg["ask_text"].format(name=name)

    try:
        await context.bot.send_message(
            chat_id=booking["client_id"],
            text=text,
            reply_markup=InlineKeyboardMarkup([row]),
        )
    except Exception:
        return

    booking["followup_sent"] = True
    await save_bookings_locked()


async def rate_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    parts = q.data.split("_")
    if len(parts) != 3:
        return

    booking_id = int(parts[1])
    rating = int(parts[2])

    booking = get_booking(booking_id)
    if not booking:
        await q.answer("Запись не найдена", show_alert=True)
        return

    # защита: оценивать может только клиент этой записи
    if q.from_user.id != booking.get("client_id"):
        await q.answer("Нет доступа", show_alert=True)
        return

    # защита от повторной оценки
    if booking.get("client_rating") is not None:
        await q.answer("Оценка уже сохранена ✅", show_alert=True)
        return

    if rating < 1 or rating > 5:
        return

    booking["client_rating"] = rating
    booking["rated_at"] = datetime.now().isoformat(timespec="seconds")
    await save_bookings_locked()

    cfg = get_followup_cfg()
    two_gis_url = (cfg.get("two_gis_url") or "").strip()

    text = cfg["thanks_text"].format(rating=rating)

    kb = []
    if two_gis_url and rating >= 4:
        kb.append([InlineKeyboardButton("📝 Оставить отзыв в 2ГИС", url=two_gis_url)])
    elif rating <= 3:
        kb.append([InlineKeyboardButton("💬 Написать администратору", callback_data=f"fb_{booking_id}")])

    # редактируем сообщение с кнопками оценок
    await safe_edit_text(q.message, text, InlineKeyboardMarkup(kb) if kb else None)

# -----------------------------------------------------------------------------
# CLIENT FLOW
# -----------------------------------------------------------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_context[user_id] = {"state": States.MASTER}

    all_masters = get_all_masters()
    keyboard = [
        [InlineKeyboardButton(str(m.get("name", mid)), callback_data=f"master_{mid}")]
        for mid, m in all_masters.items()
        if master_enabled(mid)
    ]

    await update.message.reply_text("Выберите мастера:", reply_markup=InlineKeyboardMarkup(keyboard))

async def choose_master(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id

    if not check_state(user_id, States.MASTER):
        await safe_edit_text(q.message, "Старая кнопка недоступна. Начните заново /start.")
        return

    master_id = int(q.data.split("_")[1])
    all_m = get_all_masters()
    master_data = all_m.get(master_id)

    if not master_data or not master_enabled(master_id):
        await safe_edit_text(q.message, "Мастер недоступен. Введите /start заново.")
        return

    ctx = user_context[user_id]
    ctx["master_id"] = master_id
    ctx["state"] = States.SERVICE

    services = list_services_for_master(master_id)
    enabled_services: list[dict] = []
    for s in services:
        svc = get_service_for_master(master_id, s["id"])
        if svc and svc.get("enabled", True):
            enabled_services.append(svc)

    if not enabled_services:
        await safe_edit_text(q.message, "У этого мастера нет доступных услуг.")
        return

    # мультивыбор услуг (чекбоксы)
    ctx["selected_service_ids"] = []   # пустой список выбранных услуг

    keyboard = []
    for svc in enabled_services:
        keyboard.append([InlineKeyboardButton(f"▫️ {svc['name']} - {svc['price']}₽", callback_data=f"service_{svc['id']}")])

    keyboard.append([InlineKeyboardButton("➡ Далее", callback_data="service_done")])
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="back")])


    mprof = ensure_master_profile(master_id)
    about = (mprof.get("about") or "").strip()
    contacts = format_contacts(mprof.get("contacts", {}))
    photo_id = (mprof.get("photo_file_id") or "").strip()

    header = f"👤 {master_data.get('name', master_id)}"
    if about:
        header += f"\n📝 {about}"
    if contacts != "—":
        header += f"\nКонтакты:\n{contacts}"

    text = f"{header}\n\nВыберите услугу:"
    markup = InlineKeyboardMarkup(keyboard)

    # ✅ Если есть фото — удаляем старое сообщение и отправляем новое с фото+кнопками
    if photo_id:
        try:
            await q.message.delete()
        except Exception:
            pass

        try:
            await context.bot.send_photo(
                chat_id=q.message.chat_id,
                photo=photo_id,
                caption=text,
                reply_markup=markup,
            )
        except Exception:
            # если фото битое/недоступное — просто покажем текстом
            await context.bot.send_message(
                chat_id=q.message.chat_id,
                text=text,
                reply_markup=markup,
            )
        return

    # ✅ Если фото нет — работаем как раньше (редактируем текущее сообщение)
    await safe_edit_text(
        q.message,
        text,
        markup,
    )
    return

async def choose_service(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id

    if not check_state(user_id, States.SERVICE):
        await safe_edit_text(q.message, "Старая кнопка недоступна. Начните заново /start.")
        return

    ctx = user_context[user_id]
    master_id = ctx["master_id"]

    # список доступных услуг мастера (с учетом enabled)
    services = list_services_for_master(master_id)
    enabled_services: list[dict] = []
    for s in services:
        svc = get_service_for_master(master_id, s["id"])
        if svc and svc.get("enabled", True):
            enabled_services.append(svc)

    if not enabled_services:
        await safe_edit_text(q.message, "У этого мастера нет доступных услуг.")
        return

    # инициализация выбранных
    selected = ctx.get("selected_service_ids")
    if not isinstance(selected, list):
        selected = []
        ctx["selected_service_ids"] = selected

    # кнопка "Далее"
    if q.data == "service_done":
        if not selected:
            await q.answer("Выберите хотя бы одну услугу", show_alert=True)
            return

        # считаем сумму
        picked_svcs = []
        for sid in selected:
            svc = get_service_for_master(master_id, int(sid))
            if not svc or not svc.get("enabled", True):
                await safe_edit_text(q.message, "Одна из выбранных услуг стала недоступна. Выберите заново.")
                ctx["selected_service_ids"] = []
                return
            picked_svcs.append(svc)

        total_price = sum(int(s.get("price", 0) or 0) for s in picked_svcs)
        total_duration = sum(int(s.get("duration", 0) or 0) for s in picked_svcs)
        names = [s.get("name", "") for s in picked_svcs if s.get("name")]

        # сохраняем в контекст
        ctx["service_ids"] = [int(x) for x in selected]             # список услуг
        ctx["service_id"] = int(selected[0])                        # для совместимости (старые места)
        ctx["service_name"] = " + ".join(names) if names else "Услуга"
        ctx["service_price"] = int(total_price)
        ctx["service_duration"] = int(total_duration)

        ctx["state"] = States.DATE
        ctx["day_offset"] = 0
        await show_date_step(q.message, ctx)
        return

    # иначе: toggle конкретной услуги service_<id>
    try:
        service_id = int(q.data.split("_")[1])
    except Exception:
        return

    if service_id in selected:
        selected.remove(service_id)
    else:
        selected.append(service_id)

    # перерисовка меню услуг с чекбоксами
    keyboard = []
    for svc in enabled_services:
        mark = "✅" if svc["id"] in selected else "▫️"
        keyboard.append([InlineKeyboardButton(f"{mark} {svc['name']} - {svc['price']}₽", callback_data=f"service_{svc['id']}")])

    # показываем текущую сумму
    total_price = 0
    total_duration = 0
    if selected:
        for sid in selected:
            s = get_service_for_master(master_id, int(sid))
            if s:
                total_price += int(s.get("price", 0) or 0)
                total_duration += int(s.get("duration", 0) or 0)

    footer = ""
    if selected:
        footer = f"\n\nВыбрано: {len(selected)}\n⏱ {fmt_duration(total_duration)}\n💰 {total_price} ₽"

    keyboard.append([InlineKeyboardButton("➡ Далее", callback_data="service_done")])
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="back")])

    all_m = get_all_masters()
    master_data = all_m.get(master_id, {})
    mprof = ensure_master_profile(master_id)
    about = (mprof.get("about") or "").strip()
    contacts = format_contacts(mprof.get("contacts", {}))

    header = f"👤 {master_data.get('name', master_id)}"
    if about:
        header += f"\n📝 {about}"
    if contacts != "—":
        header += f"\nКонтакты:\n{contacts}"

    await safe_edit_text(q.message, f"{header}\n\nВыберите услугу(и):{footer}", InlineKeyboardMarkup(keyboard))

async def show_date_step(message, ctx: dict):
    ctx["state"] = States.DATE

    master_id = ctx.get("master_id")
    service_id = ctx.get("service_id")
    if not master_id or not service_id:
        await safe_edit_text(message, "Контекст утерян. Начните заново: /start")
        return

    offset = int(ctx.get("day_offset", 0))

    def _calc():
        svc_ids = ctx.get("service_ids") or ctx.get("service_id")
        return scan_available_days(master_id, offset, svc_ids, limit=DAYS_PER_PAGE)

    # ✅ тяжёлое — в поток
    available_days, end_offset = await asyncio.to_thread(_calc)

    ctx["day_end_offset"] = end_offset  # понадобится для ▶

    keyboard = [[InlineKeyboardButton(d, callback_data=f"date_{d}")] for d in available_days]
    keyboard.insert(0, [InlineKeyboardButton("⚡ Ближайшее время", callback_data="nearest_times")])

    keyboard.append([
        InlineKeyboardButton("◀", callback_data="prev_days"),
        InlineKeyboardButton("▶", callback_data="next_days"),
    ])
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="back")])

    text = "📅 Выберите дату:"
    if not available_days:
        text += "\n\nСвободных дней не найдено. Попробуйте позже или выберите другого мастера."

    await safe_edit_text(message, text, InlineKeyboardMarkup(keyboard))


async def next_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ctx = user_context.get(q.from_user.id)
    if not ctx:
        return

    # история для кнопки "◀"
    hist = ctx.get("day_offset_hist")
    if not isinstance(hist, list):
        hist = []
        ctx["day_offset_hist"] = hist

    hist.append(ctx.get("day_offset", 0))

    # прыгаем на следующий день после последнего проверенного
    end_offset = ctx.get("day_end_offset")
    if end_offset is None:
        end_offset = ctx.get("day_offset", 0)

    ctx["day_offset"] = int(end_offset) + 1
    await show_date_step(q.message, ctx)


async def prev_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ctx = user_context.get(q.from_user.id)
    if not ctx:
        return

    hist = ctx.get("day_offset_hist")
    if not isinstance(hist, list) or not hist:
        ctx["day_offset"] = 0
    else:
        ctx["day_offset"] = hist.pop()

    await show_date_step(q.message, ctx)

async def choose_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id

    if not check_state(user_id, States.DATE):
        await safe_edit_text(q.message, "Старая кнопка недоступна. Начните заново /start.")
        return

    ctx = user_context[user_id]
    ctx["date"] = q.data.split("_", 1)[1]
    ctx["state"] = States.TIME

    svc_ids = ctx.get("service_ids") or ctx["service_id"]
    slots = await asyncio.to_thread(get_available_slots, ctx["master_id"], ctx["date"], svc_ids)
    # choose_date
    ctx["slots_cache"] = {"date": ctx["date"], "slots": slots}

    if not slots:
        await safe_edit_text(q.message, "На этот день нет свободного времени. Выберите другую дату.")
        return

    keyboard = [[InlineKeyboardButton(t, callback_data=f"time_{t}")] for t in slots]
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="back")])
    await safe_edit_text(q.message, f"Дата: {ctx['date']}\nВыберите время:", InlineKeyboardMarkup(keyboard))

async def choose_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id

    if not check_state(user_id, States.TIME):
        await safe_edit_text(q.message, "Старая кнопка недоступна. Начните заново /start.")
        return

    ctx = user_context[user_id]
    date = ctx["date"]
    master_id = ctx["master_id"]
    service_id = ctx["service_id"]
    selected_time = q.data.split("_", 1)[1]

    # проверка доступности
    svc_ids = ctx.get("service_ids") or service_id
    available_slots = await asyncio.to_thread(get_available_slots, master_id, date, svc_ids)

    if selected_time not in available_slots:
        await safe_edit_text(q.message, "Этот слот уже занят или заблокирован. Выберите другой.")
        return

    # жёсткая защита от дубля
    already_taken = any(
        b.get("master_id") == master_id
        and b.get("date") == date
        and b.get("time") == selected_time
        and b.get("status") in ("PENDING", "CONFIRMED")
        for b in bookings
    )
    if already_taken:
        await safe_edit_text(q.message, "Это время уже занято. Выберите другое.")
        return

    ctx["time"] = selected_time

    # сохраняем выбранные услуги
    svc_ids = ctx.get("service_ids") or [ctx["service_id"]]

    username = q.from_user.username
    full_name = q.from_user.full_name

    # Если у клиента нет @username — собираем телефон и имя, чтобы мастер мог связаться
    if not username:
        pending = {
            "id": next_booking_id(),
            "client_id": user_id,
            "client_username": None,
            "client_full_name": full_name,
            "master_id": master_id,

            # совместимость (старое поле оставляем)
            "service_id": int(svc_ids[0]),
            # новое поле
            "service_ids": [int(x) for x in svc_ids],

            "service_name": ctx["service_name"],
            "service_price": ctx["service_price"],
            "service_duration": ctx["service_duration"],

            "date": date,
            "time": selected_time,
            "status": "PENDING",

            # доп. поля (уйдут в extra_json и не потребуют миграции БД)
            "client_phone": "",
            "client_contact_name": "",
        }

        context.user_data["pending_booking"] = pending
        context.user_data["pending_booking_step"] = "phone"

        # чтобы не оставлять "незавершённые" шаги выбора услуг/даты/времени
        clear_user_context(user_id)

        kb = ReplyKeyboardMarkup(
            [
                [KeyboardButton("📱 Отправить номер", request_contact=True)],
                [KeyboardButton("Отмена")],
            ],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        await q.message.reply_text(
            "У вас не задан @username. Чтобы мастер мог связаться, отправьте номер телефона (кнопка ниже) "
            "или напишите его сообщением. Затем я попрошу указать имя.",
            reply_markup=kb,
        )
        await safe_edit_text(q.message, "Шаг 1/2: отправьте номер телефона в чате ниже.")
        return

    booking = {
        "id": next_booking_id(),
        "client_id": user_id,
        "client_username": username,
        "client_full_name": full_name,
        "master_id": master_id,

        # совместимость (старое поле оставляем)
        "service_id": int(svc_ids[0]),
        # новое поле
        "service_ids": [int(x) for x in svc_ids],

        "service_name": ctx["service_name"],            # уже "Услуга1 + Услуга2"
        "service_price": ctx["service_price"],          # сумма
        "service_duration": ctx["service_duration"],    # сумма

        "date": date,
        "time": selected_time,
        "status": "PENDING",
    }

    bookings.append(booking)
    await save_bookings_locked()

    keyboard = [
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_{booking['id']}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"cancel_booking_{booking['id']}"),
        ],
        [InlineKeyboardButton("💬 Связаться с клиентом", callback_data=f"chat_{booking['id']}")],
    ]

    await context.bot.send_message(
        chat_id=master_id,
        text=(
            f"Новая заявка #{booking['id']}\n"
            f"👤 Клиент: {format_client(booking)}\n"
            f"Услуга: {booking['service_name']}\n"
            f"Дата: {date}\n"
            f"Время: {selected_time}"
        ),
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

    await safe_edit_text(q.message, "Заявка отправлена мастеру. Ожидайте подтверждения.")
    clear_user_context(user_id)

def _iter_future_days(max_days: int = 120):
    today = datetime.now().date()
    for i in range(max_days):
        yield (today + timedelta(days=i)).strftime(DATE_FORMAT)

def _calc_nearest_slots(master_id: int, svc_ids, limit: int = 10, max_days: int = 120):
    out: list[tuple[str, str]] = []
    for d in _iter_future_days(max_days):
        slots = get_available_slots(master_id, d, svc_ids)
        for t in slots:
            out.append((d, t))
            if len(out) >= limit:
                return out
    return out

async def nearest_times(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id

    if not check_state(user_id, States.DATE):
        await safe_edit_text(q.message, "Старая кнопка недоступна. Начните заново /start.")
        return

    ctx = user_context[user_id]
    master_id = ctx["master_id"]
    svc_ids = ctx.get("service_ids") or ctx["service_id"]

    nearest = await asyncio.to_thread(_calc_nearest_slots, master_id, svc_ids, 10, 120)

    if not nearest:
        kb = [[InlineKeyboardButton("⬅ Назад", callback_data="nearest_back")]]
        await safe_edit_text(q.message, "Ближайших свободных слотов не найдено. Попробуйте пролистать даты ▶.", InlineKeyboardMarkup(kb))
        return

    keyboard = [
        [InlineKeyboardButton(f"{d} {t}", callback_data=f"nearest_pick_{d}_{t}")]
        for d, t in nearest
    ]
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="nearest_back")])

    await safe_edit_text(q.message, "⚡ Ближайшее время (до 10 вариантов):\nВыберите подходящий слот:", InlineKeyboardMarkup(keyboard))

async def nearest_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ctx = user_context.get(q.from_user.id)
    if not ctx:
        return
    await show_date_step(q.message, ctx)

async def nearest_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id

    if not check_state(user_id, States.DATE):
        await safe_edit_text(q.message, "Старая кнопка недоступна. Начните заново /start.")
        return

    # nearest_pick_YYYY-MM-DD_HH:MM
    parts = q.data.split("_", 3)
    if len(parts) < 4:
        return
    date = parts[2]
    selected_time = parts[3]

    ctx = user_context[user_id]
    master_id = ctx["master_id"]
    service_id = ctx["service_id"]

    svc_ids = ctx.get("service_ids") or service_id
    available_slots = await asyncio.to_thread(get_available_slots, master_id, date, svc_ids)

    if selected_time not in available_slots:
        await safe_edit_text(q.message, "Этот слот уже занят или заблокирован. Нажмите «⚡ Ближайшее время» ещё раз.")
        return

    # жёсткая защита от дубля (как в choose_time)
    already_taken = any(
        b.get("master_id") == master_id
        and b.get("date") == date
        and b.get("time") == selected_time
        and b.get("status") in ("PENDING", "CONFIRMED")
        for b in bookings
    )
    if already_taken:
        await safe_edit_text(q.message, "Это время уже занято. Нажмите «⚡ Ближайшее время» ещё раз.")
        return

    # сохраняем в контекст (чтобы формат/контакты работали как обычно)
    ctx["date"] = date
    ctx["time"] = selected_time

    svc_ids_list = ctx.get("service_ids") or [ctx["service_id"]]

    booking = {
        "id": next_booking_id(),
        "client_id": user_id,
        "client_username": q.from_user.username,
        "client_full_name": q.from_user.full_name,
        "master_id": master_id,

        "service_id": int(svc_ids_list[0]),
        "service_ids": [int(x) for x in svc_ids_list],

        "service_name": ctx["service_name"],
        "service_price": ctx["service_price"],
        "service_duration": ctx["service_duration"],

        "date": date,
        "time": selected_time,
        "status": "PENDING",
    }

    bookings.append(booking)
    await save_bookings_locked()

    keyboard = [
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_{booking['id']}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"cancel_booking_{booking['id']}"),
        ],
        [InlineKeyboardButton("💬 Связаться с клиентом", callback_data=f"chat_{booking['id']}")],
    ]

    await context.bot.send_message(
        chat_id=master_id,
        text=(
            f"Новая заявка #{booking['id']}\n"
            f"👤 Клиент: {format_client(booking)}\n"
            f"Услуга: {booking['service_name']}\n"
            f"Дата: {date}\n"
            f"Время: {selected_time}"
        ),
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

    await safe_edit_text(q.message, "Заявка отправлена мастеру. Ожидайте подтверждения.")
    clear_user_context(user_id)

async def my_booking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    now = datetime.now()
    active = []
    for b in bookings:
        if b.get("client_id") != user_id:
            continue
        if b.get("status") not in ("PENDING", "CONFIRMED"):
            continue

        # скрываем, если сеанс уже закончился
        end_dt = _booking_end_dt(b)  # функция у тебя уже есть выше
        if end_dt is None:
            # если не смогли посчитать конец — пробуем хотя бы старт
            start_dt = parse_booking_dt(b)
            if start_dt and start_dt >= now:
                active.append(b)
            continue

        if end_dt >= now:
            active.append(b)


    if not active:
        await update.message.reply_text("У вас нет активных записей.")
        return

    active.sort(key=lambda b: (b.get("date", ""), b.get("time", ""), b.get("id", 0)))

    for b in active:
        status = "⏳ Ожидает подтверждения" if b["status"] == "PENDING" else "✅ Подтверждена"
        text = (
            "📌 Ваша запись:\n\n"
            f"🆔 #{b['id']}\n"
            f"Статус: {status}\n"
            f"💅 Услуга: {b.get('service_name','-')}\n"
            f"📅 Дата: {b.get('date','-')}\n"
            f"⏰ Время: {b.get('time','-')}\n"
            f"💰 Цена: {b.get('service_price',0)} ₽"
        )
        kb = [[InlineKeyboardButton("Изменить", callback_data=f"client_cancel_{b['id']}")]]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb))

# -----------------------------------------------------------------------------
# CHAT: мастер <-> клиент
# -----------------------------------------------------------------------------
async def start_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    booking_id = int(q.data.split("_")[1])
    booking = get_booking(booking_id)
    if not booking:
        await safe_edit_text(q.message, "Заявка не найдена.")
        return

    # права: только мастер этой заявки
    if q.from_user.id != booking["master_id"]:
        await q.answer("Нет доступа", show_alert=True)
        return

    # запрет параллельных чатов
    if booking["client_id"] in active_chat_by_user or booking["master_id"] in active_chat_by_user:
        await q.answer("Сначала завершите текущий чат", show_alert=True)
        return

    active_chats[booking_id] = {"client_id": booking["client_id"], "master_id": booking["master_id"]}
    active_chat_by_user[booking["client_id"]] = booking_id
    active_chat_by_user[booking["master_id"]] = booking_id

    await context.bot.send_message(
        chat_id=booking["client_id"],
        text="✉️ Мастер хочет связаться с вами.\n\nНапишите сообщение сюда — я передам его мастеру.",
    )

    keyboard = [[InlineKeyboardButton("❌ Завершить чат", callback_data=f"end_chat_{booking_id}")]]
    await safe_edit_text(q.message, "💬 Чат активен. Мастер может завершить чат кнопкой ниже.", InlineKeyboardMarkup(keyboard))

async def end_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = q.from_user.id
    booking_id = int(q.data.split("_")[2])

    chat = active_chats.get(booking_id)
    if not chat:
        await safe_edit_text(q.message, "Чат уже завершён.")
        return

    if user_id != chat["master_id"]:
        await q.answer("Только мастер может завершить чат", show_alert=True)
        return

    client_id = chat["client_id"]

    active_chats.pop(booking_id, None)
    active_chat_by_user.pop(chat["client_id"], None)
    active_chat_by_user.pop(chat["master_id"], None)

    await context.bot.send_message(chat_id=client_id, text="❌ Чат с мастером завершён.")
    await safe_edit_text(q.message, "❌ Чат с клиентом завершён.")

# -----------------------------------------------------------------------------
# MASTER: подтверждение/отклонение заявки
# -----------------------------------------------------------------------------
async def confirm_booking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    booking_id = int(q.data.split("_")[1])
    booking = get_booking(booking_id)
    if not booking:
        await safe_edit_text(q.message, "Заявка не найдена.")
        return

    if q.from_user.id != booking["master_id"]:
        await q.answer("Нет доступа", show_alert=True)
        return

    if booking.get("status") != "PENDING":
        await safe_edit_text(q.message, "Заявка уже обработана.")
        return

    booking["status"] = "CONFIRMED"
    await save_bookings_locked()

    addr = get_master_address(booking["master_id"])

    await context.bot.send_message(
        chat_id=booking["client_id"],
        text=(
            "✅ Ваша запись подтверждена!\n\n"
            f"💅 Услуга: {booking['service_name']}\n"
            f"📅 Дата: {booking['date']}\n"
            f"⏰ Время: {booking['time']}\n"
            f"📍 Адрес: {addr}\n"
            f"💰 Цена: {booking['service_price']} ₽"
        ),
    )

    await safe_edit_text(
        q.message,
        (
            "✅ Запись подтверждена\n\n"
            f"👤 Клиент: {format_client(booking)}\n"
            f"💅 Услуга: {booking['service_name']}\n"
            f"📅 {booking['date']} {booking['time']}"
        ),
    )

    schedule_reminders_for_booking(context.job_queue, booking)
    schedule_followup_for_booking(context.job_queue, booking)

async def cancel_booking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    booking_id = int(q.data.rsplit("_", 1)[1])
    booking = get_booking(booking_id)
    if not booking:
        await safe_edit_text(q.message, "Заявка не найдена.")
        return

    if q.from_user.id != booking["master_id"]:
        await q.answer("Нет доступа", show_alert=True)
        return

    if booking.get("status") == "CANCELLED":
        await safe_edit_text(q.message, "Заявка уже обработана.")
        return

    booking["status"] = "CANCELLED"
    await save_bookings_locked()
    remove_reminders(context.job_queue, booking_id)

    try:
        await context.bot.send_message(
            chat_id=booking["client_id"],
            text=(
                "❌ Ваша заявка была отклонена мастером.\n\n"
                f"💅 Услуга: {booking.get('service_name', '')}\n"
                f"📅 Дата: {booking.get('date', '')}\n"
                f"⏰ Время: {booking.get('time', '')}"
            ),
        )
    except Exception as e:
        print(f"[CANCEL_NOTIFY_ERROR] booking_id={booking_id} err={e}")

    await safe_edit_text(q.message, "Заявка отклонена ❌")

# -----------------------------------------------------------------------------
# CLIENT: отмена/перенос (меню)
# -----------------------------------------------------------------------------
async def client_cancel_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    user_id = q.from_user.id
    booking_id = int(q.data.split("_")[2])

    booking = get_booking(booking_id)
    if not booking:
        await safe_edit_text(q.message, "Запись не найдена.")
        return
    if booking.get("client_id") != user_id:
        await q.answer("Нет доступа", show_alert=True)
        return
    if booking.get("status") not in ("PENDING", "CONFIRMED"):
        await safe_edit_text(q.message, "Эту запись уже нельзя изменить.")
        return

    text = (
        f"Запись #{booking_id}\n"
        f"{booking.get('date','-')} {booking.get('time','-')}\n"
        f"💅 {booking.get('service_name','-')}\n\n"
        "Что вы хотите сделать?"
    )

    kb = [
        [InlineKeyboardButton("🔁 Перенести запись", callback_data=f"client_cancel_choose_{booking_id}_resched")],
        [InlineKeyboardButton("💅 Изменить услугу", callback_data=f"client_change_service_{booking_id}")],
        [InlineKeyboardButton("❌ Отменить запись", callback_data=f"client_cancel_choose_{booking_id}_cancel")],
    ]

    await safe_edit_text(q.message, text, InlineKeyboardMarkup(kb))
async def client_change_service_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    booking_id = int(q.data.split("_")[-1])
    b = get_booking(booking_id)
    if not b or b.get("client_id") != q.from_user.id or b.get("status") not in ("PENDING", "CONFIRMED"):
        await safe_edit_text(q.message, "Эту запись нельзя изменить.")
        return

    master_id = b["master_id"]

    # доступные услуги
    services = []
    for s in list_services_for_master(master_id):
        svc = get_service_for_master(master_id, int(s["id"]))
        if svc and svc.get("enabled", True):
            services.append(svc)

    if not services:
        await safe_edit_text(q.message, "Нет доступных услуг для выбора.")
        return

    # стартовое выделение: если в записи уже есть service_ids — их и покажем
    selected = b.get("service_ids")
    if not isinstance(selected, list) or not selected:
        selected = [int(b.get("service_id") or services[0]["id"])]

    context.user_data["client_chsvc"] = {
        "booking_id": booking_id,
        "master_id": master_id,
        "selected": [int(x) for x in selected],
    }

    await client_chsvc_render(q.message, context)


async def client_chsvc_render(message, context: ContextTypes.DEFAULT_TYPE):
    st = context.user_data.get("client_chsvc")
    if not st:
        await safe_edit_text(message, "Сеанс устарел. Откройте /mybooking заново.")
        return

    booking = get_booking(st["booking_id"])
    if not booking:
        await safe_edit_text(message, "Запись не найдена.")
        return

    master_id = st["master_id"]
    selected = st.get("selected", [])

    services = []
    for s in list_services_for_master(master_id):
        svc = get_service_for_master(master_id, int(s["id"]))
        if svc and svc.get("enabled", True):
            services.append(svc)

    kb = []
    total_price = 0
    total_duration = 0

    for svc in services:
        mark = "✅" if svc["id"] in selected else "▫️"
        kb.append([InlineKeyboardButton(f"{mark} {svc['name']} - {svc.get('price',0)}₽", callback_data=f"client_chsvc_tgl_{svc['id']}")])
        if svc["id"] in selected:
            total_price += int(svc.get("price", 0) or 0)
            total_duration += int(svc.get("duration", 0) or 0)

    kb.append([InlineKeyboardButton("ДАЛЕЕ", callback_data="client_chsvc_apply")])
    kb.append([InlineKeyboardButton("⬅ Назад", callback_data=f"client_cancel_{st['booking_id']}")])

    text = (
        f"Текущая запись: #{booking['id']}\n"
        f"📅 {booking.get('date','-')} ⏰ {booking.get('time','-')}\n"
        f"Сейчас: {booking.get('service_name','-')}\n\n"
        "Выберите новую услугу(и):\n"
    )
    if selected:
        text += f"\nВыбрано: {len(selected)}\n⏱ {fmt_duration(total_duration)}\n💰 {total_price} ₽"

    await safe_edit_text(message, text, InlineKeyboardMarkup(kb))


async def client_chsvc_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    st = context.user_data.get("client_chsvc")
    if not st:
        await safe_edit_text(q.message, "Сеанс устарел. Откройте /mybooking заново.")
        return

    sid = int(q.data.split("_")[-1])
    selected = st.get("selected", [])
    if sid in selected:
        selected.remove(sid)
    else:
        selected.append(sid)
    st["selected"] = selected

    await client_chsvc_render(q.message, context)


async def client_chsvc_apply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    st = context.user_data.get("client_chsvc")
    if not st:
        await safe_edit_text(q.message, "Сеанс устарел. Откройте /mybooking заново.")
        return

    booking_id = st["booking_id"]
    old = get_booking(booking_id)
    if not old or old.get("client_id") != q.from_user.id or old.get("status") not in ("PENDING", "CONFIRMED"):
        await safe_edit_text(q.message, "Эту запись нельзя изменить.")
        return

    master_id = st["master_id"]
    selected = st.get("selected", [])
    if not selected:
        await q.answer("Выберите хотя бы одну услугу", show_alert=True)
        return

    # проверяем, что на ТОМ ЖЕ времени новая длительность влезет без пересечений
    ok = await asyncio.to_thread(
        can_book_at_time,
        master_id,
        old["date"],
        old["time"],
        [int(x) for x in selected],
        True,               # ignore_min_advance=True (раз уже была запись на это время)
        booking_id,         # ignore_booking_id=старую запись исключаем из пересечений
    )

    if not ok:
        await safe_edit_text(
            q.message,
            "❌ Нельзя изменить услугу на это же время: новая длительность пересекается с другими записями или не влезает в график.\n\n"
            "Выберите другую услугу(и) или перенесите запись.",
        )
        return

    # формируем новую запись на то же время
    picked_svcs = []
    for sid in selected:
        svc = get_service_for_master(master_id, int(sid))
        if not svc or not svc.get("enabled", True):
            await safe_edit_text(q.message, "Одна из выбранных услуг стала недоступна. Выберите заново.")
            return
        picked_svcs.append(svc)

    total_price = sum(int(s.get("price", 0) or 0) for s in picked_svcs)
    total_duration = sum(int(s.get("duration", 0) or 0) for s in picked_svcs)
    names = [s.get("name", "") for s in picked_svcs if s.get("name")]
    new_name = " + ".join(names) if names else "Услуга"

    # отменяем старую запись
    cancel_cleanup_for_booking(booking_id, context)
    old["status"] = "CANCELLED"
    old["cancelled_by"] = "client"
    old["cancel_reason"] = "Изменение услуги (время сохранено)"
    await save_bookings_locked()

    new_booking = {
        "id": next_booking_id(),
        "client_id": old["client_id"],
        "client_username": old.get("client_username"),
        "client_full_name": old.get("client_full_name"),
        "master_id": master_id,

        "service_id": int(selected[0]),               # совместимость
        "service_ids": [int(x) for x in selected],    # новое
        "service_name": new_name,
        "service_price": int(total_price),
        "service_duration": int(total_duration),

        "date": old["date"],
        "time": old["time"],
        "status": "PENDING",
        "changed_from": booking_id,
    }

    bookings.append(new_booking)
    await save_bookings_locked()

    kb = [
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_{new_booking['id']}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"cancel_booking_{new_booking['id']}"),
        ],
        [InlineKeyboardButton("💬 Связаться с клиентом", callback_data=f"chat_{new_booking['id']}")],
    ]

    await context.bot.send_message(
        chat_id=master_id,
        text=(
            "✏️ Клиент просит изменить услугу (время НЕ меняется)\n"
            f"Старая запись: #{booking_id} {old.get('date','-')} {old.get('time','-')} — {old.get('service_name','-')}\n"
            f"Новая заявка: #{new_booking['id']} {new_booking['date']} {new_booking['time']} — {new_booking['service_name']}\n"
            f"👤 Клиент: {format_client(new_booking)}"
        ),
        reply_markup=InlineKeyboardMarkup(kb),
    )

    context.user_data.pop("client_chsvc", None)
    await safe_edit_text(q.message, "✅ Запрос на изменение услуги отправлен мастеру. Ожидайте подтверждения.")

async def client_cancel_choose(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    parts = q.data.split("_")
    booking_id = int(parts[3])
    action = parts[4]

    booking = get_booking(booking_id)
    if not booking:
        await safe_edit_text(q.message, "Запись не найдена.")
        return
    if booking.get("client_id") != q.from_user.id:
        await q.answer("Нет доступа", show_alert=True)
        return

    if action == "cancel":
        kb = [
            [InlineKeyboardButton(CLIENT_CANCEL_REASONS["changed_mind"], callback_data=f"client_cancel_reason_{booking_id}_changed_mind")],
            [InlineKeyboardButton(CLIENT_CANCEL_REASONS["cant_time"], callback_data=f"client_cancel_reason_{booking_id}_cant_time")],
            [InlineKeyboardButton(CLIENT_CANCEL_REASONS["other_master"], callback_data=f"client_cancel_reason_{booking_id}_other_master")],
            [InlineKeyboardButton(CLIENT_CANCEL_REASONS["other"], callback_data=f"client_cancel_reason_{booking_id}_other")],
            [InlineKeyboardButton("⬅ Назад", callback_data=f"client_cancel_{booking_id}")],
        ]
        await safe_edit_text(q.message, "Укажите причину отмены:", InlineKeyboardMarkup(kb))
        return

    context.user_data["client_resched"] = {"booking_id": booking_id, "offset": 0}
    await client_resched_show_date(q.message, context)

async def client_cancel_reason_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    parts = q.data.split("_")
    # формат: client_cancel_reason_<booking_id>_<reason_code...>
    if len(parts) < 5:
        await safe_edit_text(q.message, "Ошибка: некорректная кнопка.")
        return

    booking_id = int(parts[3])
    reason_code = "_".join(parts[4:])  # changed_mind / other_master / cant_time / other

    booking = get_booking(booking_id)
    if not booking:
        await safe_edit_text(q.message, "Запись не найдена.")
        return
    if booking.get("client_id") != q.from_user.id:
        await q.answer("Нет доступа", show_alert=True)
        return

    if reason_code == "other":
        context.user_data["client_cancel_reason_text"] = {"booking_id": booking_id}
        await safe_edit_text(q.message, "Напишите причину отмены одним сообщением.")
        return

    reason_text = CLIENT_CANCEL_REASONS.get(reason_code, "Причина не указана")
    await finalize_client_cancel(q.message, context, booking_id, reason_text)


async def finalize_client_cancel(msg_or_message, context: ContextTypes.DEFAULT_TYPE, booking_id: int, reason_text: str):
    booking = get_booking(booking_id)
    if not booking:
        if hasattr(msg_or_message, "edit_text"):
            await safe_edit_text(msg_or_message, "Запись не найдена.")
        else:
            await msg_or_message.reply_text("Запись не найдена.")
        return

    if booking.get("status") not in ("PENDING", "CONFIRMED"):
        if hasattr(msg_or_message, "edit_text"):
            await safe_edit_text(msg_or_message, "Эта запись уже обработана.")
        else:
            await msg_or_message.reply_text("Эта запись уже обработана.")
        return

    cancel_cleanup_for_booking(booking_id, context)

    booking["status"] = "CANCELLED"
    booking["cancelled_by"] = "client"
    booking["cancel_reason"] = reason_text
    await save_bookings_locked()

    try:
        await context.bot.send_message(
            chat_id=booking["master_id"],
            text=(
                f"❌ Клиент отменил запись #{booking_id}\n"
                f"👤 Клиент: {format_client(booking)}\n"
                f"💅 Услуга: {booking.get('service_name','-')}\n"
                f"📅 {booking.get('date','-')} {booking.get('time','-')}\n"
                f"Причина: {reason_text}"
            ),
        )
    except Exception:
        pass

    if hasattr(msg_or_message, "edit_text"):
        await safe_edit_text(msg_or_message, "❌ Запись отменена. Спасибо! ✅")
    else:
        await msg_or_message.reply_text("❌ Запись отменена. Спасибо! ✅")

# -----------------------------------------------------------------------------
# CLIENT: перенос
# -----------------------------------------------------------------------------
async def client_resched_show_date(message, context: ContextTypes.DEFAULT_TYPE):
    st = context.user_data.get("client_resched")
    if not st:
        await safe_edit_text(message, "Сеанс переноса устарел. Откройте /mybooking заново.")
        return

    booking = get_booking(st["booking_id"])
    if not booking:
        await safe_edit_text(message, "Запись не найдена.")
        return

    offset = int(st.get("offset", 0))

    def _calc():
        svc_ids = booking.get("service_ids") or booking["service_id"]
        return scan_available_days(booking["master_id"], offset, svc_ids, limit=DAYS_PER_PAGE)

    available_days, end_offset = await asyncio.to_thread(_calc)

    st["end_offset"] = end_offset

    kb = [[InlineKeyboardButton(d, callback_data=f"client_resched_date_{d}")] for d in available_days]

    kb.append([
        InlineKeyboardButton("◀", callback_data="client_resched_prev"),
        InlineKeyboardButton("▶", callback_data="client_resched_next"),
    ])
    kb.append([InlineKeyboardButton("⬅ Назад", callback_data=f"client_cancel_{st['booking_id']}")])

    if not available_days:
        await safe_edit_text(
            message,
            "Свободных дней не найдено. Попробуйте позже или выберите отмену записи.",
            InlineKeyboardMarkup(kb),
        )
        return

    await safe_edit_text(message, "Выберите новую дату:", InlineKeyboardMarkup(kb))

async def client_resched_next_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    st = context.user_data.get("client_resched")
    if not st:
        return

    hist = st.get("offset_hist")
    if not isinstance(hist, list):
        hist = []
        st["offset_hist"] = hist

    hist.append(int(st.get("offset", 0)))

    end_offset = st.get("end_offset")
    if end_offset is None:
        end_offset = int(st.get("offset", 0))

    st["offset"] = int(end_offset) + 1
    await client_resched_show_date(q.message, context)


async def client_resched_prev_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    st = context.user_data.get("client_resched")
    if not st:
        return

    hist = st.get("offset_hist")
    if isinstance(hist, list) and hist:
        st["offset"] = hist.pop()
    else:
        st["offset"] = 0

    await client_resched_show_date(q.message, context)

async def client_resched_choose_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    st = context.user_data.get("client_resched")
    if not st:
        await safe_edit_text(q.message, "Сеанс переноса устарел. Откройте /mybooking заново.")
        return

    date = q.data.replace("client_resched_date_", "", 1)
    st["date"] = date

    booking = get_booking(st["booking_id"])
    if not booking:
        await safe_edit_text(q.message, "Запись не найдена.")
        return

    svc_ids = booking.get("service_ids") or booking["service_id"]
    slots = await asyncio.to_thread(get_available_slots, booking["master_id"], date, svc_ids)

    if not slots:
        await safe_edit_text(q.message, "На этот день нет свободного времени. Выберите другую дату.")
        return

    kb = [[InlineKeyboardButton(t, callback_data=f"client_resched_time_{t}")] for t in slots]
    kb.append([InlineKeyboardButton("⬅ Назад", callback_data="client_resched_prev")])

    await safe_edit_text(q.message, f"Дата: {date}\nВыберите новое время:", InlineKeyboardMarkup(kb))

def build_services_kb(master_id: int, selected: list[int]) -> InlineKeyboardMarkup:
    services = list_services_for_master(master_id)

    rows = []
    for s in services:
        svc = get_service_for_master(master_id, s["id"])
        if not (svc and svc.get("enabled", True)):
            continue

        mark = "✅" if svc["id"] in selected else "▫️"
        rows.append([InlineKeyboardButton(
            f"{mark} {svc['name']} — {svc['price']}₽",
            callback_data=f"svc_pick_{svc['id']}"
        )])

    # навигация
    rows.append([
        InlineKeyboardButton("➡ Далее", callback_data="svc_next"),
        InlineKeyboardButton("⬅ Назад", callback_data="back"),
    ])
    return InlineKeyboardMarkup(rows)

async def show_services_step(message, ctx):
    master_id = ctx.get("master_id")
    selected = ctx.get("service_ids", [])
    await safe_edit_text(message, "Выберите услуги (можно несколько):", build_services_kb(master_id, selected))

async def svc_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = q.from_user.id
    ctx = user_context.get(user_id)
    if not ctx:
        await safe_edit_text(q.message, "Сеанс устарел. Введите /start")
        return

    sid = int(q.data.split("_")[-1])

    selected = ctx.setdefault("service_ids", [])
    if sid in selected:
        selected.remove(sid)
    else:
        selected.append(sid)

    # остаёмся на шаге SERVICE
    ctx["state"] = States.SERVICE
    await show_services_step(q.message, ctx)

async def svc_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = q.from_user.id
    ctx = user_context.get(user_id)
    if not ctx:
        await safe_edit_text(q.message, "Сеанс устарел. Введите /start")
        return

    if not ctx.get("service_ids"):
        await q.answer("Выберите хотя бы 1 услугу", show_alert=True)
        return

    ctx["state"] = States.DATE
    ctx["day_offset"] = 0
    await show_date_step(q.message, ctx)

async def client_resched_choose_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    st = context.user_data.get("client_resched")
    if not st or "date" not in st:
        await safe_edit_text(q.message, "Сеанс переноса устарел. Откройте /mybooking заново.")
        return

    new_time = q.data.replace("client_resched_time_", "", 1)
    new_date = st["date"]
    booking_id = st["booking_id"]

    old = get_booking(booking_id)
    if not old:
        await safe_edit_text(q.message, "Запись не найдена.")
        return

    svc_ids = old.get("service_ids") or old["service_id"]
    slots = await asyncio.to_thread(get_available_slots, old["master_id"], new_date, svc_ids)

    if new_time not in slots:
        await safe_edit_text(q.message, "Это время уже занято. Выберите другое время.")
        return

    cancel_cleanup_for_booking(booking_id, context)
    old["status"] = "CANCELLED"
    old["cancelled_by"] = "client"
    old["cancel_reason"] = "Перенос записи"
    await save_bookings_locked()

    new_booking = {
        "id": next_booking_id(),
        "client_id": old["client_id"],
        "client_username": old.get("client_username"),
        "client_full_name": old.get("client_full_name"),
        "master_id": old["master_id"],
        "service_id": old["service_id"],
        "service_name": old.get("service_name"),
        "service_price": old.get("service_price"),
        "service_duration": old.get("service_duration"),
        "date": new_date,
        "time": new_time,
        "status": "PENDING",
        "rescheduled_from": booking_id,
    }
    bookings.append(new_booking)
    await save_bookings_locked()

    kb = [
        [
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_{new_booking['id']}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"cancel_booking_{new_booking['id']}"),
        ],
        [InlineKeyboardButton("💬 Связаться с клиентом", callback_data=f"chat_{new_booking['id']}")],
    ]

    try:
        await context.bot.send_message(
            chat_id=new_booking["master_id"],
            text=(
                f"🔁 Клиент просит перенести запись\n"
                f"Старая: #{booking_id} {old.get('date','-')} {old.get('time','-')}\n"
                f"Новая заявка: #{new_booking['id']} {new_date} {new_time}\n"
                f"👤 Клиент: {format_client(new_booking)}\n"
                f"💅 Услуга: {new_booking.get('service_name','-')}"
            ),
            reply_markup=InlineKeyboardMarkup(kb),
        )
    except Exception:
        pass

    context.user_data.pop("client_resched", None)
    await safe_edit_text(q.message, "✅ Запрос на перенос отправлен мастеру. Ожидайте подтверждения.")

# -----------------------------------------------------------------------------
# MASTER MENU + закрытие дня/слотов + услуги
# -----------------------------------------------------------------------------
async def master_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_master(user_id):
        await update.message.reply_text("У вас нет доступа.")
        return

    keyboard = [
        [InlineKeyboardButton("📥 Заявки", callback_data="master_pending")],
        [InlineKeyboardButton("🗓 Календарь", callback_data="mcal_open")],
        [InlineKeyboardButton("📅 Записи", callback_data="master_confirmed")],
        [InlineKeyboardButton("🚫 Закрыть день / часы", callback_data="master_close_day")],
        [InlineKeyboardButton("🛠 Мои услуги", callback_data="master_services")],
        [InlineKeyboardButton("👤 Мой профиль", callback_data="master_profile")],  # ✅
    ]

    await update.message.reply_text("Меню мастера:", reply_markup=InlineKeyboardMarkup(keyboard))

async def back_to_master(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    await safe_edit_text(
        q.message,
        "Меню мастера:",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("📥 Заявки", callback_data="master_pending")],
            [InlineKeyboardButton("🗓 Календарь", callback_data="mcal_open")],
            [InlineKeyboardButton("📅 Записи", callback_data="master_confirmed")],
            [InlineKeyboardButton("🚫 Закрыть день / часы", callback_data="master_close_day")],
            [InlineKeyboardButton("🛠 Мои услуги", callback_data="master_services")],
            [InlineKeyboardButton("👤 Мой профиль", callback_data="master_profile")],
        ]),
    )

async def show_master_close_day_step(message, master_id: int, offset: int):
    days = get_days_page(offset, days_per_page=MASTER_DAYS_PER_PAGE)

    # дни, которые закрыты целиком (time=None)
    arr = blocked_slots.get(str(master_id), [])
    fully_blocked = {
        b.get("date")
        for b in arr
        if isinstance(b, dict) and b.get("date") and b.get("time") is None
    }

    # показываем только дни, которые НЕ закрыты целиком
    visible_days = [d for d in days if d not in fully_blocked]

    keyboard = []

    # кнопка "мои блокировки" (чтобы можно было открыть/снять блок)
    keyboard.append([InlineKeyboardButton("📋 Мои блокировки", callback_data="master_blocks")])
    keyboard.append([InlineKeyboardButton("📅 Заблокировать период", callback_data="block_period")])


    # список доступных дней
    for d in visible_days:
        keyboard.append([InlineKeyboardButton(d, callback_data=f"choose_block_type_{d}")])

    keyboard.append([
        InlineKeyboardButton("◀", callback_data="m_prev_days"),
        InlineKeyboardButton("▶", callback_data="m_next_days")
    ])
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="back_to_master")])

    text = "Выберите день для закрытия или редактирования:"
    if not visible_days:
        text += "\n\n(На этой странице все дни уже закрыты целиком — листайте ▶ или откройте «Мои блокировки».)"

    await safe_edit_text(message, text, InlineKeyboardMarkup(keyboard))

async def _render_block_period(message, master_id: int, offset: int, step: str, start_date: str | None):
    """
    step: "start" или "end"
    start_date: строка DATE_FORMAT, когда step="end"
    """
    days = get_days_page(offset, days_per_page=MASTER_DAYS_PER_PAGE)

    # если выбираем конец — нельзя выбрать дату раньше старта
    if step == "end" and start_date:
        try:
            sd = datetime.strptime(start_date, DATE_FORMAT).date()
        except Exception:
            sd = None

        if sd:
            filtered = []
            for d in days:
                try:
                    dd = datetime.strptime(d, DATE_FORMAT).date()
                except Exception:
                    continue
                if dd >= sd:
                    filtered.append(d)
            days = filtered

    keyboard = []
    for d in days:
        keyboard.append([InlineKeyboardButton(d, callback_data=f"bp_pick_{d}")])

    keyboard.append([
        InlineKeyboardButton("◀", callback_data="bp_prev"),
        InlineKeyboardButton("▶", callback_data="bp_next"),
    ])
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="master_close_day")])

    if step == "start":
        text = "📅 Блокировка периода\n\nВыберите ДАТУ НАЧАЛА:"
    else:
        text = f"📅 Блокировка периода\n\nДата начала: {start_date}\nВыберите ДАТУ КОНЦА:"

    # подсказка, если на странице нет доступных дат (например, фильтр по start_date всё вырезал)
    if not days:
        text += "\n\n(На этой странице нет подходящих дат — нажмите ▶)"

    await safe_edit_text(message, text, InlineKeyboardMarkup(keyboard))


async def block_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_master(q):
        return

    context.user_data["block_period"] = {"step": "start", "offset": 0, "start": None}
    await _render_block_period(q.message, q.from_user.id, 0, "start", None)


async def bp_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_master(q):
        return

    st = context.user_data.get("block_period")
    if not isinstance(st, dict):
        return

    st["offset"] = int(st.get("offset", 0)) + MASTER_DAYS_PER_PAGE
    await _render_block_period(q.message, q.from_user.id, st["offset"], st.get("step", "start"), st.get("start"))


async def bp_prev(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_master(q):
        return

    st = context.user_data.get("block_period")
    if not isinstance(st, dict):
        return

    st["offset"] = max(0, int(st.get("offset", 0)) - MASTER_DAYS_PER_PAGE)
    await _render_block_period(q.message, q.from_user.id, st["offset"], st.get("step", "start"), st.get("start"))


async def bp_pick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_master(q):
        return

    st = context.user_data.get("block_period")
    if not isinstance(st, dict):
        return

    date_s = q.data.replace("bp_pick_", "", 1)

    # валидируем дату
    try:
        picked = datetime.strptime(date_s, DATE_FORMAT).date()
    except Exception:
        await q.answer("Некорректная дата", show_alert=True)
        return

    # шаг 1: выбираем старт
    if st.get("step") == "start":
        st["start"] = date_s
        st["step"] = "end"
        st["offset"] = 0
        await _render_block_period(q.message, q.from_user.id, 0, "end", date_s)
        return

    # шаг 2: выбираем конец и закрываем диапазон
    start_s = st.get("start")
    if not start_s:
        # на всякий случай сброс
        context.user_data.pop("block_period", None)
        await safe_edit_text(q.message, "Ошибка состояния. Попробуйте ещё раз.", InlineKeyboardMarkup([
            [InlineKeyboardButton("📅 Заблокировать период", callback_data="block_period")],
            [InlineKeyboardButton("⬅ Назад", callback_data="master_close_day")]
        ]))
        return

    try:
        start_d = datetime.strptime(start_s, DATE_FORMAT).date()
    except Exception:
        await q.answer("Некорректная дата начала", show_alert=True)
        return

    end_d = picked
    if end_d < start_d:
        await q.answer("Конец раньше начала", show_alert=True)
        return

    # блокируем все дни
    cur = start_d
    count = 0
    while cur <= end_d:
        ds = cur.strftime(DATE_FORMAT)
        block_slot(q.from_user.id, ds, time=None)   # как в block_day :contentReference[oaicite:3]{index=3}
        count += 1
        cur += timedelta(days=1)

    await save_blocked_locked()

    context.user_data.pop("block_period", None)
    await safe_edit_text(
        q.message,
        f"✅ Период закрыт.\n"
        f"📅 {start_s} — {date_s}\n"
        f"Дней закрыто: {count}",
        InlineKeyboardMarkup([
            [InlineKeyboardButton("📋 Мои блокировки", callback_data="master_blocks")],
            [InlineKeyboardButton("⬅ Назад", callback_data="master_close_day")],
        ])
    )

async def master_close_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_master(q):
        return
    context.user_data["m_day_offset"] = 0
    await show_master_close_day_step(q.message, q.from_user.id, 0)

async def master_next_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_master(q):
        return

    offset = context.user_data.get("m_day_offset", 0) + MASTER_DAYS_PER_PAGE
    context.user_data["m_day_offset"] = offset
    await show_master_close_day_step(q.message, q.from_user.id, offset)

async def master_prev_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_master(q):
        return

    offset = max(0, context.user_data.get("m_day_offset", 0) - MASTER_DAYS_PER_PAGE)
    context.user_data["m_day_offset"] = offset
    await show_master_close_day_step(q.message, q.from_user.id, offset)

async def choose_block_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not await guard_master(q):
        return

    date = q.data.replace("choose_block_type_", "", 1)
    context.user_data["block_date"] = date

    keyboard = [
        [InlineKeyboardButton("🚫 Закрыть весь день", callback_data=f"block_day_{date}")],
        [InlineKeyboardButton("⏰ Закрыть отдельные часы", callback_data=f"block_hours_{date}")],
    ]
    await safe_edit_text(q.message, f"День {date}: выберите действие", InlineKeyboardMarkup(keyboard))

async def block_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not await guard_master(q):
        return

    date = q.data.split("_")[2]
    block_slot(q.from_user.id, date, time=None)
    await save_blocked_locked()   # ✅ ВОТ ЭТО ДОБАВЬ
    await safe_edit_text(q.message, f"День {date} закрыт ✅")

async def block_hours_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not await guard_master(q):
        return

    master_id = q.from_user.id
    date_s = q.data.split("_")[2]

    # состояние выбора времени
    st = context.user_data.get("block_hours_sel")
    if not isinstance(st, dict) or st.get("date") != date_s:
        st = {"date": date_s, "sel": set()}
        context.user_data["block_hours_sel"] = st
    # гарантируем set
    sel = st.get("sel")
    if not isinstance(sel, set):
        sel = set(sel or [])
        st["sel"] = sel

    await _render_block_hours_menu(q.message, master_id, date_s, sel)


def _blocked_times_for_day(master_id: int, date_s: str) -> tuple[bool, set[str]]:
    """Возвращает (day_blocked, blocked_times_set). day_blocked=True если день закрыт целиком (time=None)."""
    arr = blocked_slots.get(str(master_id), [])
    day_blocked = False
    bt: set[str] = set()
    if isinstance(arr, list):
        for it in arr:
            if not isinstance(it, dict):
                continue
            if str(it.get("date") or "") != date_s:
                continue
            t = it.get("time")
            if t is None:
                day_blocked = True
                continue
            bt.add(str(t))
    return day_blocked, bt


def _booked_start_times_for_day(master_id: int, date_s: str) -> set[str]:
    bt: set[str] = set()
    for b in bookings:
        if not isinstance(b, dict):
            continue
        if int(b.get("master_id", -1)) != int(master_id):
            continue
        if str(b.get("date") or "") != date_s:
            continue
        if b.get("status") not in ("PENDING", "CONFIRMED"):
            continue
        t = b.get("time")
        if t:
            bt.add(str(t))
    return bt

def _booked_intervals_for_day(master_id: int, date_s: str) -> list[tuple[int, int]]:
    """
    Возвращает интервалы (start_min, end_min) занятости по записям мастера на дату.
    """
    from config import PAUSE_MINUTES, TIME_STEP  # если в файле уже импортировано — не нужно
    pause = int(PAUSE_MINUTES or 0)
    if pause < 0:
        pause = 0

    def hhmm_to_min(hhmm: str):
        try:
            h, m = hhmm.split(":")
            return int(h) * 60 + int(m)
        except Exception:
            return None

    intervals: list[tuple[int, int]] = []
    for b in bookings:
        if b.get("master_id") != master_id:
            continue
        if b.get("date") != date_s:
            continue
        if b.get("status") not in ("PENDING", "CONFIRMED"):
            continue
        t = b.get("time")
        if not t:
            continue

        start = hhmm_to_min(t)
        if start is None:
            continue

        # длительность записи (если нет — fallback на TIME_STEP)
        try:
            dur = int(b.get("service_duration", 0) or 0)
        except Exception:
            dur = 0
        if dur <= 0:
            dur = int(TIME_STEP or 0)

        end = start + dur + pause
        intervals.append((start, end))

    return intervals

async def _render_block_hours_menu(message, master_id: int, date_s: str, sel: set[str]):
    work = get_master_schedule_from_data(master_id)
    if not work:
        await safe_edit_text(message, "Рабочий график не задан. Задайте график в профиле мастера.")
        return

    day_blocked, already_blocked = _blocked_times_for_day(master_id, date_s)
    if day_blocked:
        kb = [[InlineKeyboardButton("⬅ Назад", callback_data="master_close_day")]]
        await safe_edit_text(message, f"День {date_s} закрыт целиком (⛔). Чтобы закрывать отдельные часы — сначала снимите закрытие дня.", InlineKeyboardMarkup(kb))
        return

    booked_intervals = _booked_intervals_for_day(master_id, date_s)

    start_dt = datetime.strptime(date_s, DATE_FORMAT).replace(
        hour=work["start"].hour, minute=work["start"].minute, second=0, microsecond=0
    )
    end_dt = datetime.strptime(date_s, DATE_FORMAT).replace(
        hour=work["end"].hour, minute=work["end"].minute, second=0, microsecond=0
    )

    start_dt = ceil_to_step(start_dt, TIME_STEP)

    times: list[str] = []
    cur = start_dt
    while cur < end_dt:
        times.append(cur.strftime(TIME_FORMAT))
        cur += timedelta(minutes=TIME_STEP)

    # --- фильтры: прошлое / уже закрыто / пересечение с записями ---
    day = datetime.strptime(date_s, DATE_FORMAT).date()
    now = datetime.now().replace(second=0, microsecond=0)

    def hhmm_to_min(hhmm: str):
        try:
            h, m = hhmm.split(":")
            return int(h) * 60 + int(m)
        except Exception:
            return None

    available: list[str] = []
    for t in times:
        # уже закрыто
        if t in already_blocked:
            continue

        # прошлое (для сегодняшнего дня)
        if day == now.date():
            tm = datetime.strptime(t, TIME_FORMAT).time()
            dt = datetime.combine(day, tm)
            # если время уже прошло или прямо сейчас — не показываем
            if dt <= now:
                continue

        # пересечение с существующими записями (интервал слота = TIME_STEP)
        start = hhmm_to_min(t)
        if start is None:
            continue
        end = start + int(TIME_STEP or 0)

        if any(start < b_end and end > b_start for b_start, b_end in booked_intervals):
            continue

        available.append(t)


    if not available:
        kb = [[InlineKeyboardButton("⬅ Назад", callback_data="master_close_day")]]
        await safe_edit_text(message, f"На {date_s} нет доступных стартовых слотов для закрытия (все уже закрыты или заняты).", InlineKeyboardMarkup(kb))
        return

    keyboard = []
    row = []
    for t in available:
        label = f"✅ {t}" if t in sel else t
        row.append(InlineKeyboardButton(label, callback_data=f"block_time_{date_s}_{t}"))
        if len(row) == 4:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    action_row = []
    action_row.append(InlineKeyboardButton("✅ Закрыть выбранное", callback_data=f"block_apply_{date_s}"))
    action_row.append(InlineKeyboardButton("🧹 Очистить", callback_data=f"block_clear_{date_s}"))
    keyboard.append(action_row)

    keyboard.append([InlineKeyboardButton("📋 Мои блокировки", callback_data=f"block_view_{date_s}")])
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="master_close_day")])


    hint = "Выберите несколько слотов и нажмите «Закрыть выбранное»."
    await safe_edit_text(message, f"Закрытие часов на {date_s}:\n{hint}", InlineKeyboardMarkup(keyboard))


async def block_time_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тоггл выбора времени (не закрывает сразу)."""
    q = update.callback_query
    await q.answer()
    if not await guard_master(q):
        return

    parts = q.data.split("_")
    date_s, time_s = parts[2], parts[3]

    st = context.user_data.get("block_hours_sel")
    if not isinstance(st, dict) or st.get("date") != date_s:
        st = {"date": date_s, "sel": set()}
        context.user_data["block_hours_sel"] = st

    sel = st.get("sel")
    if not isinstance(sel, set):
        sel = set(sel or [])
        st["sel"] = sel

    # если слот уже закрыт — не показываем и не даём выбрать
    _, already_blocked = _blocked_times_for_day(q.from_user.id, date_s)
    if time_s in already_blocked:
        await q.answer("Этот слот уже закрыт", show_alert=True)
        return

    if time_s in sel:
        sel.remove(time_s)
    else:
        sel.add(time_s)

    await _render_block_hours_menu(q.message, q.from_user.id, date_s, sel)

async def _render_block_view_day(message, master_id: int, date: str):
    arr = blocked_slots.get(str(master_id), [])
    day_blocked = any(b.get("date") == date and b.get("time") is None for b in arr)
    times = sorted([b.get("time") for b in arr if b.get("date") == date and b.get("time")])

    lines = [f"📋 Блокировки на {date}:"]
    if day_blocked:
        lines.append("⛔ День закрыт целиком")
    if times:
        lines.append("Закрытые слоты: " + ", ".join(times))
    if (not day_blocked) and (not times):
        lines.append("Нет блокировок.")

    keyboard = []
    if day_blocked:
        keyboard.append([InlineKeyboardButton("✅ Открыть день (снять блокировку)", callback_data=f"unblock_day_{date}")])

    for t in times:
        keyboard.append([InlineKeyboardButton(f"❌ Открыть {t}", callback_data=f"unblock_time_{date}_{t}")])

    keyboard.append([InlineKeyboardButton("⏰ Закрыть ещё часы", callback_data=f"block_hours_{date}")])
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="master_blocks")])

    await safe_edit_text(message, "\n".join(lines), InlineKeyboardMarkup(keyboard))

async def block_view_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not await guard_master(q):
        return

    date = q.data.replace("block_view_", "", 1)  # block_view_YYYY-MM-DD
    master_id = q.from_user.id

    await _render_block_view_day(q.message, master_id, date)

async def master_blocks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_master(q):
        return

    master_id = q.from_user.id
    arr = blocked_slots.get(str(master_id), [])

    # собираем по датам
    by_date: dict[str, dict] = {}
    for b in arr:
        if not isinstance(b, dict):
            continue
        d = b.get("date")
        if not d:
            continue
        rec = by_date.setdefault(d, {"day_blocked": False, "times": []})
        if b.get("time") is None:
            rec["day_blocked"] = True
        else:
            rec["times"].append(str(b.get("time")))

    dates = sorted(by_date.keys())

    if not dates:
        kb = [
            [InlineKeyboardButton("⬅ Назад", callback_data="master_close_day")]
        ]
        await safe_edit_text(q.message, "Блокировок нет ✅", InlineKeyboardMarkup(kb))
        return

    keyboard = []
    for d in dates:
        rec = by_date[d]
        if rec["day_blocked"]:
            label = f"⛔ {d} (день)"
        else:
            n = len(set(rec["times"]))
            label = f"⏰ {d} ({n} сл.)"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"block_view_{d}")])

    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="master_close_day")])
    await safe_edit_text(q.message, "📋 Мои блокировки (выберите день):", InlineKeyboardMarkup(keyboard))

async def unblock_day_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not await guard_master(q):
        return

    date = q.data.replace("unblock_day_", "", 1)
    unblock_day(q.from_user.id, date)
    await save_blocked_locked()

    # показать обновлённый список
    await _render_block_view_day(q.message, q.from_user.id, date)

async def unblock_time_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not await guard_master(q):
        return

    parts = q.data.split("_", 3)  # unblock_time_YYYY-MM-DD_HH:MM
    date, time = parts[2], parts[3]
    unblock_time(q.from_user.id, date, time)
    await save_blocked_locked()

    await _render_block_view_day(q.message, q.from_user.id, date)

async def block_apply_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not await guard_master(q):
        return

    date_s = q.data.split("_")[2]
    st = context.user_data.get("block_hours_sel")
    sel: set[str] = set()
    if isinstance(st, dict) and st.get("date") == date_s:
        raw = st.get("sel")
        if isinstance(raw, set):
            sel = set(raw)
        elif isinstance(raw, list):
            sel = set(raw)

    if not sel:
        await q.answer("Ничего не выбрано", show_alert=True)
        return

    # закрываем выбранное одним сохранением
    master_id = q.from_user.id
    for t in sorted(sel):
        block_time(master_id, date_s, t)

    await save_blocked_locked()

    # очищаем выбор
    context.user_data["block_hours_sel"] = {"date": date_s, "sel": set()}

    keyboard = [
        [InlineKeyboardButton("📋 Мои блокировки", callback_data=f"block_view_{date_s}")],
        [InlineKeyboardButton("⏰ Закрыть ещё часы", callback_data=f"block_hours_{date_s}")],
        [InlineKeyboardButton("⬅ Назад", callback_data="master_close_day")],
    ]
    await safe_edit_text(q.message, f"Закрыто слотов: {len(sel)} ✅", InlineKeyboardMarkup(keyboard))



async def block_clear_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not await guard_master(q):
        return

    date_s = q.data.split("_")[2]
    context.user_data["block_hours_sel"] = {"date": date_s, "sel": set()}
    await _render_block_hours_menu(q.message, q.from_user.id, date_s, set())


# -----------------------------------------------------------------------------
# MASTER: услуги
# -----------------------------------------------------------------------------
async def master_services(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    master_id = q.from_user.id
    if not is_master(master_id):
        await q.answer("Нет доступа", show_alert=True)
        return

    services = list_services_for_master(master_id)
    keyboard = []
    for s in services:
        svc = get_service_for_master(master_id, s["id"])
        if not svc:
            continue
        keyboard.append([InlineKeyboardButton(format_service_line(svc), callback_data=f"svc_manage_{svc['id']}")])

    keyboard.append([InlineKeyboardButton("➕ Добавить услугу", callback_data="svc_add")])
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="back_to_master")])

    await safe_edit_text(q.message, "🛠 Мои услуги (нажмите, чтобы управлять):", InlineKeyboardMarkup(keyboard))

async def svc_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not is_master(q.from_user.id):
        await q.answer("Нет доступа", show_alert=True)
        return

    context.user_data["svc_add"] = {"step": "name"}
    await safe_edit_text(q.message, "Введите название новой услуги (например: 'Коррекция бровей')")

async def svc_add_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    if not is_master(user_id):
        return False

    state = context.user_data.get("svc_add")
    if not state:
        return False

    text = (update.message.text or "").strip()

    if state["step"] == "name":
        if len(text) < 2:
            await update.message.reply_text("Название слишком короткое. Попробуйте ещё раз.")
            return True
        state["name"] = text
        state["step"] = "price"
        await update.message.reply_text("Введите цену (число), например: 2500")
        return True

    if state["step"] == "price":
        if not text.isdigit():
            await update.message.reply_text("Нужно число. Введите цену ещё раз.")
            return True
        price = int(text)
        if price <= 0:
            await update.message.reply_text("Цена должна быть > 0.")
            return True
        state["price"] = price
        state["step"] = "duration"
        await update.message.reply_text("Введите длительность в минутах (число), например: 90")
        return True

    if state["step"] == "duration":
        if not text.isdigit():
            await update.message.reply_text("Нужно число. Введите длительность ещё раз.")
            return True

        duration = int(text)
        if duration <= 0:
            await update.message.reply_text("Длительность должна быть > 0.")
            return True
        if duration % TIME_STEP != 0:
            await update.message.reply_text(f"Длительность должна быть кратна {TIME_STEP} минутам (например: 30, 60, 90, 120).")
            return True

        sid = next_service_id(user_id)
        new_service = {"id": sid, "name": state["name"], "duration": duration, "price": state["price"]}

        services_custom.setdefault(str(user_id), [])
        services_custom[str(user_id)].append(new_service)
        await save_services_custom_locked()

        await set_service_override(user_id, sid, enabled=True)

        context.user_data.pop("svc_add", None)
        await update.message.reply_text(f"✅ Услуга добавлена: {new_service['name']}")
        await update.message.reply_text("Откройте /master → 🛠 Мои услуги, чтобы увидеть её в списке.")
        return True

    return False

async def svc_manage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    master_id = q.from_user.id
    if not is_master(master_id):
        await q.answer("Нет доступа", show_alert=True)
        return

    service_id = int(q.data.split("_")[2])
    svc = get_service_for_master(master_id, service_id)
    if not svc:
        await safe_edit_text(q.message, "Услуга не найдена.")
        return

    enabled = svc.get("enabled", True)
    toggle_text = "🚫 Скрыть услугу" if enabled else "✅ Показать услугу"

    keyboard = [
        [InlineKeyboardButton("💰 Изменить цену", callback_data=f"svc_edit_price_{service_id}")],
        [InlineKeyboardButton("✏️ Переименовать", callback_data=f"svc_rename_{service_id}")],
        [InlineKeyboardButton("🗑 Удалить", callback_data=f"svc_delete_{service_id}")],
        [InlineKeyboardButton("⏱ Изменить длительность", callback_data=f"svc_edit_duration_{service_id}")],
        [InlineKeyboardButton(toggle_text, callback_data=f"svc_toggle_{service_id}")],
        [InlineKeyboardButton("⬅ К списку", callback_data="master_services")],
    ]

    text = (
        "Управление услугой:\n\n"
        f"Название: {svc['name']}\n"
        f"Цена: {svc['price']} ₽\n"
        f"Длительность: {fmt_duration(svc['duration'])}\n"
        f"Статус: {'включена' if enabled else 'скрыта'}"
    )
    await safe_edit_text(q.message, text, InlineKeyboardMarkup(keyboard))

async def svc_rename_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    mid = q.from_user.id
    if not is_master(mid):
        await q.answer("Нет доступа", show_alert=True)
        return

    service_id = int(q.data.split("_")[2])
    svc = get_service_for_master(mid, service_id)
    if not svc:
        await safe_edit_text(q.message, "Услуга не найдена.")
        return

    context.user_data["svc_rename"] = {"service_id": service_id}
    await safe_edit_text(q.message, f"Введите новое название для услуги:\n\nТекущее: {svc['name']}")

async def svc_delete_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    mid = q.from_user.id
    if not is_master(mid):
        await q.answer("Нет доступа", show_alert=True)
        return

    service_id = int(q.data.split("_")[2])
    svc = get_service_for_master(mid, service_id)
    if not svc:
        await safe_edit_text(q.message, "Услуга не найдена.")
        return

    kb = [
        [InlineKeyboardButton("✅ Да, удалить", callback_data=f"svc_delete_do_{service_id}")],
        [InlineKeyboardButton("⬅ Назад", callback_data=f"svc_manage_{service_id}")],
    ]
    await safe_edit_text(q.message, f"Удалить услугу:\n\n{svc['name']}\n\nЭто действие нельзя отменить.", InlineKeyboardMarkup(kb))

async def svc_delete_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    mid = q.from_user.id
    if not is_master(mid):
        await q.answer("Нет доступа", show_alert=True)
        return

    service_id = int(q.data.split("_")[3])

    # 1) удалить из services_custom (если кастомная)
    changed_services_custom = False
    arr = services_custom.get(str(mid), [])
    if isinstance(arr, list):
        before = len(arr)
        arr = [s for s in arr if int(s.get("id", -1)) != service_id]
        if len(arr) != before:
            services_custom[str(mid)] = arr
            changed_services_custom = True

    # 2) удалить из masters_custom[*].services (если базовая)
    changed_master_services = False
    ensure_master_profile(mid)
    base = masters_custom[str(mid)].get("services", [])
    if isinstance(base, list):
        before = len(base)
        base = [s for s in base if int(s.get("id", -1)) != service_id]
        if len(base) != before:
            masters_custom[str(mid)]["services"] = base
            changed_master_services = True

    # 3) удалить overrides (чтобы не было хвостов)
    if str(mid) in service_overrides and isinstance(service_overrides[str(mid)], dict):
        service_overrides[str(mid)].pop(str(service_id), None)

    if changed_services_custom:
        await save_services_custom_locked()
    if changed_master_services:
        await save_masters_custom_locked()
    await save_service_overrides_locked()

    await safe_edit_text(q.message, "✅ Услуга удалена.")

async def svc_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    master_id = q.from_user.id
    if not is_master(master_id):
        await q.answer("Нет доступа", show_alert=True)
        return

    service_id = int(q.data.split("_")[2])
    svc = get_service_for_master(master_id, service_id)
    if not svc:
        await safe_edit_text(q.message, "Услуга не найдена.")
        return

    await set_service_override(master_id, service_id, enabled=not svc.get("enabled", True))
    await svc_manage(update, context)

async def svc_edit_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    master_id = q.from_user.id
    if not is_master(master_id):
        await q.answer("Нет доступа", show_alert=True)
        return

    service_id = int(q.data.split("_")[3])
    context.user_data["svc_edit"] = {"field": "price", "service_id": service_id}
    await safe_edit_text(q.message, "Введите новую цену (только число), например: 2500")

async def svc_edit_duration(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    master_id = q.from_user.id
    if not is_master(master_id):
        await q.answer("Нет доступа", show_alert=True)
        return

    service_id = int(q.data.split("_")[3])
    context.user_data["svc_edit"] = {"field": "duration", "service_id": service_id}
    await safe_edit_text(q.message, "Введите новую длительность в минутах (только число), например: 90")

# -----------------------------------------------------------------------------
# MASTER: заявки/записи
# -----------------------------------------------------------------------------
async def master_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    master_id = q.from_user.id
    if not is_master(master_id):
        return

    pending = [b for b in bookings if b.get("master_id") == master_id and b.get("status") == "PENDING"]
    if not pending:
        await safe_edit_text(q.message, "Нет заявок.")
        return

    for b in pending:
        keyboard = [[InlineKeyboardButton("✅", callback_data=f"confirm_{b['id']}"), InlineKeyboardButton("❌", callback_data=f"cancel_booking_{b['id']}")]]
        await context.bot.send_message(
            chat_id=master_id,
            text=f"Заявка #{b['id']}\nКлиент: {format_client(b)}\nДата: {b['date']}\nВремя: {b['time']}",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    await safe_edit_text(q.message, "Заявки отправлены.")

def _parse_ym(ym: str) -> tuple[int, int]:
    y, m = ym.split("-")
    return int(y), int(m)

RU_MONTHS = {
    1: "Январь",  2: "Февраль", 3: "Март",     4: "Апрель",
    5: "Май",     6: "Июнь",    7: "Июль",     8: "Август",
    9: "Сентябрь",10: "Октябрь",11: "Ноябрь",  12: "Декабрь",
}

def _fmt_month_title(y: int, m: int) -> str:
    return f"{RU_MONTHS.get(m, str(m))} {y}"

def _weekday_idx(d: date) -> int:
    return d.weekday()  # Mon=0..Sun=6

def _work_minutes_for_day(master_id: int, d: date) -> int:
    mid = str(master_id)
    sch = (masters_custom.get(mid, {}) or {}).get("schedule", {}) or {}

    days = sch.get("days", [])
    if not isinstance(days, list) or _weekday_idx(d) not in days:
        return 0

    start = sch.get("start") or ""
    end = sch.get("end") or ""
    if not start or not end:
        return 0

    try:
        st = datetime.strptime(start, "%H:%M")
        en = datetime.strptime(end, "%H:%M")
        return max(0, int((en - st).total_seconds() // 60))
    except Exception:
        return 0

def _booked_minutes_for_day(master_id: int, day_obj: date) -> tuple[int, int]:
    mins = 0
    cnt = 0
    for b in bookings:
        if not isinstance(b, dict):
            continue
        if int(b.get("master_id", -1)) != int(master_id):
            continue
        if b.get("status") not in ("PENDING", "CONFIRMED"):
            continue

        bdate = b.get("date") or ""
        try:
            b_day = datetime.strptime(bdate, DATE_FORMAT).date()
        except Exception:
            continue
        if b_day != day_obj:
            continue

        dur = _booking_duration_minutes(master_id, b)  # <-- ВОТ КЛЮЧЕВОЕ
        mins += max(0, dur)
        cnt += 1

    return mins, cnt

def _booking_duration_minutes(master_id: int, b: dict) -> int:
    # 1) пробуем взять готовую сумму
    raw = b.get("service_duration")
    try:
        dur = int(raw)
    except Exception:
        dur = 0
    if dur > 0:
        return dur

    # 2) fallback: считаем по service_ids / service_id
    svc_ids = b.get("service_ids")
    if isinstance(svc_ids, list) and svc_ids:
        ids = svc_ids
    else:
        sid = b.get("service_id")
        ids = [sid] if sid is not None else []

    total = 0
    for sid in ids:
        try:
            sid_i = int(sid)
        except Exception:
            continue
        svc = get_service_for_master(master_id, sid_i)
        if not svc:
            continue
        try:
            total += int(svc.get("duration", 0) or 0)
        except Exception:
            pass
    return max(0, total)

def _is_day_blocked(master_id: int, date_str: str) -> bool:
    # блокировка всего дня = time is None
    arr = blocked_slots.get(str(master_id), [])
    return any((b.get("date") == date_str and b.get("time") is None) for b in arr)

def _blocked_minutes_for_day(master_id: int, date_str: str, work_min: int) -> int:
    # если закрыт весь день — считаем, что занято всё рабочее время
    if _is_day_blocked(master_id, date_str):
        return max(0, int(work_min or 0))
    # иначе считаем заблокированные слоты по TIME_STEP
    arr = blocked_slots.get(str(master_id), [])
    cnt = sum(1 for b in arr if b.get("date") == date_str and b.get("time"))
    return cnt * int(TIME_STEP or 0)

def _min_enabled_service_id(master_id: int) -> tuple[int, int] | None:
    # берём минимальную длительность среди включённых услуг мастера
    best = None
    for s in list_services_for_master(master_id):
        sid = s.get("id")
        if sid is None:
            continue
        try:
            sid = int(sid)
        except Exception:
            continue
        svc = get_service_for_master(master_id, sid)  # учитывает overrides/enabled :contentReference[oaicite:5]{index=5}
        if not svc or not svc.get("enabled", True):
            continue
        try:
            dur = int(svc.get("duration", 0) or 0)
        except Exception:
            dur = 0
        if dur <= 0:
            continue
        if best is None or dur < best[1]:
            best = (sid, dur)
    return best

def _total_start_slots_for_day(master_id: int, d: date, duration_min: int) -> int:
    sch = get_master_schedule_from_data(master_id)  # :contentReference[oaicite:6]{index=6}
    if not sch:
        return 0
    if d.weekday() not in (sch.get("days") or []):
        return 0

    st = sch["start"]
    en = sch["end"]

    start_min = st.hour * 60 + st.minute
    end_min = en.hour * 60 + en.minute

    step = int(TIME_STEP or 0)
    if step <= 0:
        return 0

    # выравниваем начало по шагу
    r = start_min % step
    if r:
        start_min += (step - r)

    # учитываем паузу между клиентами
    # (PAUSE_MINUTES у вас используется в schedule.py, поэтому она должна быть в config.py)
    from config import PAUSE_MINUTES
    pause = int(PAUSE_MINUTES or 0)
    if pause < 0:
        pause = 0

    last_start = end_min - (int(duration_min) + pause)
    if last_start < start_min:
        return 0

    return ((last_start - start_min) // step) + 1

def _calendar_symbol_for_day(master_id: int, d: date) -> str:
    # 1) выходной по графику
    work = _work_minutes_for_day(master_id, d)
    if work <= 0:
        return "—"

    date_str = d.strftime(DATE_FORMAT)

    # 2) закрыт целиком
    if _is_day_blocked(master_id, date_str):
        return "⛔"

    # 3) занятость = брони + точечные блокировки
    booked_min, cnt = _booked_minutes_for_day(master_id, d)
    blocked_min = _blocked_minutes_for_day(master_id, date_str, work)

    # если есть хоть что-то (записи или блокировки) — считаем день занятым
    if cnt > 0 or blocked_min > 0 or booked_min > 0:
        return "●"

    return "○"

async def master_cal_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    master_id = q.from_user.id
    if not is_master(master_id):
        await q.answer("Нет доступа", show_alert=True)
        return

    today = date.today()
    ym = today.strftime("%Y-%m")
    await master_cal_month_render(q.message, master_id, ym)

async def master_cal_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    master_id = q.from_user.id
    if not is_master(master_id):
        await q.answer("Нет доступа", show_alert=True)
        return

    ym = q.data.split("mcal_month_")[1]  # YYYY-MM
    await master_cal_month_render(q.message, master_id, ym)

async def master_cal_month_render(message, master_id: int, ym: str):
    y, m = _parse_ym(ym)
    cal = calendar.Calendar(firstweekday=0)  # Monday=0

    # Заголовок
    title = f"🗓 {_fmt_month_title(y, m)}\n(○ свободно • ◐ частично • ● занято • ⛔ закрыто • — выходной)"

    # Сетка дней
    weeks = cal.monthdayscalendar(y, m)  # 0 = пустые
    kb = []

    # строка дней недели
    kb.append([
        InlineKeyboardButton("Пн", callback_data="noop"),
        InlineKeyboardButton("Вт", callback_data="noop"),
        InlineKeyboardButton("Ср", callback_data="noop"),
        InlineKeyboardButton("Чт", callback_data="noop"),
        InlineKeyboardButton("Пт", callback_data="noop"),
        InlineKeyboardButton("Сб", callback_data="noop"),
        InlineKeyboardButton("Вс", callback_data="noop"),
    ])

    for w in weeks:
        row = []
        for day_num in w:
            if day_num == 0:
                row.append(InlineKeyboardButton(" ", callback_data="noop"))
                continue

            d = date(y, m, day_num)
            ymd = d.strftime("%Y-%m-%d")

            sym = _calendar_symbol_for_day(master_id, d)


            label = f"{day_num}{sym}"
            row.append(InlineKeyboardButton(label, callback_data=f"mcal_day_{ymd}"))

        kb.append(row)

    # Навигация по месяцам
    first = date(y, m, 1)
    prev_month = (first - timedelta(days=1)).replace(day=1)
    next_month = (first.replace(day=28) + timedelta(days=4)).replace(day=1)

    kb.append([
        InlineKeyboardButton("⬅", callback_data=f"mcal_month_{prev_month.strftime('%Y-%m')}"),
        InlineKeyboardButton("Сегодня", callback_data=f"mcal_month_{date.today().strftime('%Y-%m')}"),
        InlineKeyboardButton("➡", callback_data=f"mcal_month_{next_month.strftime('%Y-%m')}"),
    ])
    kb.append([InlineKeyboardButton("⬅ В меню мастера", callback_data="back_to_master")])

    await safe_edit_text(message, title, InlineKeyboardMarkup(kb))

async def master_cal_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    master_id = q.from_user.id
    if not is_master(master_id):
        await q.answer("Нет доступа", show_alert=True)
        return

    ymd = q.data.split("mcal_day_")[1]
    d = datetime.strptime(ymd, "%Y-%m-%d").date()  # <-- ВОТ ЭТОГО НЕ ХВАТАЛО

    # Собираем записи дня
    items = []
    for b in bookings:
        if not isinstance(b, dict):
            continue
        if int(b.get("master_id", -1)) != int(master_id):
            continue
        bdate = b.get("date") or ""
        try:
            b_day = datetime.strptime(bdate, DATE_FORMAT).date()
        except Exception:
            continue
        if b_day != d:
            continue
        if b.get("status") not in ("PENDING", "CONFIRMED"):
            continue
        items.append(b)

    items.sort(key=lambda x: (x.get("time") or ""))

    work = _work_minutes_for_day(master_id, d)
    booked, cnt = _booked_minutes_for_day(master_id, d)
    blocked_min = _blocked_minutes_for_day(master_id, d.strftime(DATE_FORMAT), work)
    busy_total = min(work, booked + blocked_min)


    header = (
        f"📅 {d.strftime('%d.%m.%Y')}\n"
        f"Записей: {cnt}\n"
        f"Занято: {fmt_duration(busy_total)} из {fmt_duration(work)}\n"
    )

    if not items:
        body = "\nСвободно."
    else:
        lines = []
        for b in items:
            t = b.get("time") or ""
            s = b.get("service_name") or ""
            dur = fmt_duration(_booking_duration_minutes(master_id, b))
            client = format_client(b)
            st = b.get("status")
            lines.append(f"• {t} — {s} ({dur}) — {client} — {st}")
        body = "\n" + "\n".join(lines)

    ym = d.strftime("%Y-%m")
    kb = [
        [InlineKeyboardButton("⬅ Назад к месяцу", callback_data=f"mcal_month_{ym}")],
        [InlineKeyboardButton("⬅ В меню мастера", callback_data="back_to_master")],
    ]
    await safe_edit_text(q.message, header + body, InlineKeyboardMarkup(kb))


# ---------------------------
# ADMIN: календарь мастеров (просмотр)
# ---------------------------
async def admin_mcal_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    try:
        master_id = int(q.data.split("_")[-1])
    except Exception:
        await q.answer("Некорректный мастер", show_alert=True)
        return

    today = date.today()
    ym = today.strftime("%Y-%m")
    await admin_mcal_month_render(q.message, master_id, ym)


async def admin_mcal_month(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    # admin_mcal_month_<mid>_<YYYY-MM>
    parts = q.data.split("_")
    try:
        master_id = int(parts[3])
        ym = parts[4]
    except Exception:
        await q.answer("Некорректные параметры", show_alert=True)
        return

    await admin_mcal_month_render(q.message, master_id, ym)


async def admin_mcal_month_render(message, master_id: int, ym: str):
    y, m = _parse_ym(ym)
    cal = calendar.Calendar(firstweekday=0)

    mname = (get_all_masters().get(master_id) or {}).get("name") or str(master_id)
    title = f"🗓 {_fmt_month_title(y, m)} — {mname}\n(○ свободно • ● занято • ⛔ закрыто • — выходной)"

    weeks = cal.monthdayscalendar(y, m)
    kb = []
    kb.append([
        InlineKeyboardButton("Пн", callback_data="noop"),
        InlineKeyboardButton("Вт", callback_data="noop"),
        InlineKeyboardButton("Ср", callback_data="noop"),
        InlineKeyboardButton("Чт", callback_data="noop"),
        InlineKeyboardButton("Пт", callback_data="noop"),
        InlineKeyboardButton("Сб", callback_data="noop"),
        InlineKeyboardButton("Вс", callback_data="noop"),
    ])

    for w in weeks:
        row = []
        for day_num in w:
            if day_num == 0:
                row.append(InlineKeyboardButton(" ", callback_data="noop"))
                continue
            d = date(y, m, day_num)
            ymd = d.strftime("%Y-%m-%d")
            sym = _calendar_symbol_for_day(master_id, d)
            row.append(InlineKeyboardButton(f"{day_num}{sym}", callback_data=f"admin_mcal_day_{master_id}_{ymd}"))
        kb.append(row)

    first = date(y, m, 1)
    prev_month = (first - timedelta(days=1)).replace(day=1)
    next_month = (first.replace(day=28) + timedelta(days=4)).replace(day=1)

    kb.append([
        InlineKeyboardButton("⬅", callback_data=f"admin_mcal_month_{master_id}_{prev_month.strftime('%Y-%m')}"),
        InlineKeyboardButton("Сегодня", callback_data=f"admin_mcal_month_{master_id}_{date.today().strftime('%Y-%m')}"),
        InlineKeyboardButton("➡", callback_data=f"admin_mcal_month_{master_id}_{next_month.strftime('%Y-%m')}"),
    ])
    kb.append([InlineKeyboardButton("⬅ К мастеру", callback_data=f"admin_master_{master_id}")])

    await safe_edit_text(message, title, InlineKeyboardMarkup(kb))


async def admin_mcal_day(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    # admin_mcal_day_<mid>_<YYYY-MM-DD>
    parts = q.data.split("_")
    try:
        master_id = int(parts[3])
        ymd = parts[4]
    except Exception:
        await q.answer("Некорректные параметры", show_alert=True)
        return

    d = datetime.strptime(ymd, "%Y-%m-%d").date()

    items = []
    for b in bookings:
        if not isinstance(b, dict):
            continue
        if int(b.get("master_id", -1)) != int(master_id):
            continue
        bdate = b.get("date") or ""
        try:
            b_day = datetime.strptime(bdate, DATE_FORMAT).date()
        except Exception:
            continue
        if b_day != d:
            continue
        if b.get("status") not in ("PENDING", "CONFIRMED"):
            continue
        items.append(b)

    items.sort(key=lambda x: (x.get("time") or ""))

    work = _work_minutes_for_day(master_id, d)
    booked, cnt = _booked_minutes_for_day(master_id, d)
    blocked_min = _blocked_minutes_for_day(master_id, d.strftime(DATE_FORMAT), work)
    busy_total = min(work, booked + blocked_min)

    header = (
        f"📅 {d.strftime('%d.%m.%Y')}\n"
        f"Записей: {cnt}\n"
        f"Занято: {fmt_duration(busy_total)} из {fmt_duration(work)}\n"
    )

    if not items:
        body = "\nСвободно."
    else:
        lines = []
        for b in items:
            t = b.get("time") or ""
            s = b.get("service_name") or ""
            dur = fmt_duration(_booking_duration_minutes(master_id, b))
            client = format_client(b)
            st = b.get("status")
            lines.append(f"• {t} — {s} ({dur}) — {client} — {st}")
        body = "\n" + "\n".join(lines)

    ym = d.strftime("%Y-%m")
    kb = [
        [InlineKeyboardButton("⬅ Назад к месяцу", callback_data=f"admin_mcal_month_{master_id}_{ym}")],
        [InlineKeyboardButton("⬅ К мастеру", callback_data=f"admin_master_{master_id}")],
    ]
    await safe_edit_text(q.message, header + body, InlineKeyboardMarkup(kb))


async def master_confirmed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    master_id = q.from_user.id
    now = datetime.now()

    records = []
    for b in bookings:
        if b.get("master_id") != master_id:
            continue
        if b.get("status") != "CONFIRMED":
            continue

        end_dt = _booking_end_dt(b)  # у тебя эта функция уже есть выше
        if end_dt is None:
            # если почему-то не смогли посчитать конец — считаем по старту
            start_dt = parse_booking_dt(b)
            if start_dt and start_dt >= now:
                records.append(b)
            continue

        # показываем только если сеанс ещё не закончился
        if end_dt >= now:
            records.append(b)


    # сортируем по дате/времени начала
    records.sort(key=lambda x: (parse_booking_dt(x) or datetime.max))

    if not records:
        await safe_edit_text(q.message, "Подтверждённых записей нет.")
        return

    for b in records:
        keyboard = [[InlineKeyboardButton("🔁 Перенести", callback_data=f"reschedule_{b['id']}"), InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_master_{b['id']}")]]
        await context.bot.send_message(
            chat_id=master_id,
            text=f"#{b['id']}\n{b['date']} {b['time']}\nУслуга: {b['service_name']}\nКлиент: {format_client(b)}",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    await safe_edit_text(q.message, "Ваши записи:")

async def cancel_by_master(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    booking_id = int(q.data.split("_")[2])
    master_id = q.from_user.id

    booking = get_booking(booking_id)
    if not booking or booking.get("master_id") != master_id:
        await safe_edit_text(q.message, "Запись не найдена.")
        return

    cancel_cleanup_for_booking(booking_id, context)
    booking["status"] = "CANCELLED"
    await save_bookings_locked()

    await safe_edit_text(q.message, "Запись отменена ❌")

    await context.bot.send_message(
        chat_id=booking["client_id"],
        text=(
            "❌ Ваша запись была отменена мастером.\n\n"
            f"{booking['date']} {booking['time']}\n"
            f"Услуга: {booking['service_name']}"
        ),
    )

# -----------------------------------------------------------------------------
# MASTER: перенос записи (мастер)
# -----------------------------------------------------------------------------
async def start_reschedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    booking_id = int(q.data.split("_")[1])
    master_id = q.from_user.id

    booking = get_booking(booking_id)
    if not booking or booking.get("master_id") != master_id:
        await safe_edit_text(q.message, "Запись не найдена.")
        return

    context.user_data["reschedule_booking"] = booking_id

    days = get_next_days(14)
    keyboard = [[InlineKeyboardButton(d, callback_data=f"resched_date_{d}")] for d in days]

    await safe_edit_text(q.message, "Выберите новую дату:", InlineKeyboardMarkup(keyboard))

async def reschedule_choose_date(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    date = q.data.split("_")[2]
    booking_id = context.user_data.get("reschedule_booking")

    booking = get_booking(booking_id)
    if not booking:
        await safe_edit_text(q.message, "Запись не найдена.")
        return

    if q.from_user.id != booking["master_id"]:
        await q.answer("Нет доступа", show_alert=True)
        return

    slots = await asyncio.to_thread(get_available_slots, booking["master_id"], date, booking["service_id"], ignore_min_advance=True)

    if not slots:
        await safe_edit_text(q.message, "Нет свободных слотов. Выберите другую дату.")
        return

    context.user_data["reschedule_date"] = date
    keyboard = [[InlineKeyboardButton(t, callback_data=f"resched_time_{t}")] for t in slots]
    await safe_edit_text(q.message, f"Дата {date}. Выберите время:", InlineKeyboardMarkup(keyboard))

async def reschedule_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    time = q.data.split("_")[2]
    booking_id = context.user_data.get("reschedule_booking")
    date = context.user_data.get("reschedule_date")

    booking = get_booking(booking_id)
    if not booking:
        await safe_edit_text(q.message, "Запись не найдена.")
        return

    booking["date"] = date
    booking["time"] = time
    await save_bookings_locked()

    schedule_reminders_for_booking(context.job_queue, booking)
    schedule_followup_for_booking(context.job_queue, booking)

    context.user_data.clear()
    await safe_edit_text(q.message, "Запись перенесена ✅")

    await context.bot.send_message(
        chat_id=booking["client_id"],
        text=("🔁 Ваша запись была перенесена мастером.\n\n" f"Новая дата: {date}\n" f"Новое время: {time}"),
    )

# -----------------------------------------------------------------------------
# ADMIN: бронирования/статистика/мастера/настройки
# -----------------------------------------------------------------------------
def filter_bookings_by_days(days: int | None):
    # оставил твою исходную логику "в обе стороны", как было
    if days is None:
        items = list(bookings)
    else:
        now = datetime.now()
        start = (now - timedelta(days=days)).date()
        end = (now + timedelta(days=days)).date()
        items = []
        for b in bookings:
            try:
                d = datetime.strptime(b["date"], DATE_FORMAT).date()
            except Exception:
                continue
            if start <= d <= end:
                items.append(b)

    items.sort(key=sort_booking_key)
    return items

async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not is_admin(user_id):
        await update.message.reply_text("У вас нет доступа.")
        return

    keyboard = [
        [InlineKeyboardButton("👮 Админы", callback_data="admin_admins")],
        [InlineKeyboardButton("👥 Мастера", callback_data="admin_masters")],
        [InlineKeyboardButton("📒 Записи", callback_data="admin_bookings")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("📦 Бэкап данных", callback_data="admin_backup")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings")],
    ]

    await safe_send(chat_id=update.effective_chat.id, context=context, text="Админка:", reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_admins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    admins = sorted(get_dynamic_admin_ids())

    profiles = ADMIN_SETTINGS.get("admin_profiles", {}) if isinstance(ADMIN_SETTINGS, dict) else {}
    lines = []
    for aid in admins:
        p = profiles.get(str(aid), {})
        name = (p.get("name") or "").strip() or str(aid)
        username = (p.get("username") or "").strip()
        if username:
            lines.append(f"• {name} (@{username}) — {aid}")
        else:
            lines.append(f"• {name} — {aid}")

    text = "👮 Админы (динамические)\n\n"
    text += "\n".join(lines) if lines else "— пока пусто —"


    kb = [
        [InlineKeyboardButton("➕ Добавить админа", callback_data="admin_admin_add")],
        [InlineKeyboardButton("➖ Удалить админа", callback_data="admin_admin_remove")],
        [InlineKeyboardButton("⬅ Назад", callback_data="admin_back")],
    ]
    await safe_edit_text(q.message, text, InlineKeyboardMarkup(kb))


async def admin_admin_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return
    context.user_data["admin_admin_edit"] = {"mode": "add"}
    await safe_edit_text(q.message, "Введите Telegram ID нового админа (число) одним сообщением:")


async def admin_admin_remove_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return
    context.user_data["admin_admin_edit"] = {"mode": "remove"}
    await safe_edit_text(q.message, "Введите Telegram ID админа для удаления (число) одним сообщением:")

async def admin_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not await guard_admin(q):
        return

    keyboard = [
        [InlineKeyboardButton("👮 Админы", callback_data="admin_admins")],
        [InlineKeyboardButton("👥 Мастера", callback_data="admin_masters")],
        [InlineKeyboardButton("📒 Записи", callback_data="admin_bookings")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton("📦 Бэкап данных", callback_data="admin_backup")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="admin_settings")],
    ]

    # Пытаемся отредактировать текущее сообщение. Если нельзя — отправим новое.
    try:
        await safe_edit_text(q.message, "Админка:", InlineKeyboardMarkup(keyboard))
    except telegram.error.BadRequest:
        await context.bot.send_message(
            chat_id=q.message.chat_id,
            text="Админка:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )


# --- ADMIN: Мастера
async def admin_masters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not await guard_admin(q):
        return

    all_m = get_all_masters()
    keyboard = []
    for mid, m in sorted(all_m.items(), key=lambda x: x[0]):
        name = m.get("name", str(mid))
        status = "✅" if master_enabled(mid) else "🚫"
        keyboard.append([InlineKeyboardButton(f"{status} {name} ({mid})", callback_data=f"admin_master_{mid}")])

    keyboard.append([InlineKeyboardButton("➕ Добавить мастера", callback_data="admin_master_add")])
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="admin_back")])

    await safe_edit_text(q.message, "👥 Мастера (нажмите на мастера):", InlineKeyboardMarkup(keyboard))

async def admin_master_open(update: Update, context: ContextTypes.DEFAULT_TYPE, mid: int | None = None):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not await guard_admin(q):
        return

    if mid is None:
        mid = int(q.data.split("_")[-1])

    m = get_all_masters().get(mid)
    if not m:
        await safe_edit_text(q.message, "Мастер не найден.")
        return

    name = m.get("name", str(mid))
    enabled = master_enabled(mid)

    kb = [
        [InlineKeyboardButton("✅ Включить" if not enabled else "🚫 Выключить", callback_data=f"admin_master_toggle_{mid}")],
        [InlineKeyboardButton("✏️ Переименовать", callback_data=f"admin_master_rename_{mid}")],

        [InlineKeyboardButton("📝 Описание", callback_data=f"admin_master_about_{mid}")],
        [InlineKeyboardButton("📞 Контакты", callback_data=f"admin_master_contacts_{mid}")],
        [InlineKeyboardButton("🗓 График", callback_data=f"admin_master_schedule_{mid}")],
        [InlineKeyboardButton("🗓 Календарь", callback_data=f"admin_mcal_open_{mid}")],

        [InlineKeyboardButton("🗑 Удалить мастера", callback_data=f"admin_master_del_{mid}")],  # ✅

        [InlineKeyboardButton("⬅ К списку", callback_data="admin_masters")],
    ]


    await safe_edit_text(q.message, f"Мастер: {name}\nID: {mid}\nСтатус: {'включён' if enabled else 'выключен'}", InlineKeyboardMarkup(kb))

async def admin_master_del_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    mid = int(q.data.split("_")[-1])

    m = get_all_masters().get(mid)
    name = (m or {}).get("name", str(mid))

    kb = [
        [InlineKeyboardButton("❗ Да, удалить", callback_data=f"admin_master_del_do_{mid}")],
        [InlineKeyboardButton("⬅ Назад", callback_data=f"admin_master_{mid}")],
    ]
    await safe_edit_text(q.message, f"Удалить мастера {name} ({mid})?\n\n⚠️ Все активные записи мастера будут отменены.", InlineKeyboardMarkup(kb))


async def admin_master_del_do(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    mid = int(q.data.split("_")[-1])

    # 1) Отменяем все активные записи мастера (PENDING/CONFIRMED)
    now = datetime.now()
    cancelled_ids = []
    for b in list(bookings):
        if b.get("master_id") != mid:
            continue
        if b.get("status") not in ("PENDING", "CONFIRMED"):
            continue

        # отменяем (и чистим напоминания/чаты)
        bid = b.get("id")
        if isinstance(bid, int):
            cancel_cleanup_for_booking(bid, context)

        b["status"] = "CANCELLED"
        b["cancelled_by"] = "admin"
        b["cancel_reason"] = "Мастер удалён администратором"

        cancelled_ids.append(b.get("id"))

        # уведомим клиента (best-effort)
        try:
            await context.bot.send_message(
                chat_id=b["client_id"],
                text=(
                    "❌ Ваша запись отменена, потому что мастер больше недоступен.\n\n"
                    f"📅 {b.get('date','-')} {b.get('time','-')}\n"
                    f"💅 {b.get('service_name','-')}\n\n"
                    "Пожалуйста, создайте новую запись через /start."
                ),
            )
        except Exception:
            pass

    await save_bookings_locked()

    # 2) Удаляем мастера из хранилищ (без трогания истории — история в bookings уже CANCELLED)
    masters_custom.pop(str(mid), None)
    master_overrides.pop(str(mid), None)

    services_custom.pop(str(mid), None)  # кастом-услуги мастера
    service_overrides.pop(str(mid), None)  # оверрайды услуг мастера

    blocked_slots.pop(str(mid), None)  # блокировки мастера

    # сохраняем всё
    await save_masters_custom_locked()
    await save_master_overrides_locked()
    await save_services_custom_locked()
    await save_service_overrides_locked()
    await save_blocked_locked()

    await safe_edit_text(
        q.message,
        f"✅ Мастер {mid} удалён.\nОтменено активных записей: {len(cancelled_ids)}",
        InlineKeyboardMarkup([[InlineKeyboardButton("⬅ В админку", callback_data="admin_back")]]),
    )

async def admin_master_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not await guard_admin(q):
        return

    mid = int(q.data.split("_")[-1])
    cur = master_overrides.get(str(mid), {})
    new_enabled = not cur.get("enabled", True)

    master_overrides.setdefault(str(mid), {})
    master_overrides[str(mid)]["enabled"] = new_enabled
    await save_master_overrides_locked()

    await admin_master_open(update, context, mid=mid)

async def admin_master_rename(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not await guard_admin(q):
        return

    mid = int(q.data.split("_")[3])
    context.user_data["admin_rename_master"] = {"master_id": mid}
    await safe_edit_text(q.message, f"Введите новое имя для мастера {mid} одним сообщением:")

async def admin_master_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not await guard_admin(q):
        return

    context.user_data["admin_add_master"] = {"step": "id"}
    await safe_edit_text(q.message, "Введите Telegram ID нового мастера (число):")

# --- ADMIN: bookings list
async def admin_bookings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not await guard_admin(q):
        return

    kb = [
        [InlineKeyboardButton("📅 Сегодня", callback_data="admin_bookings_days_0_page_0")],
        [InlineKeyboardButton("🗓 7 дней", callback_data="admin_bookings_days_7_page_0")],
        [InlineKeyboardButton("🗓 30 дней", callback_data="admin_bookings_days_30_page_0")],
        [InlineKeyboardButton("♾ Всё", callback_data="admin_bookings_days_all_page_0")],
        [InlineKeyboardButton("⬅ Назад", callback_data="admin_back")],
    ]
    await safe_edit_text(q.message, "📒 Записи: выберите период", InlineKeyboardMarkup(kb))

async def admin_bookings_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not await guard_admin(q):
        return

    parts = q.data.split("_")
    tail = parts[3]         # 0/7/30/all
    page = int(parts[-1])   # page

    if tail == "all":
        days = None
        period_name = "Всё"
    else:
        days = int(tail)
        period_name = "Сегодня" if days == 0 else f"{days} дней"

    items = filter_bookings_by_days(days)
    total = len(items)

    start_i = page * ADMIN_BOOKINGS_PER_PAGE
    end_i = start_i + ADMIN_BOOKINGS_PER_PAGE
    chunk = items[start_i:end_i]

    keyboard = []
    for b in chunk:
        keyboard.append([InlineKeyboardButton(booking_label(b), callback_data=f"admin_booking_{b['id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀", callback_data=f"admin_bookings_days_{tail}_page_{page-1}"))
    if end_i < total:
        nav.append(InlineKeyboardButton("▶", callback_data=f"admin_bookings_days_{tail}_page_{page+1}"))
    if nav:
        keyboard.append(nav)

    keyboard.append([InlineKeyboardButton("🔄 Период", callback_data="admin_bookings")])
    keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="admin_back")])

    text = f"📒 Записи ({period_name})\nПоказано: {start_i+1 if total else 0}-{min(end_i,total)} из {total}"
    await safe_edit_text(q.message, text, InlineKeyboardMarkup(keyboard))

async def admin_booking_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    if not await guard_admin(q):
        return

    booking_id = int(q.data.split("_")[-1])
    b = get_booking(booking_id)
    if not b:
        await safe_edit_text(q.message, "Запись не найдена.")
        return

    all_m = get_all_masters()
    master_name = all_m.get(b.get("master_id"), {}).get("name", str(b.get("master_id")))

    st = b.get("status")
    status_text = {"PENDING": "⏳ Ожидает", "CONFIRMED": "✅ Подтверждена", "CANCELLED": "❌ Отменена"}.get(st, st)

    # ✅ ДОБАВЬ ВОТ ЭТО (до text)
    rating = b.get("client_rating")
    rated_at = b.get("rated_at")

    if rating is None:
        rating_line = "—"
    else:
        rating_line = f"{rating}⭐️"
        if rated_at:
            rating_line += f" ({rated_at})"

    text = (
        f"📌 Запись #{b.get('id')}\n"
        f"Статус: {status_text}\n\n"
        f"🧑‍🔧 Мастер: {master_name} ({b.get('master_id')})\n"
        f"💅 Услуга: {b.get('service_name','-')}\n"
        f"💰 Цена: {b.get('service_price',0)} ₽\n"
        f"📅 {b.get('date','-')} {b.get('time','-')}\n"
        f"👤 Клиент: {format_client(b)}\n"
        f"⭐ Оценка клиента: {rating_line}\n"
    )

    kb = []
    if st == "PENDING":
        kb.append([InlineKeyboardButton("✅ Подтвердить", callback_data=f"admin_booking_confirm_{booking_id}")])
    if st in ("PENDING", "CONFIRMED"):
        kb.append([InlineKeyboardButton("❌ Отменить", callback_data=f"admin_booking_cancel_{booking_id}")])

    kb.append([InlineKeyboardButton("💬 Написать клиенту", callback_data=f"admin_booking_msg_client_{booking_id}")])
    kb.append([InlineKeyboardButton("💬 Написать мастеру", callback_data=f"admin_booking_msg_master_{booking_id}")])
    kb.append([InlineKeyboardButton("⬅ Назад", callback_data="admin_bookings")])

    await safe_edit_text(q.message, text, InlineKeyboardMarkup(kb))


async def admin_booking_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if not await guard_admin(q):
        return

    bid = int(q.data.split("_")[-1])
    b = get_booking(bid)
    if not b:
        await safe_edit_text(q.message, "Запись не найдена.")
        return
    if b.get("status") != "PENDING":
        await safe_edit_text(q.message, "Нельзя подтвердить: статус уже не PENDING.")
        return

    b["status"] = "CONFIRMED"
    await save_bookings_locked()
    addr = get_master_address(b["master_id"])
    try:
        await context.bot.send_message(
            chat_id=b["client_id"],
            text=(
                "✅ Ваша запись подтверждена администратором!\n\n"
                f"💅 {b.get('service_name','-')}\n"
                f"📅 {b.get('date','-')} {b.get('time','-')}\n"
                f"💰 {b.get('service_price',0)} ₽"
                f"📍 Адрес: {addr}\n"
            ),
        )
    except Exception:
        pass

    try:
        await context.bot.send_message(
            chat_id=b["master_id"],
            text=f"✅ Администратор подтвердил запись #{bid}\n{b.get('date','-')} {b.get('time','-')}\nКлиент: {format_client(b)}",
        )
    except Exception:
        pass

    schedule_reminders_for_booking(context.job_queue, b)
    schedule_followup_for_booking(context.job_queue, b)
    await admin_booking_open(update, context)

async def admin_booking_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if not await guard_admin(q):
        return

    bid = int(q.data.split("_")[-1])
    b = get_booking(bid)
    if not b:
        await safe_edit_text(q.message, "Запись не найдена.")
        return
    if b.get("status") == "CANCELLED":
        await safe_edit_text(q.message, "Запись уже отменена.")
        return

    cancel_cleanup_for_booking(bid, context)
    b["status"] = "CANCELLED"
    b["cancelled_by"] = "admin"
    b["cancel_reason"] = "Отмена администратором"
    await save_bookings_locked()

    try:
        await context.bot.send_message(chat_id=b["client_id"], text=f"❌ Запись #{bid} отменена администратором.")
    except Exception:
        pass
    try:
        await context.bot.send_message(chat_id=b["master_id"], text=f"❌ Запись #{bid} отменена администратором.")
    except Exception:
        pass

    await admin_booking_open(update, context)

async def admin_booking_msg_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not await guard_admin(q):
        return
    bid = int(q.data.split("_")[-1])
    context.user_data["admin_msg"] = {"booking_id": bid, "target": "client"}
    await safe_edit_text(q.message, "Введите сообщение для клиента одним сообщением:")

async def admin_booking_msg_master(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not await guard_admin(q):
        return
    bid = int(q.data.split("_")[-1])
    context.user_data["admin_msg"] = {"booking_id": bid, "target": "master"}
    await safe_edit_text(q.message, "Введите сообщение для мастера одним сообщением:")

# --- ADMIN: статистика
def calc_stats(days: int | None):
    now = datetime.now()
    items = []
    for b in bookings:
        dt = parse_booking_dt(b)
        if not dt:
            continue
        if days is None:
            items.append((b, dt))
        else:
            if (now - timedelta(days=days)) <= dt <= (now + timedelta(days=days)):
                items.append((b, dt))

    total = len(items)
    pending = sum(1 for b, _ in items if b.get("status") == "PENDING")
    confirmed = sum(1 for b, _ in items if b.get("status") == "CONFIRMED")
    cancelled = sum(1 for b, _ in items if b.get("status") == "CANCELLED")
    upcoming_confirmed = sum(1 for b, dt in items if b.get("status") == "CONFIRMED" and dt >= now)
    revenue = sum(int(b.get("service_price") or 0) for b, _ in items if b.get("status") == "CONFIRMED")

    by_master = {}
    for b, _ in items:
        mid = b.get("master_id")
        if not mid:
            continue
        by_master.setdefault(mid, {"confirmed": 0, "pending": 0, "cancelled": 0, "revenue": 0})
        st = b.get("status")
        if st == "CONFIRMED":
            by_master[mid]["confirmed"] += 1
            by_master[mid]["revenue"] += int(b.get("service_price") or 0)
        elif st == "PENDING":
            by_master[mid]["pending"] += 1
        elif st == "CANCELLED":
            by_master[mid]["cancelled"] += 1

    return {
        "total": total,
        "pending": pending,
        "confirmed": confirmed,
        "cancelled": cancelled,
        "upcoming_confirmed": upcoming_confirmed,
        "revenue": revenue,
        "by_master": by_master,
    }

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    kb = [
        [InlineKeyboardButton("📅 Сегодня", callback_data="admin_stats_days_0")],
        [InlineKeyboardButton("🗓 7 дней", callback_data="admin_stats_days_7")],
        [InlineKeyboardButton("🗓 30 дней", callback_data="admin_stats_days_30")],
        [InlineKeyboardButton("♾ Всё", callback_data="admin_stats_days_all")],
        [InlineKeyboardButton("⬅ Назад", callback_data="admin_back")],
    ]
    await safe_edit_text(q.message, "📊 Статистика: выберите период", InlineKeyboardMarkup(kb))

async def admin_stats_show(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    tail = q.data.split("_")[-1]
    if tail == "all":
        days = None
        period_name = "Всё"
    else:
        days = int(tail)
        period_name = "Сегодня" if days == 0 else f"{days} дней"

    s = calc_stats(days)
    all_m = get_all_masters()

    top = sorted(s["by_master"].items(), key=lambda x: (x[1]["confirmed"], x[1]["revenue"]), reverse=True)[:3]
    top_lines = []
    for mid, v in top:
        name = all_m.get(mid, {}).get("name", str(mid))
        top_lines.append(f"• {name} ({mid}) — ✅{v['confirmed']} / ⏳{v['pending']} / ❌{v['cancelled']} / 💰{v['revenue']}₽")

    top_text = "\n".join(top_lines) if top_lines else "—"

    text = (
        f"📊 Статистика ({period_name})\n\n"
        f"Всего записей: {s['total']}\n"
        f"⏳ Ожидают: {s['pending']}\n"
        f"✅ Подтверждены: {s['confirmed']}\n"
        f"❌ Отменены: {s['cancelled']}\n"
        f"🔜 Будущие подтверждённые: {s['upcoming_confirmed']}\n"
        f"💰 Выручка (по подтверждённым): {s['revenue']} ₽\n\n"
        f"🏆 Топ мастера:\n{top_text}"
    )

    kb = [
        [InlineKeyboardButton("🔄 Выбрать период", callback_data="admin_stats")],
        [InlineKeyboardButton("⬅ Назад", callback_data="admin_back")],
    ]
    await safe_edit_text(q.message, text, InlineKeyboardMarkup(kb))

# --- ADMIN: настройки/напоминания
def fmt_rem_cfg(cfg: dict) -> str:
    if not isinstance(cfg, dict):
        return str(cfg)
    if "minutes" in cfg:
        return f"{cfg['minutes']} мин"
    if "hours" in cfg:
        return f"{cfg['hours']} ч"
    return str(cfg)

async def admin_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    kb = [
        [InlineKeyboardButton("🔔 Напоминания", callback_data="admin_settings_reminders")],
        [InlineKeyboardButton("⭐ Отзывы после визита", callback_data="admin_settings_followup")],
        [InlineKeyboardButton("⬅ Назад", callback_data="admin_back")],
    ]

    await safe_edit_text(q.message, "⚙️ Настройки:", InlineKeyboardMarkup(kb))

async def admin_settings_reminders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    rcfg = get_reminders_cfg()
    text = (
        "🔔 Напоминания\n\n"
        f"Клиент: {fmt_rem_cfg(rcfg['client'])}\n"
        f"Мастер: {fmt_rem_cfg(rcfg['master'])}\n\n"
        "Выберите что менять:"
    )

    kb = [
        [InlineKeyboardButton("👤 Клиент", callback_data="admin_set_rem_client")],
        [InlineKeyboardButton("🧑‍🔧 Мастер", callback_data="admin_set_rem_master")],
        [InlineKeyboardButton("⬅ Назад", callback_data="admin_settings")],
    ]
    await safe_edit_text(q.message, text, InlineKeyboardMarkup(kb))

def _cut(s: str, n: int = 120) -> str:
    s = (s or "").strip()
    if not s:
        return "—"
    return s if len(s) <= n else s[:n] + "…"

async def admin_settings_followup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    cfg = get_followup_cfg()
    status = "✅ включено" if cfg["enabled"] else "🚫 выключено"

    text = (
        "⭐ Отзывы после визита\n\n"
        f"Статус: {status}\n"
        f"Через: {cfg['after_hours']} ч после окончания услуги\n"
        f"2ГИС: {cfg['two_gis_url'] or '—'}\n\n"
        f"Текст запроса оценки:\n{_cut(cfg['ask_text'])}\n\n"
        f"Текст после оценки:\n{_cut(cfg['thanks_text'])}\n\n"
        "Что меняем?"
    )

    kb = [
        [InlineKeyboardButton("✅ Включить" if not cfg["enabled"] else "🚫 Выключить", callback_data="admin_followup_toggle")],
        [InlineKeyboardButton("⏱ Через N часов", callback_data="admin_followup_set_hours")],
        [InlineKeyboardButton("🔗 Ссылка 2ГИС", callback_data="admin_followup_set_2gis")],
        [InlineKeyboardButton("📝 Текст запроса оценки", callback_data="admin_followup_set_ask")],
        [InlineKeyboardButton("💬 Текст после оценки", callback_data="admin_followup_set_thanks")],
        [InlineKeyboardButton("⬅ Назад", callback_data="admin_settings")],
    ]
    await safe_edit_text(q.message, text, InlineKeyboardMarkup(kb))


async def admin_followup_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    ADMIN_SETTINGS.setdefault("followup", {})
    cur = bool(ADMIN_SETTINGS["followup"].get("enabled", True))
    ADMIN_SETTINGS["followup"]["enabled"] = not cur
    await save_admin_settings_locked()
    reschedule_all_followups(context.job_queue)
    await admin_settings_followup(update, context)

async def admin_followup_set_hours(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return
    context.user_data["admin_followup_edit"] = {"field": "after_hours"}
    await safe_edit_text(q.message, "Введите число часов после окончания услуги (например 12):")


async def admin_followup_set_2gis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return
    context.user_data["admin_followup_edit"] = {"field": "two_gis_url"}
    await safe_edit_text(q.message, "Отправьте ссылку 2ГИС одним сообщением (или '-' чтобы очистить):")


async def admin_followup_set_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return
    context.user_data["admin_followup_edit"] = {"field": "ask_text"}
    await safe_edit_text(q.message, "Введите текст запроса оценки одним сообщением.\nМожно использовать {name}.")


async def admin_followup_set_thanks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return
    context.user_data["admin_followup_edit"] = {"field": "thanks_text"}
    await safe_edit_text(q.message, "Введите текст после оценки одним сообщением.\nМожно использовать {rating}.")

async def admin_set_rem_client(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not await guard_admin(q):
        return
    context.user_data["admin_set_rem"] = {"target": "client"}
    await safe_edit_text(q.message, "Введите за сколько минут напоминать КЛИЕНТУ (например 1440 = 24 часа):")

async def admin_set_rem_master(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not await guard_admin(q):
        return
    context.user_data["admin_set_rem"] = {"target": "master"}
    await safe_edit_text(q.message, "Введите за сколько минут напоминать МАСТЕРУ (например 120 = 2 часа):")

# ---------------------------
# MASTER PROFILE (мастер сам)
# ---------------------------
async def master_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return

    mid = q.from_user.id

    # 1) СНАЧАЛА проверка доступа, и только ОДИН ответ на callback
    if not is_master(mid):
        try:
            await q.answer("Нет доступа", show_alert=True)
        except BadRequest:
            pass
        return

    try:
        await q.answer()  # просто снять "часики"
    except BadRequest:
        pass

    # 2) Если master_card_text может быть тяжёлым (БД/файлы) — уводим в поток
    text = await asyncio.to_thread(master_card_text, mid)

    kb = [
        [InlineKeyboardButton("📝 Изменить описание", callback_data="m_edit_about")],
        [InlineKeyboardButton("📞 Контакты", callback_data="m_contacts_menu")],
        [InlineKeyboardButton("🖼 Фото", callback_data="m_set_photo")],          # ✅ добавили
        [InlineKeyboardButton("🗓 Изменить график", callback_data="m_edit_schedule")],
        [InlineKeyboardButton("⬅ Назад", callback_data="back_to_master")],
    ]

    await safe_edit_text(q.message, text, InlineKeyboardMarkup(kb))

async def m_set_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    mid = q.from_user.id
    if not is_master(mid):
        return

    # ставим "режим ожидания фото"
    context.user_data["profile_photo_edit"] = {"mid": mid}

    await safe_edit_text(
        q.message,
        "Отправьте ОДНО фото (картинку) следующим сообщением.\n\n"
        "Чтобы удалить фото — отправьте текст: -"
    )


async def m_receive_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ловим фото от мастера, сохраняем file_id"""
    user_id = update.effective_user.id

    st = context.user_data.get("profile_photo_edit")
    if not st:
        return  # фото не ждали

    mid = st.get("mid")
    if not mid or user_id != mid:
        return

    # вариант: мастер хочет удалить фото текстом "-"
    if update.message and update.message.text and update.message.text.strip() == "-":
        ensure_master_profile(mid)
        masters_custom[str(mid)]["photo_file_id"] = ""
        await save_masters_custom_locked()

        context.user_data.pop("profile_photo_edit", None)
        await update.message.reply_text("✅ Фото удалено.")
        return

    # если пришло фото
    if not update.message or not update.message.photo:
        await update.message.reply_text("⚠️ Это не фото. Отправьте картинку (или '-' чтобы удалить фото).")
        return

    # берём самое большое по размеру
    photo = update.message.photo[-1]
    file_id = photo.file_id

    ensure_master_profile(mid)
    masters_custom[str(mid)]["photo_file_id"] = file_id
    await save_masters_custom_locked()

    context.user_data.pop("profile_photo_edit", None)
    await update.message.reply_text("✅ Фото сохранено. Клиенты будут видеть его при выборе мастера.")

async def m_contacts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q: return
    await q.answer()

    mid = q.from_user.id
    if not is_master(mid):
        return

    kb = [
        [InlineKeyboardButton("📞 Телефон", callback_data="m_edit_contact_phone")],
        [InlineKeyboardButton("📷 Instagram", callback_data="m_edit_contact_instagram")],
        [InlineKeyboardButton("✈️ Telegram", callback_data="m_edit_contact_telegram")],
        [InlineKeyboardButton("📍 Адрес", callback_data="m_edit_contact_address")],
        [InlineKeyboardButton("⬅ Назад", callback_data="master_profile")],
    ]
    await safe_edit_text(q.message, "Что изменить в контактах?", InlineKeyboardMarkup(kb))

async def m_edit_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q: return
    await q.answer()
    mid = q.from_user.id
    if not is_master(mid):
        return

    field = q.data.replace("m_edit_contact_", "", 1)  # phone/instagram/telegram/address
    context.user_data["profile_edit"] = {"scope": "master", "mid": mid, "field": f"contact_{field}"}
    await safe_edit_text(q.message, "Введите новое значение (или '-' чтобы очистить):")

async def m_edit_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    mid = q.from_user.id
    if not is_master(mid):
        return
    context.user_data["profile_edit"] = {"scope": "master", "mid": mid, "field": "about"}
    await safe_edit_text(q.message, "Введите описание мастера одним сообщением (или '-' чтобы очистить):")


async def m_edit_contacts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    mid = q.from_user.id
    if not is_master(mid):
        return
    context.user_data["profile_edit"] = {"scope": "master", "mid": mid, "field": "contacts", "step": "phone"}
    await safe_edit_text(q.message, "Введите телефон (или '-' пропустить):")


# ---------------------------
# ADMIN PROFILE EDIT
# ---------------------------
async def admin_master_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    mid = int(q.data.split("_")[-1])
    context.user_data["profile_edit"] = {"scope": "admin", "mid": mid, "field": "about"}
    await safe_edit_text(q.message, f"Введите описание мастера {mid} одним сообщением (или '-' чтобы очистить):")


async def admin_master_contacts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    mid = int(q.data.split("_")[-1])
    context.user_data["profile_edit"] = {"scope": "admin", "mid": mid, "field": "contacts", "step": "phone"}
    await safe_edit_text(q.message, f"Введите телефон мастера {mid} (или '-' пропустить):")

def _schedule_kb(mid: int, days: list[int], scope: str):
    # scope: "admin" / "master"
    pref = "a" if scope == "admin" else "m"

    # toggles
    row = []
    rows = []
    for i, wd in enumerate(WEEKDAYS):
        mark = "✅" if i in days else "▫️"
        row.append(InlineKeyboardButton(f"{mark} {wd}", callback_data=f"{pref}_sch_tgl_{mid}_{i}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    # nav
    rows.append([
        InlineKeyboardButton("➡ Далее", callback_data=f"{pref}_sch_next_{mid}"),
        InlineKeyboardButton("❌ Отмена", callback_data=f"{pref}_sch_cancel_{mid}"),
    ])
    return InlineKeyboardMarkup(rows)

async def _schedule_show_days(message, context: ContextTypes.DEFAULT_TYPE):
    st = context.user_data.get("sch_edit")
    if not st:
        return
    mid = st["mid"]
    days = st.get("days", [])
    await safe_edit_text(message, f"Выберите рабочие дни мастера {mid}:", _schedule_kb(mid, days, st["scope"]))

async def _schedule_start(scope: str, mid: int, context: ContextTypes.DEFAULT_TYPE, message):
    ensure_master_profile(mid)
    sch = masters_custom[str(mid)].get("schedule", {})
    cur_days = sch.get("days") or []
    context.user_data["sch_edit"] = {"scope": scope, "mid": mid, "step": "days", "days": list(cur_days)}
    await _schedule_show_days(message, context)


async def admin_master_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return
    mid = int(q.data.split("_")[-1])
    await _schedule_start("admin", mid, context, q.message)


async def m_edit_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    mid = q.from_user.id
    if not is_master(mid):
        return
    await _schedule_start("master", mid, context, q.message)


async def sch_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    parts = q.data.split("_")  # a/m, sch, tgl, mid, day
    scope = "admin" if parts[0] == "a" else "master"
    mid = int(parts[3])
    day = int(parts[4])

    # права
    if scope == "admin":
        if not await guard_admin(q):
            return
    else:
        if q.from_user.id != mid:
            await q.answer("Нет доступа", show_alert=True)
            return

    st = context.user_data.get("sch_edit")
    if not st or st.get("mid") != mid:
        context.user_data["sch_edit"] = {"scope": scope, "mid": mid, "step": "days", "days": []}
        st = context.user_data["sch_edit"]

    days = st.get("days", [])
    if day in days:
        days.remove(day)
    else:
        days.append(day)
        days.sort()
    st["days"] = days

    await _schedule_show_days(q.message, context)


async def sch_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()

    parts = q.data.split("_")  # a/m, sch, next, mid
    scope = "admin" if parts[0] == "a" else "master"
    mid = int(parts[3])

    if scope == "admin":
        if not await guard_admin(q):
            return
    else:
        if q.from_user.id != mid:
            await q.answer("Нет доступа", show_alert=True)
            return

    st = context.user_data.get("sch_edit")
    if not st or st.get("mid") != mid:
        await q.answer("Сеанс устарел", show_alert=True)
        return

    if not st.get("days"):
        await q.answer("Выберите хотя бы 1 день", show_alert=True)
        return

    st["step"] = "start"
    await safe_edit_text(q.message, "Введите время начала в формате HH:MM (например 10:00):")


async def sch_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    parts = q.data.split("_")  # a/m, sch, cancel, mid
    scope = "admin" if parts[0] == "a" else "master"
    mid = int(parts[3])

    context.user_data.pop("sch_edit", None)

    # вернём в меню
    if scope == "admin":
        # открываем карточку мастера как было
        await admin_master_open(update, context, mid=mid)
    else:
        await master_profile(update, context)

# -----------------------------------------------------------------------------
# BACK (клиентский визард назад) — исправлено: показывает всех мастеров
# -----------------------------------------------------------------------------
async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = q.from_user.id
    ctx = user_context.get(user_id)
    if not ctx or "state" not in ctx:
        await safe_edit_text(q.message, "Сеанс устарел. Введите /start")
        return

    current_state = ctx["state"]
    if current_state not in BACK_MAPPING:
        return

    prev_state = BACK_MAPPING[current_state]
    ctx["state"] = prev_state

    if prev_state == States.MASTER:
        all_m = get_all_masters()
        keyboard = [
            [InlineKeyboardButton(str(m.get("name", mid)), callback_data=f"master_{mid}")]
            for mid, m in all_m.items()
            if master_enabled(mid)
        ]
        await safe_edit_text(q.message, "Выберите мастера:", InlineKeyboardMarkup(keyboard))
        return

    if prev_state == States.SERVICE:
        master_id = ctx.get("master_id")
        if not master_id:
            clear_user_context(user_id)
            await safe_edit_text(q.message, "Контекст утерян. Введите /start")
            return

        ctx["state"] = States.SERVICE
        ctx.setdefault("service_ids", [])  # не теряем список

        await show_services_step(q.message, ctx)
        return


    if prev_state == States.DATE:
        ctx["day_offset"] = ctx.get("day_offset", 0)
        await show_date_step(q.message, ctx)
        return

    if prev_state == States.TIME:
        date = ctx.get("date")
        master_id = ctx.get("master_id")
        service_id = ctx.get("service_id")
        if not all([date, master_id, service_id]):
            clear_user_context(user_id)
            await safe_edit_text(q.message, "Контекст утерян. Введите /start")
            return

        slots = get_available_slots(master_id, date, service_id)
        keyboard = [[InlineKeyboardButton(t, callback_data=f"time_{t}")] for t in slots]
        keyboard.append([InlineKeyboardButton("⬅ Назад", callback_data="back")])
        await safe_edit_text(q.message, f"Дата: {date}\nВыберите время:", InlineKeyboardMarkup(keyboard))
        return

# -----------------------------------------------------------------------------
# TEXT ROUTER: всё, что приходит текстом (admin modes + мастера + чат)
# -----------------------------------------------------------------------------
async def relay_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = (update.message.text or "").strip()

    # --------------------------------------------
    # Сбор контактов для клиентов без @username
    # --------------------------------------------
    pb = context.user_data.get("pending_booking")
    step = context.user_data.get("pending_booking_step")

    if isinstance(pb, dict) and step == "phone":
        t = (text or "").strip()
        if t.lower() == "отмена":
            context.user_data.pop("pending_booking", None)
            context.user_data.pop("pending_booking_step", None)
            await update.message.reply_text("Ок, отменил. Можете начать запись заново.", reply_markup=ReplyKeyboardRemove())
            return

        phone = _normalize_phone(t)
        # минимальная проверка: 10+ цифр
        digits = "".join(ch for ch in phone if ch.isdigit())
        if len(digits) < 10:
            await update.message.reply_text("Похоже, это не номер телефона. Отправьте номер ещё раз или нажмите «Отмена».")
            return

        pb["client_phone"] = phone
        context.user_data["pending_booking"] = pb
        context.user_data["pending_booking_step"] = "name"

        await update.message.reply_text("Теперь напишите, пожалуйста, как к вам обращаться (имя):", reply_markup=ReplyKeyboardRemove())
        return

    if isinstance(pb, dict) and step == "name":
        t = (text or "").strip()
        if not t or len(t) < 2:
            await update.message.reply_text("Введите имя (минимум 2 символа) или «Отмена».")
            return
        if t.lower() == "отмена":
            context.user_data.pop("pending_booking", None)
            context.user_data.pop("pending_booking_step", None)
            await update.message.reply_text("Ок, отменил. Можете начать запись заново.", reply_markup=ReplyKeyboardRemove())
            return

        pb["client_contact_name"] = t
        context.user_data["pending_booking"] = pb

        await _finalize_pending_booking(update, context)
        return
    st = context.user_data.get("admin_admin_edit")
    if st and is_admin(user_id):
        if not text.isdigit():
            await update.message.reply_text("Нужно число (Telegram ID). Попробуйте ещё раз.")
            return

        target_id = int(text)
        mode = st.get("mode")

        ADMIN_SETTINGS.setdefault("admins", [])
        admins_list = ADMIN_SETTINGS["admins"]
        if not isinstance(admins_list, list):
            admins_list = []
            ADMIN_SETTINGS["admins"] = admins_list

        if mode == "add":
            if target_id in ADMIN_IDS:
                await update.message.reply_text("Этот ID уже в базовых админах (config).")
            elif target_id in get_dynamic_admin_ids():
                await update.message.reply_text("Этот ID уже добавлен.")
            else:
                admins_list.append(target_id)

                # ✅ сохраняем профиль (имя/@username) в admin_settings.json
                ADMIN_SETTINGS.setdefault("admin_profiles", {})
                try:
                    chat = await context.bot.get_chat(target_id)
                    ADMIN_SETTINGS["admin_profiles"][str(target_id)] = {
                        "name": chat.full_name or str(target_id),
                        "username": chat.username or "",
                    }
                except Exception:
                    ADMIN_SETTINGS["admin_profiles"][str(target_id)] = {
                        "name": str(target_id),
                        "username": "",
                    }

                await save_admin_settings_locked()
                p = ADMIN_SETTINGS["admin_profiles"][str(target_id)]
                shown = f"{p['name']}" + (f" (@{p['username']})" if p.get("username") else "")
                await update.message.reply_text(f"✅ Админ добавлен: {shown} — {target_id}")


        elif mode == "remove":
            before = set(get_dynamic_admin_ids())
            admins_list[:] = [x for x in admins_list if str(x) != str(target_id)]
            ADMIN_SETTINGS.setdefault("admin_profiles", {})
            ADMIN_SETTINGS["admin_profiles"].pop(str(target_id), None)

            await save_admin_settings_locked()
            after = set(get_dynamic_admin_ids())
            if target_id in before and target_id not in after:
                await update.message.reply_text(f"✅ Админ удалён: {target_id}")
            else:
                await update.message.reply_text("Этого админа нет в динамическом списке.")

        context.user_data.pop("admin_admin_edit", None)
        await update.message.reply_text("Откройте /admin → 👮 Админы")
        return

    # 0) Редактор графика (admin/master) — текстовые шаги start/end/limit
    st_sch = context.user_data.get("sch_edit")
    if st_sch:
        mid = st_sch["mid"]
        scope = st_sch["scope"]

        # права
        if scope == "admin":
            if not is_admin(user_id):
                context.user_data.pop("sch_edit", None)
                return
        else:
            if user_id != mid:
                context.user_data.pop("sch_edit", None)
                return

        step = st_sch.get("step")

        if step == "start":
            if text == "-":
                await update.message.reply_text("Нельзя пропустить. Введите HH:MM (например 10:00).")
                return
            if not _is_hhmm(text):
                await update.message.reply_text("Неверный формат. Введите HH:MM (например 10:00).")
                return
            st_sch["start"] = text.strip()
            st_sch["step"] = "end"
            await update.message.reply_text("Теперь введите время окончания HH:MM (например 18:00):")
            return

        if step == "end":
            if text == "-":
                await update.message.reply_text("Нельзя пропустить. Введите HH:MM (например 18:00).")
                return
            if not _is_hhmm(text):
                await update.message.reply_text("Неверный формат. Введите HH:MM (например 18:00).")
                return
            st_sch["end"] = text.strip()
            st_sch["step"] = "limit"
            await update.message.reply_text("Введите лимит записей в день (0 = без лимита):")
            return

        if step == "limit":
            if not text.isdigit():
                await update.message.reply_text("Нужно число. Например 0 или 6.")
                return
            limit = int(text)
            if limit < 0:
                await update.message.reply_text("Лимит не может быть отрицательным.")
                return

            ensure_master_profile(mid)
            masters_custom[str(mid)]["schedule"] = {
                "days": st_sch.get("days", []),
                "start": st_sch.get("start", ""),
                "end": st_sch.get("end", ""),
                "daily_limit": limit,
            }
            await save_masters_custom_locked()

            context.user_data.pop("sch_edit", None)
            await update.message.reply_text("✅ График сохранён.")

            # вернуть в меню
            if scope == "admin":
                # можно просто показать карточку мастера
                fake = update  # не обязательно
                # чтобы не усложнять — подсказываем куда нажать:
                await update.message.reply_text("Откройте /admin → 👥 Мастера → выберите мастера.")
            else:
                await update.message.reply_text("Откройте /master → 👤 Мой профиль.")
            return

    # 0.1) Редактор описания/контактов (admin/master)
    st_prof = context.user_data.get("profile_edit")
    if st_prof:
        scope = st_prof["scope"]
        mid = st_prof["mid"]

        if scope == "admin" and not is_admin(user_id):
            context.user_data.pop("profile_edit", None)
            return
        if scope == "master" and user_id != mid:
            context.user_data.pop("profile_edit", None)
            return

        field = st_prof.get("field", "")

        # --- точечное редактирование контакта: field = contact_phone/contact_instagram/contact_telegram/contact_address
        if field.startswith("contact_"):
            key = field.replace("contact_", "", 1)

            if key not in ("phone", "instagram", "telegram", "address"):
                context.user_data.pop("profile_edit", None)
                await update.message.reply_text("⚠️ Неизвестное поле контакта.")
                return

            ensure_master_profile(mid)
            c = masters_custom[str(mid)].get("contacts", {})
            if not isinstance(c, dict):
                c = {"phone": "", "instagram": "", "address": "", "telegram": ""}

            value = (text or "").strip()
            c[key] = "" if value == "-" else value

            masters_custom[str(mid)]["contacts"] = c
            await save_masters_custom_locked()

            context.user_data.pop("profile_edit", None)
            await update.message.reply_text("✅ Контакт обновлён.")
            await update.message.reply


        ensure_master_profile(mid)

        if st_prof["field"] == "about":
            masters_custom[str(mid)]["about"] = "" if text.strip() == "-" else text.strip()
            await save_masters_custom_locked()
            context.user_data.pop("profile_edit", None)
            await update.message.reply_text("✅ Описание сохранено.")
            return

        if st_prof["field"] == "contacts":
            step = st_prof.get("step", "phone")
            c = masters_custom[str(mid)].get("contacts", {})
            if not isinstance(c, dict):
                c = {"phone": "", "instagram": "", "address": "", "telegram": ""}

            val = "" if text.strip() == "-" else text.strip()

            if step == "phone":
                c["phone"] = val
                st_prof["step"] = "instagram"
                masters_custom[str(mid)]["contacts"] = c
                await save_masters_custom_locked()
                await update.message.reply_text("Введите Instagram (или '-' пропустить):")
                return

            if step == "instagram":
                c["instagram"] = val
                st_prof["step"] = "telegram"
                masters_custom[str(mid)]["contacts"] = c
                await save_masters_custom_locked()
                await update.message.reply_text("Введите Telegram @username (или '-' пропустить):")
                return

            if step == "telegram":
                c["telegram"] = val
                st_prof["step"] = "address"
                masters_custom[str(mid)]["contacts"] = c
                await save_masters_custom_locked()
                await update.message.reply_text("Введите адрес (или '-' пропустить):")
                return

            if step == "address":
                c["address"] = val
                masters_custom[str(mid)]["contacts"] = c
                await save_masters_custom_locked()
                context.user_data.pop("profile_edit", None)
                await update.message.reply_text("✅ Контакты сохранены.")
                return


    # 1) ADMIN: отправка сообщения клиенту/мастеру по записи
    st_msg = context.user_data.get("admin_msg")
    if st_msg and is_admin(user_id):
        bid = st_msg["booking_id"]
        target = st_msg["target"]

        b = get_booking(bid)
        if not b:
            context.user_data.pop("admin_msg", None)
            await update.message.reply_text("Запись не найдена.")
            return

        chat_id = b["client_id"] if target == "client" else b["master_id"]
        prefix = "💬 Сообщение от администратора:\n\n"

        try:
            await context.bot.send_message(chat_id=chat_id, text=prefix + text)
        except Exception:
            await update.message.reply_text("Не удалось отправить сообщение (возможно пользователь не писал боту).")
            return

        context.user_data.pop("admin_msg", None)
        await update.message.reply_text("✅ Отправлено.")
        return

    # 2) ADMIN: добавление мастера
    # 2) ADMIN: добавление мастера (полный визард)
    st_add = context.user_data.get("admin_add_master")
    if st_add and is_admin(user_id):
        step = st_add.get("step")

        if step == "id":
            if not text.isdigit():
                await update.message.reply_text("Нужен числовой Telegram ID. Введите ещё раз.")
                return
            mid = int(text)

            # защита от дублей
            if str(mid) in masters_custom:
                await update.message.reply_text("Такой мастер уже есть. Введите другой ID или удалите старого.")
                return

            st_add["master_id"] = mid
            st_add["step"] = "name"
            await update.message.reply_text("Введите имя мастера (например: Анна).")
            return

        if step == "name":
            if len(text) < 2:
                await update.message.reply_text("Имя слишком короткое. Введите ещё раз.")
                return
            st_add["name"] = text.strip()
            st_add["step"] = "about"
            await update.message.reply_text("Введите описание мастера (или '-' чтобы пропустить):")
            return

        if step == "about":
            st_add["about"] = "" if text.strip() == "-" else text.strip()
            st_add["step"] = "phone"
            await update.message.reply_text("Введите телефон (или '-' пропустить):")
            return

        if step == "phone":
            st_add["phone"] = "" if text.strip() == "-" else text.strip()
            st_add["step"] = "instagram"
            await update.message.reply_text("Введите Instagram (или '-' пропустить):")
            return

        if step == "instagram":
            st_add["instagram"] = "" if text.strip() == "-" else text.strip()
            st_add["step"] = "telegram"
            await update.message.reply_text("Введите Telegram @username (или '-' пропустить):")
            return

        if step == "telegram":
            st_add["telegram"] = "" if text.strip() == "-" else text.strip()
            st_add["step"] = "address"
            await update.message.reply_text("Введите адрес (или '-' пропустить):")
            return

        if step == "address":
            st_add["address"] = "" if text.strip() == "-" else text.strip()

            # переходим к выбору дней (inline)
            st_add["step"] = "schedule_days"
            st_add["schedule_days"] = []
            await update.message.reply_text("Теперь настроим график. Откройте меню выбора дней в админке (кнопки ниже).")

            # если админ добавлял через inline-кнопку — удобнее редактировать то же сообщение,
            # но мы в text handler, поэтому просто отправляем отдельным сообщением:
            await update.message.reply_text("Выберите рабочие дни мастера:", reply_markup=_add_master_days_kb([]))
            return

        if step == "schedule_start":
            if not _is_hhmm(text):
                await update.message.reply_text("Неверный формат. Введите HH:MM (например 10:00).")
                return
            st_add["schedule_start"] = text.strip()
            st_add["step"] = "schedule_end"
            await update.message.reply_text("Введите время окончания HH:MM (например 18:00):")
            return

        if step == "schedule_end":
            if not _is_hhmm(text):
                await update.message.reply_text("Неверный формат. Введите HH:MM (например 18:00).")
                return
            st_add["schedule_end"] = text.strip()
            st_add["step"] = "daily_limit"
            await update.message.reply_text("Введите лимит записей в день (0 = без лимита):")
            return

        if step == "daily_limit":
            if not text.isdigit():
                await update.message.reply_text("Нужно число. Например 0 или 6.")
                return
            limit = int(text)
            if limit < 0:
                await update.message.reply_text("Лимит не может быть отрицательным.")
                return

            mid = st_add["master_id"]

            # сохраняем мастера целиком
            masters_custom[str(mid)] = {
                "name": st_add.get("name", str(mid)),
                "services": [],
                "about": st_add.get("about", ""),
                "contacts": {
                    "phone": st_add.get("phone", ""),
                    "instagram": st_add.get("instagram", ""),
                    "telegram": st_add.get("telegram", ""),
                    "address": st_add.get("address", ""),
                },
                "schedule": {
                    "days": st_add.get("schedule_days", []),
                    "start": st_add.get("schedule_start", ""),
                    "end": st_add.get("schedule_end", ""),
                    "daily_limit": limit,
                },
            }
            await save_masters_custom_locked()

            # включаем мастера
            master_overrides.setdefault(str(mid), {})
            master_overrides[str(mid)]["enabled"] = True
            await save_master_overrides_locked()

            context.user_data.pop("admin_add_master", None)
            await update.message.reply_text(f"✅ Мастер добавлен: {masters_custom[str(mid)]['name']} ({mid}).")
            await update.message.reply_text("Откройте /admin → 👥 Мастера")
            return

        # Если дошли сюда — значит шаг не тот (например, ждём inline-выбор дней)
        return


    # 3) ADMIN: переименование мастера
    st_ren = context.user_data.get("admin_rename_master")
    if st_ren and is_admin(user_id):
        new_name = text
        if len(new_name) < 2:
            await update.message.reply_text("Имя слишком короткое. Введите ещё раз.")
            return

        mid = st_ren["master_id"]
        master_overrides.setdefault(str(mid), {})
        master_overrides[str(mid)]["name"] = new_name
        await save_master_overrides_locked()

        context.user_data.pop("admin_rename_master", None)
        await update.message.reply_text(f"✅ Переименовано. Теперь мастер {mid}: {new_name}. Откройте /admin → 👥 Мастера")
        return

    # 4) ADMIN: изменение напоминаний
    st_rem = context.user_data.get("admin_set_rem")
    if st_rem and is_admin(user_id):
        if not text.isdigit():
            await update.message.reply_text("Введите число минут (например 60 или 1440).")
            return
        minutes = int(text)
        if minutes <= 0:
            await update.message.reply_text("Минуты должны быть > 0.")
            return

        who = st_rem["target"]  # client/master
        ADMIN_SETTINGS.setdefault("reminders", {})
        ADMIN_SETTINGS["reminders"][who] = {"minutes": minutes}
        await save_admin_settings_locked()

        context.user_data.pop("admin_set_rem", None)
        await update.message.reply_text("✅ Сохранено. Откройте /admin → ⚙️ Настройки → 🔔 Напоминания")
        return
    # 4.5) ADMIN: followup (отзывы) настройки
    st_fu = context.user_data.get("admin_followup_edit")
    if st_fu and is_admin(user_id):
        field = st_fu.get("field")
        val = text.strip()

        ADMIN_SETTINGS.setdefault("followup", {})

        if field == "after_hours":
            if not val.isdigit():
                await update.message.reply_text("Нужно число часов (например 12).")
                return
            hours = int(val)
            if hours < 0:
                await update.message.reply_text("Часы не могут быть отрицательными.")
                return
            ADMIN_SETTINGS["followup"]["after_hours"] = hours

        elif field == "two_gis_url":
            if val == "-":
                ADMIN_SETTINGS["followup"]["two_gis_url"] = ""
            else:
                ADMIN_SETTINGS["followup"]["two_gis_url"] = val

        elif field == "ask_text":
            ADMIN_SETTINGS["followup"]["ask_text"] = val

        elif field == "thanks_text":
            ADMIN_SETTINGS["followup"]["thanks_text"] = val

        await save_admin_settings_locked()
        reschedule_all_followups(context.job_queue)
        context.user_data.pop("admin_followup_edit", None)
        await update.message.reply_text("✅ Сохранено. Откройте /admin → ⚙️ Настройки → ⭐ Отзывы после визита")
        return


    # 5) CLIENT: текстовая причина отмены
    cancel_text = context.user_data.get("client_cancel_reason_text")
    if cancel_text:
        bid = cancel_text["booking_id"]
        reason = text
        if len(reason) < 2:
            await update.message.reply_text("Причина слишком короткая. Напишите чуть подробнее.")
            return
        context.user_data.pop("client_cancel_reason_text", None)
        await finalize_client_cancel(update.message, context, bid, reason)
        return

    # 5.5) MASTER: переименование услуги
    st_rename = context.user_data.get("svc_rename")
    if st_rename and is_master(user_id):
        service_id = int(st_rename.get("service_id"))
        new_name = text.strip()

        if len(new_name) < 2:
            await update.message.reply_text("Название слишком короткое. Введите ещё раз.")
            return

        updated = False

        # 1) кастомные услуги мастера
        arr = services_custom.get(str(user_id), [])
        if isinstance(arr, list):
            for s in arr:
                if int(s.get("id", -1)) == service_id:
                    s["name"] = new_name
                    updated = True
                    await save_services_custom_locked()
                    break

        # 2) базовые услуги мастера (masters_custom[mid]["services"])
        if not updated:
            ensure_master_profile(user_id)  # у вас такая функция есть
            base = masters_custom.get(str(user_id), {}).get("services", [])
            if isinstance(base, list):
                for s in base:
                    if int(s.get("id", -1)) == service_id:
                        s["name"] = new_name
                        updated = True
                        await save_masters_custom_locked()
                        break

        context.user_data.pop("svc_rename", None)

        if updated:
            await update.message.reply_text(f"✅ Переименовано: {new_name}")
        else:
            await update.message.reply_text("Не удалось найти услугу для переименования.")
        return

    # 6) MASTER: добавление услуги (wizard)
    if await svc_add_text(update, context):
        return

    # 7) MASTER: редактирование услуги (price/duration)
    edit = context.user_data.get("svc_edit")
    if edit and is_master(user_id):
        field = edit["field"]
        service_id = edit["service_id"]

        if not text.isdigit():
            await update.message.reply_text("Нужно число. Попробуйте ещё раз.")
            return

        value = int(text)
        if field == "duration":
            if value <= 0:
                await update.message.reply_text("Длительность должна быть > 0.")
                return
            if value % TIME_STEP != 0:
                await update.message.reply_text(f"Длительность должна быть кратна {TIME_STEP} минутам (например: 30, 60, 90, 120).")
                return
        if field == "price" and value <= 0:
            await update.message.reply_text("Цена должна быть > 0.")
            return

        await set_service_override(user_id, service_id, **{field: value})
        context.user_data.pop("svc_edit", None)

        await update.message.reply_text("✅ Сохранено.")
        await update.message.reply_text("Откройте /master → 🛠 Мои услуги, чтобы увидеть изменения.")
        return

    # 8) ЧАТ: если пользователь сейчас в активном чате — пересылаем
    booking_id = active_chat_by_user.get(user_id)
    if not booking_id:
        return

    chat = active_chats.get(booking_id)
    if not chat:
        active_chat_by_user.pop(user_id, None)
        return

    if user_id == chat["client_id"]:
        await context.bot.send_message(chat_id=chat["master_id"], text=f"💬 Сообщение от клиента (заявка #{booking_id}):\n\n{text}")
        return

    if user_id == chat["master_id"]:
        await context.bot.send_message(chat_id=chat["client_id"], text=f"💬 Сообщение от мастера:\n\n{text}")
        return

WEEKDAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]

def _add_master_days_kb(days: list[int]) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, wd in enumerate(WEEKDAYS):
        mark = "✅" if i in days else "▫️"
        row.append(InlineKeyboardButton(f"{mark} {wd}", callback_data=f"adm_add_day_{i}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([
        InlineKeyboardButton("➡ Далее", callback_data="adm_add_days_next"),
        InlineKeyboardButton("❌ Отмена", callback_data="adm_add_cancel"),
    ])
    return InlineKeyboardMarkup(rows)

async def admin_add_master_days_menu(message, context: ContextTypes.DEFAULT_TYPE):
    st = context.user_data.get("admin_add_master")
    if not st:
        return
    days = st.get("schedule_days", [])
    await safe_edit_text(message, "Выберите рабочие дни мастера:", _add_master_days_kb(days))

async def adm_add_day_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    st = context.user_data.get("admin_add_master")
    if not st:
        await q.answer("Сеанс устарел", show_alert=True)
        return

    day = int(q.data.split("_")[-1])
    days = st.setdefault("schedule_days", [])
    if day in days:
        days.remove(day)
    else:
        days.append(day)
        days.sort()

    await admin_add_master_days_menu(q.message, context)

async def adm_add_days_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    st = context.user_data.get("admin_add_master")
    if not st:
        await q.answer("Сеанс устарел", show_alert=True)
        return

    if not st.get("schedule_days"):
        await q.answer("Выберите хотя бы 1 день", show_alert=True)
        return

    st["step"] = "schedule_start"
    await safe_edit_text(q.message, "Введите время начала HH:MM (например 10:00):")

async def adm_add_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    context.user_data.pop("admin_add_master", None)
    await safe_edit_text(q.message, "Отменено.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅ В админку", callback_data="admin_back")]]))

def _list_backups(limit: int = 5) -> list[Path]:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    zips = sorted(BACKUP_DIR.glob("backup_*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    return zips[:limit]


async def admin_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    items = _list_backups(5)
    if items:
        lines = []
        for p in items:
            size_kb = max(1, p.stat().st_size // 1024)
            dt = datetime.fromtimestamp(p.stat().st_mtime).strftime("%d.%m.%Y %H:%M:%S")
            lines.append(f"• {p.name} — {size_kb} KB — {dt}")
        txt = "📦 Бэкапы\n\nПоследние:\n" + "\n".join(lines)
    else:
        txt = "📦 Бэкапы\n\n— пока нет ни одного архива —"

    kb = [
        [InlineKeyboardButton("✅ Сделать бэкап сейчас", callback_data="admin_backup_now")],
        [InlineKeyboardButton("⬅ Назад", callback_data="admin_back")],
    ]
    await safe_edit_text(q.message, txt, InlineKeyboardMarkup(kb))


async def admin_backup_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if not await guard_admin(q):
        return

    try:
        p = await asyncio.to_thread(make_backup_zip)   # ✅ ВОТ ТУТ
        with open(p, "rb") as f:
            await context.bot.send_document(
                chat_id=q.message.chat_id,
                document=f,
                filename=p.name,
                caption=f"✅ Бэкап создан: {p.name}",
            )

        kb = [
            [InlineKeyboardButton("🔄 Обновить список", callback_data="admin_backup")],
            [InlineKeyboardButton("⬅ Назад", callback_data="admin_back")],
        ]
        await safe_edit_text(q.message, "✅ Готово. Архив отправил файлом выше.", InlineKeyboardMarkup(kb))

    except Exception as e:
        kb = [
            [InlineKeyboardButton("🔁 Попробовать ещё раз", callback_data="admin_backup_now")],
            [InlineKeyboardButton("⬅ Назад", callback_data="admin_back")],
        ]
        await safe_edit_text(q.message, f"❌ Ошибка при создании бэкапа:\n{e!r}", InlineKeyboardMarkup(kb))

def reschedule_all_followups(job_queue: JobQueue):
    cfg = get_followup_cfg()
    # если выключили — просто убрать все followup jobs у подтверждённых
    if not cfg["enabled"]:
        for b in bookings:
            if b.get("status") == "CONFIRMED" and isinstance(b.get("id"), int):
                remove_followup(job_queue, b["id"])
        return

    for b in bookings:
        if b.get("status") != "CONFIRMED":
            continue
        if b.get("followup_sent") is True:
            continue
        if b.get("client_rating") is not None:
            continue
        bid = b.get("id")
        if not isinstance(bid, int):
            continue

        # пересоздаём
        remove_followup(job_queue, bid)
        schedule_followup_for_booking(job_queue, b)

async def noop_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q:
        await q.answer()


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    if not TOKEN or ":" not in TOKEN:
        raise RuntimeError(f"Плохой TOKEN: {repr(TOKEN)}")

    app = (
        Application.builder()
        .token(TOKEN)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .build()
    )

    # DB init (SYNC!)
    init_db()

    restore_reminders(app.job_queue)
    restore_followups(app.job_queue)
    app.add_error_handler(error_handler)

    # CLIENT
    app.add_handler(TypeHandler(Update, track_user_update), group=-1)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(choose_master, pattern=r"^master_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(choose_service, pattern=r"^service_", block=False))
    app.add_handler(CallbackQueryHandler(choose_date, pattern=r"^date_", block=False))
    app.add_handler(CallbackQueryHandler(choose_time, pattern=r"^time_", block=False))
    app.add_handler(CommandHandler("mybooking", my_booking))
    app.add_handler(CallbackQueryHandler(next_days, pattern=r"^next_days$", block=False))
    app.add_handler(CallbackQueryHandler(prev_days, pattern=r"^prev_days$", block=False))
    app.add_handler(CallbackQueryHandler(client_change_service_start, pattern=r"^client_change_service_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(client_chsvc_toggle, pattern=r"^client_chsvc_tgl_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(client_chsvc_apply, pattern=r"^client_chsvc_apply$", block=False))
    app.add_handler(CallbackQueryHandler(nearest_times, pattern=r"^nearest_times$", block=False))
    app.add_handler(CallbackQueryHandler(nearest_back, pattern=r"^nearest_back$", block=False))
    app.add_handler(CallbackQueryHandler(nearest_pick, pattern=r"^nearest_pick_", block=False))
    app.add_handler(CallbackQueryHandler(remind_yes, pattern=r"^remind_yes_\d+$", block=False))


    # chat + text router
    app.add_handler(CallbackQueryHandler(start_chat, pattern=r"^chat_", block=False))
    app.add_handler(CallbackQueryHandler(end_chat, pattern=r"^end_chat_", block=False))
    app.add_handler(MessageHandler(filters.CONTACT, pending_contact_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, relay_messages))

    # client cancel/resched
    app.add_handler(CallbackQueryHandler(client_cancel_menu, pattern=r"^client_cancel_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(client_cancel_choose, pattern=r"^client_cancel_choose_\d+_(resched|cancel)$", block=False))
    app.add_handler(CallbackQueryHandler(client_cancel_reason_pick, pattern=r"^client_cancel_reason_\d+_[a-z_]+$", block=False))
    app.add_handler(CallbackQueryHandler(client_resched_next_days, pattern=r"^client_resched_next$", block=False))
    app.add_handler(CallbackQueryHandler(client_resched_prev_days, pattern=r"^client_resched_prev$", block=False))
    app.add_handler(CallbackQueryHandler(client_resched_choose_date, pattern=r"^client_resched_date_", block=False))
    app.add_handler(CallbackQueryHandler(client_resched_choose_time, pattern=r"^client_resched_time_", block=False))

    # MASTER
    app.add_handler(CommandHandler("master", master_menu))
    app.add_handler(CallbackQueryHandler(back_to_master, pattern=r"^back_to_master$", block=False))

    app.add_handler(CallbackQueryHandler(master_pending, pattern=r"^master_pending$", block=False))
    app.add_handler(CallbackQueryHandler(master_confirmed, pattern=r"^master_confirmed$", block=False))
    app.add_handler(CallbackQueryHandler(confirm_booking, pattern=r"^confirm_", block=False))
    app.add_handler(CallbackQueryHandler(cancel_booking, pattern=r"^cancel_booking_", block=False))

    app.add_handler(CallbackQueryHandler(master_close_day, pattern=r"^master_close_day$", block=False))
    app.add_handler(CallbackQueryHandler(master_next_days, pattern=r"^m_next_days$", block=False))
    app.add_handler(CallbackQueryHandler(master_prev_days, pattern=r"^m_prev_days$", block=False))
    app.add_handler(CallbackQueryHandler(choose_block_type, pattern=r"^choose_block_type_", block=False))
    app.add_handler(CallbackQueryHandler(block_hours_menu, pattern=r"^block_hours_", block=False))
    app.add_handler(CallbackQueryHandler(block_day, pattern=r"^block_day_", block=False))
    app.add_handler(CallbackQueryHandler(block_apply_handler, pattern=r"^block_apply_", block=False))
    app.add_handler(CallbackQueryHandler(block_clear_handler, pattern=r"^block_clear_", block=False))
    app.add_handler(CallbackQueryHandler(block_time_handler, pattern=r"^block_time_", block=False))
    app.add_handler(CallbackQueryHandler(block_view_day, pattern=r"^block_view_", block=False))
    app.add_handler(CallbackQueryHandler(unblock_day_handler, pattern=r"^unblock_day_", block=False))
    app.add_handler(CallbackQueryHandler(unblock_time_handler, pattern=r"^unblock_time_", block=False))
    app.add_handler(CallbackQueryHandler(master_blocks, pattern=r"^master_blocks$", block=False))

    app.add_handler(CallbackQueryHandler(master_services, pattern=r"^master_services$", block=False))
    app.add_handler(CallbackQueryHandler(svc_manage, pattern=r"^svc_manage_", block=False))
    app.add_handler(CallbackQueryHandler(svc_toggle, pattern=r"^svc_toggle_", block=False))
    app.add_handler(CallbackQueryHandler(svc_edit_price, pattern=r"^svc_edit_price_", block=False))
    app.add_handler(CallbackQueryHandler(svc_edit_duration, pattern=r"^svc_edit_duration_", block=False))
    app.add_handler(CallbackQueryHandler(svc_add_start, pattern=r"^svc_add$", block=False))
    app.add_handler(CallbackQueryHandler(master_cal_open, pattern=r"^mcal_open$", block=False))
    app.add_handler(CallbackQueryHandler(master_cal_month, pattern=r"^mcal_month_\d{4}-\d{2}$", block=False))
    app.add_handler(CallbackQueryHandler(master_cal_day, pattern=r"^mcal_day_\d{4}-\d{2}-\d{2}$", block=False))
    app.add_handler(CallbackQueryHandler(noop_cb, pattern=r"^noop$", block=False))
    app.add_handler(CallbackQueryHandler(m_set_photo, pattern="^m_set_photo$", block=False))
    app.add_handler(CallbackQueryHandler(block_period, pattern=r"^block_period$", block=False))
    app.add_handler(CallbackQueryHandler(block_period, pattern=r"^block_period$", block=False))
    app.add_handler(CallbackQueryHandler(bp_next, pattern=r"^bp_next$", block=False))
    app.add_handler(CallbackQueryHandler(bp_prev, pattern=r"^bp_prev$", block=False))
    app.add_handler(CallbackQueryHandler(bp_pick, pattern=r"^bp_pick_", block=False))

    # ловим фото/картинку
    app.add_handler(MessageHandler(filters.PHOTO, m_receive_photo))

    # и на всякий случай ловим текст "-" для удаления, когда мы "ждём фото"
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, m_receive_photo))



    # NEW: rename/delete service handlers (ВАЖНО: app.add_handler, не application.add_handler)
    app.add_handler(CallbackQueryHandler(svc_rename_start, pattern=r"^svc_rename_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(svc_delete_confirm, pattern=r"^svc_delete_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(svc_delete_do, pattern=r"^svc_delete_do_\d+$", block=False))

    app.add_handler(CallbackQueryHandler(cancel_by_master, pattern=r"^cancel_master_", block=False))
    app.add_handler(CallbackQueryHandler(start_reschedule, pattern=r"^reschedule_", block=False))
    app.add_handler(CallbackQueryHandler(reschedule_choose_date, pattern=r"^resched_date_", block=False))
    app.add_handler(CallbackQueryHandler(reschedule_confirm, pattern=r"^resched_time_", block=False))

    app.add_handler(CallbackQueryHandler(master_profile, pattern=r"^master_profile$", block=False))
    app.add_handler(CallbackQueryHandler(m_edit_about, pattern=r"^m_edit_about$", block=False))
    app.add_handler(CallbackQueryHandler(m_edit_contacts, pattern=r"^m_edit_contacts$", block=False))
    app.add_handler(CallbackQueryHandler(m_edit_schedule, pattern=r"^m_edit_schedule$", block=False))

    # BACK (client wizard)
    app.add_handler(CallbackQueryHandler(go_back, pattern=r"^back$", block=False))

    # ADMIN
    app.add_handler(CommandHandler("admin", admin_menu))
    app.add_handler(CallbackQueryHandler(admin_back, pattern=r"^admin_back$", block=False))

    app.add_handler(CallbackQueryHandler(admin_masters, pattern=r"^admin_masters$", block=False))
    app.add_handler(CallbackQueryHandler(admin_master_open, pattern=r"^admin_master_\d+$", block=False))

    app.add_handler(CallbackQueryHandler(admin_mcal_open, pattern=r"^admin_mcal_open_", block=False))
    app.add_handler(CallbackQueryHandler(admin_mcal_month, pattern=r"^admin_mcal_month_", block=False))
    app.add_handler(CallbackQueryHandler(admin_mcal_day, pattern=r"^admin_mcal_day_", block=False))
    app.add_handler(CallbackQueryHandler(admin_master_toggle, pattern=r"^admin_master_toggle_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(admin_master_rename, pattern=r"^admin_master_rename_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(admin_master_add_start, pattern=r"^admin_master_add$", block=False))

    app.add_handler(CallbackQueryHandler(admin_bookings, pattern=r"^admin_bookings$", block=False))
    app.add_handler(CallbackQueryHandler(admin_bookings_list, pattern=r"^admin_bookings_days_(0|7|30|all)_page_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(admin_booking_open, pattern=r"^admin_booking_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(admin_booking_confirm, pattern=r"^admin_booking_confirm_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(admin_booking_cancel, pattern=r"^admin_booking_cancel_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(admin_booking_msg_client, pattern=r"^admin_booking_msg_client_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(admin_booking_msg_master, pattern=r"^admin_booking_msg_master_\d+$", block=False))

    app.add_handler(CallbackQueryHandler(admin_stats, pattern=r"^admin_stats$", block=False))
    app.add_handler(CallbackQueryHandler(admin_stats_show, pattern=r"^admin_stats_days_(0|7|30|all)$", block=False))

    app.add_handler(CallbackQueryHandler(admin_settings, pattern=r"^admin_settings$", block=False))
    app.add_handler(CallbackQueryHandler(admin_settings_reminders, pattern=r"^admin_settings_reminders$", block=False))
    app.add_handler(CallbackQueryHandler(admin_set_rem_client, pattern=r"^admin_set_rem_client$", block=False))
    app.add_handler(CallbackQueryHandler(admin_set_rem_master, pattern=r"^admin_set_rem_master$", block=False))

    app.add_handler(CallbackQueryHandler(admin_master_about, pattern=r"^admin_master_about_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(admin_master_contacts, pattern=r"^admin_master_contacts_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(admin_master_schedule, pattern=r"^admin_master_schedule_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(sch_toggle, pattern=r"^(a|m)_sch_tgl_\d+_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(sch_next, pattern=r"^(a|m)_sch_next_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(sch_cancel, pattern=r"^(a|m)_sch_cancel_\d+$", block=False))

    app.add_handler(CallbackQueryHandler(admin_master_del_prompt, pattern=r"^admin_master_del_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(admin_master_del_do, pattern=r"^admin_master_del_do_\d+$", block=False))

    app.add_handler(CallbackQueryHandler(adm_add_day_toggle, pattern=r"^adm_add_day_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(adm_add_days_next, pattern=r"^adm_add_days_next$", block=False))
    app.add_handler(CallbackQueryHandler(adm_add_cancel, pattern=r"^adm_add_cancel$", block=False))
    app.add_handler(CallbackQueryHandler(admin_admins, pattern=r"^admin_admins$", block=False))
    app.add_handler(CallbackQueryHandler(admin_admin_add_start, pattern=r"^admin_admin_add$", block=False))
    app.add_handler(CallbackQueryHandler(admin_admin_remove_start, pattern=r"^admin_admin_remove$", block=False))
    app.add_handler(CallbackQueryHandler(rate_pick, pattern=r"^rate_\d+_[1-5]$", block=False))
    app.add_handler(CallbackQueryHandler(admin_settings_followup, pattern=r"^admin_settings_followup$", block=False))
    app.add_handler(CallbackQueryHandler(admin_followup_toggle, pattern=r"^admin_followup_toggle$", block=False))
    app.add_handler(CallbackQueryHandler(admin_followup_set_hours, pattern=r"^admin_followup_set_hours$", block=False))
    app.add_handler(CallbackQueryHandler(admin_followup_set_2gis, pattern=r"^admin_followup_set_2gis$", block=False))
    app.add_handler(CallbackQueryHandler(admin_followup_set_ask, pattern=r"^admin_followup_set_ask$", block=False))
    app.add_handler(CallbackQueryHandler(admin_followup_set_thanks, pattern=r"^admin_followup_set_thanks$", block=False))
    app.add_handler(CallbackQueryHandler(admin_backup, pattern=r"^admin_backup$", block=False))
    app.add_handler(CallbackQueryHandler(admin_backup_now, pattern=r"^admin_backup_now$", block=False))
    app.add_handler(CallbackQueryHandler(m_contacts_menu, pattern=r"^m_contacts_menu$", block=False))
    app.add_handler(CallbackQueryHandler(m_edit_contact, pattern=r"^m_edit_contact_(phone|instagram|telegram|address)$", block=False))
    app.add_handler(CallbackQueryHandler(svc_pick, pattern=r"^svc_pick_\d+$", block=False))
    app.add_handler(CallbackQueryHandler(svc_next, pattern=r"^svc_next$", block=False))

    app.run_polling()

if __name__ == "__main__":
    main()

