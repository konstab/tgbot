import os
from pathlib import Path
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().with_name(".env")

# override=True — перезапишет переменную, если она уже есть (даже пустая)
load_dotenv(dotenv_path=ENV_PATH, override=True)

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise RuntimeError(f"BOT_TOKEN не задан. Файл .env ищу тут: {ENV_PATH}")

# --- общие настройки проекта (мастеров и графиков тут больше нет) ---

ADMIN_IDS = {5039068643}  # можно несколько: {5039068643, 111111111}

TIME_STEP = 30           # шаг слотов (мин)
PAUSE_MINUTES = 0        # пауза после услуги (мин), если надо

DATE_FORMAT = "%d.%m.%Y"  # DD.MM.YYYY
TIME_FORMAT = "%H:%M"     # HH:MM

DAYS_PER_PAGE = 7
MASTER_DAYS_PER_PAGE = 7

REMINDERS = {
    "client": {"hours": 24},
    "master": {"hours": 1}
}
