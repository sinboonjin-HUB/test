import asyncio
import logging
import os
import re
from datetime import datetime, date, timedelta
from typing import List, Optional, Tuple

import aiosqlite
from dateutil.relativedelta import relativedelta
from dateutil.parser import isoparse
from pydantic import BaseModel, Field, ValidationError
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ContextTypes,
    MessageHandler, filters, CallbackQueryHandler, AIORateLimiter,
)

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("ippt-bot")

# ----------------------------
# Settings (env-driven)
# ----------------------------
class Settings(BaseModel):
    BOT_TOKEN: str
    ADMIN_IDS: List[int] = Field(default_factory=list)
    DB_PATH: str = "/data/ippt.db"
    TZ: str = "Asia/Singapore"
    REMINDER_INTERVAL_DAYS: int = 10

    @classmethod
    def from_env(cls) -> "Settings":
        raw_admins = os.getenv("ADMIN_IDS", "")
        admin_ids = []
        for tok in [t.strip() for t in raw_admins.split(",") if t.strip()]:
            try:
                admin_ids.append(int(tok))
            except ValueError:
                log.warning("Skipping non-int ADMIN_IDS token: %s", tok)

        return cls(
            BOT_TOKEN=os.environ["BOT_TOKEN"],
            ADMIN_IDS=admin_ids,
            DB_PATH=os.getenv("DB_PATH", "/data/ippt.db"),
            TZ=os.getenv("TZ", "Asia/Singapore"),
            REMINDER_INTERVAL_DAYS=int(os.getenv("REMINDER_INTERVAL_DAYS", "10")),
        )

SET = Settings.from_env()

# ----------------------------
# DB helpers (SQLite via aiosqlite)
# ----------------------------
SCHEMA = """
CREATE TABLE IF NOT EXISTS persons (
    user_id INTEGER PRIMARY KEY,
    chat_id INTEGER NOT NULL,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    birthday TEXT NOT NULL,         -- stored as ISO date (YYYY-MM-DD) for the current year reference
    completed_on TEXT,              -- ISO date when IPPT completed (year-specific)
    last_reminded_at TEXT,          -- ISO datetime ISO format
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""

# lightweight migrations: add missing columns if an older DB exists
MIGRATIONS = [
    ("ALTER TABLE persons ADD COLUMN last_reminded_at TEXT", "last_reminded_at"),
]

async def ensure_schema(db: aiosqlite.Connection) -> None:
    await db.executescript(SCHEMA)
    # discover columns
    cols = set()
    async with db.execute("PRAGMA table_info(persons)") as cur:
        async for row in cur:
            cols.add(row[1])
    for sql, col in MIGRATIONS:
        if col not in cols:
            log.info("Applying migration: add column %s", col)
            await db.execute(sql)
    await db.commit()

async def open_db() -> aiosqlite.Connection:
    os.makedirs(os.path.dirname(SET.DB_PATH), exist_ok=True)
    db = await aiosqlite.connect(SET.DB_PATH)
    await ensure_schema(db)
    db.row_factory = aiosqlite.Row
    return db

# ----------------------------
# Utilities
# ----------------------------
DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

def today_sgt() -> date:
    # SG has no DST; we can safely use local date from UTC offset by +8 if needed.
    # For simplicity, compute from current UTC and add 8h.
    # However, Python's date.today() on the container is typically UTC. We'll just use UTC+8 proxy:
    return (datetime.utcnow() + timedelta(hours=8)).date()

def within_100_day_window(bday: date, ref: date) -> Tuple[bool, date, date]:
    """
    The "window" is 100 days after the *current-year* birthday date.
    We assume stored birthday is the YYYY-MM-DD of the user's actual birthdate (month, day are used).
    """
    # align bday to current year
    bday_this_year = bday.replace(year=ref.year)
    start = bday_this_year
    end = bday_this_year + timedelta(days=100)
    return (start <= ref <= end, start, end)

def parse_iso_date(s: str) -> date:
    return isoparse(s).date()

def fmt_user(row) -> str:
    name = (row["first_name"] or "") + (" " + row["last_name"] if row["last_name"] else "")
    name = name.strip() or (row["username"] or f"u{row['user_id']}")
    return name

# ----------------------------
# Core operations
# ----------------------------
async def upsert_person(
    db: aiosqlite.Connection,
    user_id: int,
    chat_id: int,
    username: Optional[str],
    first_name: Optional[str],
    last_name: Optional[str],
) -> None:
    now = datetime.utcnow().isoformat()
    await db.execute(
        """
        INSERT INTO persons (user_id, chat_id, username, first_name, last_name, birthday, completed_on, last_reminded_at, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, COALESCE((SELECT birthday FROM persons WHERE user_id=?), '1970-01-01'),
                COALESCE((SELECT completed_on FROM persons WHERE user_id=?), NULL),
                COALESCE((SELECT last_reminded_at FROM persons WHERE user_id=?), NULL),
                COALESCE((SELECT created_at FROM persons WHERE user_id=?), ?), ?)
        ON CONFLICT(user_id) DO UPDATE SET
          chat_id=excluded.chat_id,
          username=excluded.username,
          first_name=excluded.first_name,
          last_name=excluded.last_name,
          updated_at=excluded.updated_at
        """,
        (user_id, chat_id, username, first_name, last_name,
         user_id, user_id, user_id, user_id, now, now)
    )
    await db.commit()

async def set_birthday(db: aiosqlite.Connection, user_id: int, bday: date) -> None:
    now = datetime.utcnow().isoformat()
    await db.execute(
        "UPDATE persons SET birthday=?, updated_at=? WHERE user_id=?",
        (bday.isoformat(), now, user_id)
    )
    await db.commit()

async def set_completed(db: aiosqlite.Connection, user_id: int, completed_on: date) -> None:
    now = datetime.utcnow().isoformat()
    await db.execute(
        "UPDATE persons SET completed_on=?, updated_at=? WHERE user_id=?",
        (completed_on.isoformat(), now, user_id)
    )
    await db.commit()

async def clear_completed(db: aiosqlite.Connection, user_id: int) -> None:
    now = datetime.utcnow().isoformat()
    await db.execute(
        "UPDATE persons SET completed_on=NULL, updated_at=? WHERE user_id=?",
        (now, user_id)
    )
    await db.commit()

async def get_person(db: aiosqlite.Connection, user_id: int):
    async with db.execute("SELECT * FROM persons WHERE user_id=?", (user_id,)) as cur:
        return await cur.fetchone()

async def list_people(db: aiosqlite.Connection):
    async with db.execute("SELECT * FROM persons ORDER BY birthday ASC") as cur:
        rows = await cur.fetchall()
    return rows

async def due_for_reminder(db: aiosqlite.Connection, ref_dt: datetime) -> List[aiosqlite.Row]:
    # Idempotent: only remind if last_reminded_at is older than interval.
    interval = timedelta(days=SET.REMINDER_INTERVAL_DAYS)
    rows = []
    async with db.execute("SELECT * FROM persons") as cur:
        async for row in cur:
            bday = parse_iso_date(row["birthday"])
            ref = ref_dt.date()
            in_window, start, end = within_100_day_window(bday, ref)
            if not in_window:
                continue
            if row["completed_on"]:
                continue
            last_s = row["last_reminded_at"]
            if last_s:
                last = isoparse(last_s)
                if ref_dt - last < interval:
                    continue
            rows.append(row)
    return rows

async def mark_reminded(db: aiosqlite.Connection, user_id: int, at: datetime) -> None:
    await db.execute("UPDATE persons SET last_reminded_at=?, updated_at=? WHERE user_id=?",
                     (at.isoformat(), at.isoformat(), user_id))
    await db.commit()

def parse_date_arg(text: str) -> Optional[date]:
    m = DATE_RE.search(text)
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None

def is_admin(uid: int) -> bool:
    return uid in SET.ADMIN_IDS

# ----------------------------
# Command Handlers
# ----------------------------
HELP_TEXT = (
    "*IPPT Reminder Bot*\n\n"
    "Commands:\n"
    "• /start – register yourself with the bot\n"
    "• /setbirthday YYYY-MM-DD – set your birthday (month/day are used each year)\n"
    "• /summary – see your current status & window\n"
    "• /complete [--date YYYY-MM-DD] – mark IPPT completed (defaults to today)\n"
    "• /uncomplete – clear completion *only if within your 100-day window*\n"
    "• /export – get CSV of all users (admin only)\n"
    "• /import – upload a CSV in reply to this command (admin only)\n"
    "• /admin_add <telegram_id> – add an admin (admin only)\n"
    "• /admin_remove <telegram_id> – remove an admin (admin only)\n"
    "• /admin_complete <user_id> [YYYY-MM-DD|--date YYYY-MM-DD] – admin override\n"
    "• /help – show this help\n"
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.effective_chat:
        return
    user = update.effective_user
    chat = update.effective_chat

    async with open_db() as db:
        await upsert_person(
            db, user.id, chat.id, user.username, user.first_name, user.last_name
        )

    await update.message.reply_text(
        "Welcome! You’re registered.\n\n" + HELP_TEXT, parse_mode=ParseMode.MARKDOWN
    )

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode=ParseMode.MARKDOWN)

async def setbirthday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    args = update.message.text.split()
    if len(args) < 2:
        await update.message.reply_text("Usage: /setbirthday YYYY-MM-DD")
        return
    try:
        bday = parse_date_arg(args[1]) or parse_date_arg(update.message.text)
        if not bday:
            raise ValueError
    except Exception:
        await update.message.reply_text("Invalid date. Use YYYY-MM-DD.")
        return

    async with open_db() as db:
        await set_birthday(db, update.effective_user.id, bday)
    await update.message.reply_text(f"Birthday set to {bday.isoformat()} ✅")

async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with open_db() as db:
        row = await get_person(db, update.effective_user.id)
    if not row or row["birthday"] == "1970-01-01":
        await update.message.reply_text("Set your birthday first: /setbirthday YYYY-MM-DD")
        return

    bday = parse_iso_date(row["birthday"])
    ref = today_sgt()
    in_window, start, end = within_100_day_window(bday, ref)
    status = "✅ Completed" if row["completed_on"] else ("🟡 In window" if in_window else "🕒 Out of window")
    completed_on = row["completed_on"] or "—"
    txt = (
        f"*Your status*\n"
        f"Name: {update.effective_user.full_name}\n"
        f"Birthday: {bday.strftime('%Y-%m-%d')}\n"
        f"Window: {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}\n"
        f"Today: {ref.strftime('%Y-%m-%d')}\n"
        f"Completed on: {completed_on}\n"
        f"Status: {status}"
    )
    await update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)

async def complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # User self-complete
    target_date = parse_date_arg(update.message.text) or today_sgt()
    async with open_db() as db:
        await set_completed(db, update.effective_user.id, target_date)
    await update.message.reply_text(f"Marked completed on {target_date.isoformat()} ✅")

async def uncomplete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Only allowed if user is within their current 100-day window
    async with open_db() as db:
        row = await get_person(db, update.effective_user.id)
        if not row or row["birthday"] == "1970-01-01":
            await update.message.reply_text("Set your birthday first: /setbirthday YYYY-MM-DD")
            return
        bday = parse_iso_date(row["birthday"])
        ref = today_sgt()
        in_window, _, _ = within_100_day_window(bday, ref)
        if not in_window:
            await update.message.reply_text("You can only /uncomplete while inside your 100-day window.")
            return
        await clear_completed(db, update.effective_user.id)
    await update.message.reply_text("Completion cleared ✅")

# ----- Admin helpers -----
def require_admin(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id if update.effective_user else 0
        if not is_admin(uid):
            await update.effective_message.reply_text("Admin only.")
            return
        return await func(update, context)
    return wrapper

@require_admin
async def admin_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()
    if len(args) != 2 or not args[1].isdigit():
        await update.message.reply_text("Usage: /admin_add <telegram_id>")
        return
    new_id = int(args[1])
    if new_id in SET.ADMIN_IDS:
        await update.message.reply_text(f"{new_id} is already an admin.")
        return
    SET.ADMIN_IDS.append(new_id)
    await update.message.reply_text(f"Added admin: {new_id} ✅")

@require_admin
async def admin_remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split()
    if len(args) != 2 or not args[1].isdigit():
        await update.message.reply_text("Usage: /admin_remove <telegram_id>")
        return
    rid = int(args[1])
    if rid not in SET.ADMIN_IDS:
        await update.message.reply_text(f"{rid} is not an admin.")
        return
    SET.ADMIN_IDS = [x for x in SET.ADMIN_IDS if x != rid]
    await update.message.reply_text(f"Removed admin: {rid} ✅")

@require_admin
async def admin_complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Usage:
      /admin_complete <user_id> [YYYY-MM-DD or --date YYYY-MM-DD]
    Bare YYYY-MM-DD anywhere in the text also works.
    """
    text = update.message.text
    parts = text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        await update.message.reply_text("Usage: /admin_complete <user_id> [YYYY-MM-DD|--date YYYY-MM-DD]")
        return
    target_user = int(parts[1])
    # Find a date token either `--date YYYY-MM-DD` or any bare YYYY-MM-DD
    d = parse_date_arg(text) or today_sgt()
    async with open_db() as db:
        await set_completed(db, target_user, d)
    await update.message.reply_text(f"User {target_user} marked completed on {d.isoformat()} ✅")

@require_admin
async def export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import csv
    from io import StringIO
    async with open_db() as db:
        rows = await list_people(db)
    buf = StringIO()
    w = csv.writer(buf)
    w.writerow(["user_id","chat_id","username","first_name","last_name","birthday","completed_on","last_reminded_at","created_at","updated_at"])
    for r in rows:
        w.writerow([r[k] for k in ["user_id","chat_id","username","first_name","last_name","birthday","completed_on","last_reminded_at","created_at","updated_at"]])
    buf.seek(0)
    await update.message.reply_document(document=buf.getvalue().encode("utf-8"),
                                        filename="persons.csv",
                                        caption="Exported users")

@require_admin
async def import_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text("Reply to a CSV file with /import.")
        return
    doc = update.message.reply_to_message.document
    file = await doc.get_file()
    raw = await file.download_as_bytearray()
    import csv, io
    count = 0
    async with open_db() as db:
        async with db.execute("SELECT 1") as _:
            pass  # ensure DB
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))
        for row in reader:
            try:
                await upsert_person(
                    db,
                    int(row["user_id"]),
                    int(row.get("chat_id") or 0),
                    row.get("username"),
                    row.get("first_name"),
                    row.get("last_name"),
                )
                if row.get("birthday"):
                    await set_birthday(db, int(row["user_id"]), parse_iso_date(row["birthday"]))
                if row.get("completed_on"):
                    await set_completed(db, int(row["user_id"]), parse_iso_date(row["completed_on"]))
                count += 1
            except Exception as e:
                log.exception("Failed to import a row: %s", e)
    await update.message.reply_text(f"Imported {count} rows ✅")

# ----------------------------
# Reminders (job queue)
# ----------------------------
async def reminder_tick(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.utcnow()
    async with open_db() as db:
        rows = await due_for_reminder(db, now)
        for r in rows:
            name = fmt_user(r)
            bday = parse_iso_date(r["birthday"])
            _, start, end = within_100_day_window(bday, now.date())
            days_left = (end - now.date()).days
            msg = (
                f"👋 Hi {name}! Your 100-day IPPT window ends on {end.strftime('%Y-%m-%d')}.\n"
                f"Days left: {days_left}.\n\n"
                f"Reply /complete to mark done, or /summary to see details."
            )
            try:
                await context.bot.send_message(chat_id=r["chat_id"], text=msg)
                await mark_reminded(db, r["user_id"], now)
            except Exception as e:
                log.warning("Send failed to %s: %s", r["chat_id"], e)

# ----------------------------
# App bootstrap
# ----------------------------
def build_app() -> Application:
    app = (
        ApplicationBuilder()
        .token(SET.BOT_TOKEN)
        .rate_limiter(AIORateLimiter())  # basic flood protection
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("setbirthday", setbirthday))
    app.add_handler(CommandHandler("summary", summary))
    app.add_handler(CommandHandler("complete", complete))
    app.add_handler(CommandHandler("uncomplete", uncomplete))
    app.add_handler(CommandHandler("admin_add", admin_add))
    app.add_handler(CommandHandler("admin_remove", admin_remove))
    app.add_handler(CommandHandler("admin_complete", admin_complete))
    app.add_handler(CommandHandler("export", export_csv))
    app.add_handler(CommandHandler("import", import_csv))

    # Reminder job: run every REMINDER_INTERVAL_DAYS at 09:00 SGT equivalent
    # We’ll simply run every 24h and let idempotency/interval logic gate messages.
    app.job_queue.run_repeating(reminder_tick, interval=timedelta(hours=24), first=10)

    return app

async def main():
    app = build_app()
    log.info("Starting bot…")
    await app.initialize()
    await app.start()
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    # keep running
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
