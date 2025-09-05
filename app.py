import asyncio
import logging
import os
import re
from datetime import datetime, date, timedelta
from typing import List, Optional, Tuple

import aiosqlite
from dateutil.parser import isoparse
from pydantic import BaseModel, Field
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ContextTypes,
)

# =========================
# Optional Extras (guards)
# =========================
# Optional rate limiter (python-telegram-bot[rate-limiter])
try:
    from telegram.ext import AIORateLimiter as _AIORateLimiter
except Exception:
    _AIORateLimiter = None

# =========================
# Logging
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("ippt-bot")

# =========================
# Settings (env-driven)
# =========================
class Settings(BaseModel):
    BOT_TOKEN: str
    ADMIN_IDS: List[int] = Field(default_factory=list)
    DB_PATH: str = "/data/ippt.db"
    TZ: str = "Asia/Singapore"
    REMINDER_INTERVAL_DAYS: int = 10

    @classmethod
    def from_env(cls) -> "Settings":
        raw_admins = os.getenv("ADMIN_IDS", "")
        admin_ids: List[int] = []
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

# =========================
# DB (SQLite via aiosqlite)
# =========================
SCHEMA = """
CREATE TABLE IF NOT EXISTS persons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT UNIQUE NOT NULL,     -- your ID from the roster (e.g., 001A)
    birthday TEXT NOT NULL,               -- ISO date YYYY-MM-DD (from your roster)
    user_id INTEGER UNIQUE,               -- Telegram user who verified against this record
    chat_id INTEGER,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    display_name TEXT,                    -- user-set name via /setname
    completed_on TEXT,                    -- ISO date when IPPT completed
    last_reminded_at TEXT,                -- ISO datetime
    verified_at TEXT,                     -- ISO datetime
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_persons_user_id ON persons(user_id);
CREATE INDEX IF NOT EXISTS idx_persons_external_id ON persons(external_id);
"""

# If migrating from earlier schema, you may need manual migration; this script assumes fresh or compatible DB.

async def ensure_schema(db: aiosqlite.Connection) -> None:
    await db.executescript(SCHEMA)
    await db.commit()

async def open_db() -> aiosqlite.Connection:
    os.makedirs(os.path.dirname(SET.DB_PATH), exist_ok=True)
    db = await aiosqlite.connect(SET.DB_PATH)
    db.row_factory = aiosqlite.Row
    await ensure_schema(db)
    return db

# =========================
# Utilities
# =========================
DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

def today_sgt() -> date:
    # Approximated as UTC+8 (SG has no DST)
    return (datetime.utcnow() + timedelta(hours=8)).date()

def within_100_day_window(bday: date, ref: date) -> Tuple[bool, date, date]:
    """
    100-day window after the current-year birthday date.
    'birthday' comes from admin-imported roster.
    """
    bday_this_year = bday.replace(year=ref.year)
    start = bday_this_year
    end = bday_this_year + timedelta(days=100)
    return (start <= ref <= end, start, end)

def parse_iso_date(s: str) -> date:
    return isoparse(s).date()

def parse_date_arg(text: str) -> Optional[date]:
    m = DATE_RE.search(text or "")
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None

def is_admin(uid: int) -> bool:
    return uid in SET.ADMIN_IDS

def require_verified(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        async with open_db() as db:
            row = await get_person_by_user(db, update.effective_user.id)
        if not row:
            await update.effective_message.reply_text(
                "You are not verified yet. Use:\n/verify <ID> <YYYY-MM-DD>\n"
                "Example: /verify 001A 1997-05-03"
            )
            return
        return await func(update, context)
    return wrapper

# =========================
# Core DB Ops
# =========================
async def get_person_by_user(db: aiosqlite.Connection, user_id: int):
    async with db.execute("SELECT * FROM persons WHERE user_id=?", (user_id,)) as cur:
        return await cur.fetchone()

async def get_person_by_external_and_bday(db: aiosqlite.Connection, external_id: str, bday: date):
    async with db.execute(
        "SELECT * FROM persons WHERE external_id=? AND birthday=?",
        (external_id, bday.isoformat()),
    ) as cur:
        return await cur.fetchone()

async def link_user_to_person(
    db: aiosqlite.Connection,
    external_id: str,
    user_id: int,
    chat_id: int,
    username: Optional[str],
    first_name: Optional[str],
    last_name: Optional[str],
):
    now = datetime.utcnow().isoformat()
    await db.execute(
        """
        UPDATE persons
        SET user_id=?,
            chat_id=?,
            username=?,
            first_name=?,
            last_name=?,
            verified_at=?,
            updated_at=?
        WHERE external_id=?
        """,
        (user_id, chat_id, username, first_name, last_name, now, now, external_id),
    )
    await db.commit()

async def set_completed(db: aiosqlite.Connection, user_id: int, completed_on: date) -> None:
    now = datetime.utcnow().isoformat()
    await db.execute(
        "UPDATE persons SET completed_on=?, updated_at=? WHERE user_id=?",
        (completed_on.isoformat(), now, user_id)
    )
    await db.commit()

async def set_display_name(db: aiosqlite.Connection, user_id: int, display_name: str) -> None:
    now = datetime.utcnow().isoformat()
    await db.execute(
        "UPDATE persons SET display_name=?, updated_at=? WHERE user_id=?",
        (display_name.strip(), now, user_id)
    )
    await db.commit()

async def list_people(db: aiosqlite.Connection):
    async with db.execute("SELECT * FROM persons ORDER BY external_id ASC") as cur:
        rows = await cur.fetchall()
    return rows

async def due_for_reminder(db: aiosqlite.Connection, ref_dt: datetime):
    """
    Return verified users due for a reminder, respecting REMINDER_INTERVAL_DAYS and completion/window.
    """
    interval = timedelta(days=SET.REMINDER_INTERVAL_DAYS)
    due = []
    async with db.execute("SELECT * FROM persons WHERE user_id IS NOT NULL") as cur:
        async for row in cur:
            # need birthday from roster
            if not row["birthday"]:
                continue
            bday = parse_iso_date(row["birthday"])
            ref = ref_dt.date()
            in_window, _, _ = within_100_day_window(bday, ref)
            if not in_window:
                continue
            if row["completed_on"]:
                continue
            last_s = row["last_reminded_at"]
            if last_s:
                try:
                    last = isoparse(last_s)
                    if ref_dt - last < interval:
                        continue
                except Exception:
                    pass
            due.append(row)
    return due

async def mark_reminded(db: aiosqlite.Connection, user_id: int, at: datetime) -> None:
    await db.execute("UPDATE persons SET last_reminded_at=?, updated_at=? WHERE user_id=?",
                     (at.isoformat(), at.isoformat(), user_id))
    await db.commit()

# =========================
# Reminder helpers
# =========================
def display_name(row) -> str:
    # Prefer user-set display_name, else first/last, else username/external_id
    if row["display_name"]:
        return row["display_name"]
    parts = []
    if row["first_name"]:
        parts.append(row["first_name"])
    if row["last_name"]:
        parts.append(row["last_name"])
    if parts:
        return " ".join(parts)
    return row["username"] or row["external_id"]

def build_reminder_message(row, ref_dt: datetime) -> str:
    name = display_name(row)
    bday = parse_iso_date(row["birthday"])
    _, _, end = within_100_day_window(bday, ref_dt.date())
    days_left = (end - ref_dt.date()).days
    return (
        f"👋 Hi {name}! Your 100-day IPPT window ends on {end.strftime('%Y-%m-%d')}.\n"
        f"Days left: {days_left}.\n\n"
        f"Reply /complete to mark done, or /summary to see details."
    )

async def send_single_reminder(context: ContextTypes.DEFAULT_TYPE, db: aiosqlite.Connection, row, ref_dt: datetime, mark=True):
    msg = build_reminder_message(row, ref_dt)
    try:
        await context.bot.send_message(chat_id=row["chat_id"], text=msg)
        if mark:
            await mark_reminded(db, row["user_id"], ref_dt)
    except Exception as e:
        log.warning("Send failed to %s: %s", row["chat_id"], e)

async def reminder_tick(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.utcnow()
    async with open_db() as db:
        rows = await due_for_reminder(db, now)
        for r in rows:
            await send_single_reminder(context, db, r, now, mark=True)

# --- Fallback scheduler if JobQueue isn't available ---
async def reminder_loop_fallback(app: Application, interval_hours: int = 24):
    """Runs reminder-like logic on a simple async loop."""
    while True:
        now = datetime.utcnow()
        try:
            async with open_db() as db:
                rows = await due_for_reminder(db, now)
                for r in rows:
                    msg = build_reminder_message(r, now)
                    try:
                        await app.bot.send_message(chat_id=r["chat_id"], text=msg)
                        await mark_reminded(db, r["user_id"], now)
                    except Exception as e:
                        log.warning("Send failed to %s: %s", r["chat_id"], e)
        except Exception as e:
            log.exception("reminder_loop_fallback tick failed: %s", e)
        await asyncio.sleep(interval_hours * 3600)

# =========================
# Commands (User)
# =========================
HELP_TEXT = (
    "*IPPT Reminder Bot*\n\n"
    "User commands:\n"
    "• /start – get started\n"
    "• /verify <ID> <YYYY-MM-DD> – verify yourself against the roster\n"
    "• /setname <your preferred name> – set how I address you\n"
    "• /summary – see your status & window\n"
    "• /complete [YYYY-MM-DD] – mark IPPT completed (defaults to today)\n\n"
    "Admin commands:\n"
    "• /export – CSV export\n"
    "• /import – reply to a CSV file with this command\n"
    "• /admin_complete <user_id> [YYYY-MM-DD]\n"
    "• /notify_now [all|<user_id> ...] [--force]\n"
)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Welcome!\n\n"
        "Please verify yourself with:\n"
        "/verify <ID> <YYYY-MM-DD>\n"
        "Example: /verify 001A 1997-05-03\n\n" + HELP_TEXT,
        parse_mode=ParseMode.MARKDOWN
    )

async def verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /verify <external_id> <YYYY-MM-DD>
    Links the Telegram user to a roster record if both match.
    """
    if not update.message:
        return
    parts = (update.message.text or "").split()
    if len(parts) != 3:
        await update.message.reply_text("Usage: /verify <ID> <YYYY-MM-DD>\nExample: /verify 001A 1997-05-03")
        return

    external_id = parts[1].strip()
    bday = parse_date_arg(parts[2])
    if not bday:
        await update.message.reply_text("Invalid date. Use YYYY-MM-DD.")
        return

    async with open_db() as db:
        row = await get_person_by_external_and_bday(db, external_id, bday)
        if not row:
            await update.message.reply_text("No matching record. Check your ID and birthday.")
            return

        # If the record is already linked to someone else, block it
        if row["user_id"] and row["user_id"] != update.effective_user.id:
            await update.message.reply_text("This ID is already verified by another Telegram account.")
            return

        await link_user_to_person(
            db,
            external_id=external_id,
            user_id=update.effective_user.id,
            chat_id=update.effective_chat.id,
            username=update.effective_user.username,
            first_name=update.effective_user.first_name,
            last_name=update.effective_user.last_name,
        )

    await update.message.reply_text("Verification successful ✅\nYou can now /setname, /summary, and /complete.")

@require_verified
async def setname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    name = update.message.text.partition(" ")[2].strip()
    if not name:
        await update.message.reply_text("Usage: /setname <your preferred name>")
        return
    if len(name) > 50:
        await update.message.reply_text("Name too long. Keep it within 50 characters.")
        return
    async with open_db() as db:
        await set_display_name(db, update.effective_user.id, name)
    await update.message.reply_text(f"Okay! I’ll call you *{name}* ✅", parse_mode=ParseMode.MARKDOWN)

@require_verified
async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with open_db() as db:
        row = await get_person_by_user(db, update.effective_user.id)
    bday = parse_iso_date(row["birthday"])
    ref = today_sgt()
    in_window, start_d, end_d = within_100_day_window(bday, ref)
    status = "✅ Completed" if row["completed_on"] else ("🟡 In window" if in_window else "🕒 Out of window")
    who = display_name(row)
    completed_on = row["completed_on"] or "—"
    txt = (
        f"*Your status*\n"
        f"ID: {row['external_id']}\n"
        f"Name: {who}\n"
        f"Birthday (from roster): {bday.strftime('%Y-%m-%d')}\n"
        f"Window: {start_d.strftime('%Y-%m-%d')} → {end_d.strftime('%Y-%m-%d')}\n"
        f"Today: {ref.strftime('%Y-%m-%d')}\n"
        f"Completed on: {completed_on}\n"
        f"Status: {status}"
    )
    await update.message.reply_text(txt, parse_mode=ParseMode.MARKDOWN)

@require_verified
async def complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # allow optional explicit date; default to today
    d = parse_date_arg(update.message.text) or today_sgt()
    async with open_db() as db:
        await set_completed(db, update.effective_user.id, d)
    await update.message.reply_text(f"Marked completed on {d.isoformat()} ✅")

# =========================
# Admin helpers & commands
# =========================
def require_admin(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id if update.effective_user else 0
        if not is_admin(uid):
            await update.effective_message.reply_text("Admin only.")
            return
        return await func(update, context)
    return wrapper

@require_admin
async def export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import csv
    from io import StringIO
    async with open_db() as db:
        rows = await list_people(db)
    buf = StringIO()
    w = csv.writer(buf)
    w.writerow([
        "external_id","birthday","user_id","chat_id","username","first_name","last_name",
        "display_name","completed_on","last_reminded_at","verified_at","created_at","updated_at"
    ])
    for r in rows:
        w.writerow([r[k] for k in [
            "external_id","birthday","user_id","chat_id","username","first_name","last_name",
            "display_name","completed_on","last_reminded_at","verified_at","created_at","updated_at"
        ]])
    buf.seek(0)
    await update.message.reply_document(document=buf.getvalue().encode("utf-8"),
                                        filename="persons.csv",
                                        caption="Exported users")

@require_admin
async def import_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Expected CSV headers (minimum):
      external_id,birthday
    Optional:
      display_name,user_id,chat_id,username,first_name,last_name,completed_on,last_reminded_at,verified_at
    """
    if not update.message or not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text("Reply to a CSV file with /import.")
        return
    doc = update.message.reply_to_message.document
    file = await doc.get_file()
    raw = await file.download_as_bytearray()
    import csv, io
    count = 0
    async with open_db() as db:
        reader = csv.DictReader(io.StringIO(raw.decode("utf-8")))
        now = datetime.utcnow().isoformat()
        for row in reader:
            ext = (row.get("external_id") or "").strip()
            bday = (row.get("birthday") or "").strip()
            if not ext or not bday:
                continue
            # upsert by external_id
            await db.execute("""
                INSERT INTO persons (external_id, birthday, user_id, chat_id, username, first_name, last_name,
                                     display_name, completed_on, last_reminded_at, verified_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(external_id) DO UPDATE SET
                    birthday=excluded.birthday,
                    user_id=COALESCE(persons.user_id, excluded.user_id),
                    chat_id=COALESCE(persons.chat_id, excluded.chat_id),
                    username=COALESCE(persons.username, excluded.username),
                    first_name=COALESCE(persons.first_name, excluded.first_name),
                    last_name=COALESCE(persons.last_name, excluded.last_name),
                    display_name=COALESCE(persons.display_name, excluded.display_name),
                    completed_on=COALESCE(persons.completed_on, excluded.completed_on),
                    last_reminded_at=COALESCE(persons.last_reminded_at, excluded.last_reminded_at),
                    verified_at=COALESCE(persons.verified_at, excluded.verified_at),
                    updated_at=excluded.updated_at
            """, (
                ext, bday,
                _safe_int(row.get("user_id")), _safe_int(row.get("chat_id")),
                row.get("username"), row.get("first_name"), row.get("last_name"),
                row.get("display_name"), row.get("completed_on"), row.get("last_reminded_at"),
                row.get("verified_at"), now, now
            ))
            count += 1
        await db.commit()
    await update.message.reply_text(f"Imported/updated {count} rows ✅")

def _safe_int(x):
    try:
        return int(x) if x is not None and str(x).strip() != "" else None
    except Exception:
        return None

@require_admin
async def admin_complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Usage:
      /admin_complete <user_id> [YYYY-MM-DD]
    """
    text = update.message.text
    parts = text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        await update.message.reply_text("Usage: /admin_complete <user_id> [YYYY-MM-DD]")
        return
    target_user = int(parts[1])
    d = parse_date_arg(text) or today_sgt()
    async with open_db() as db:
        await set_completed(db, target_user, d)
    await update.message.reply_text(f"User {target_user} marked completed on {d.isoformat()} ✅")

@require_admin
async def notify_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Usage:
      /notify_now
        - Run the normal "due" reminder check immediately (respects REMINDER_INTERVAL_DAYS).

      /notify_now all [--force]
        - Notify everyone currently *in their 100-day window* and not completed.
        - With --force: notify all verified users regardless of window/completion.

      /notify_now <user_id> [user_id ...] [--force]
        - Notify specific users. Without --force, only if in-window & not completed.
    """
    now = datetime.utcnow()
    text = (update.message.text or "").strip()
    parts = text.split()
    force = parts[-1] == "--force"
    ids = [p for p in parts[1:] if p != "--force"]

    async with open_db() as db:
        # No args: run standard due logic now
        if not ids:
            rows = await due_for_reminder(db, now)
            for r in rows:
                await send_single_reminder(context, db, r, now, mark=True)
            await update.message.reply_text(f"Triggered reminders for {len(rows)} user(s).")
            return

        # "all": send to verified users in-window (or all verified if --force)
        if len(ids) == 1 and ids[0].lower() == "all":
            count = 0
            async with db.execute("SELECT * FROM persons WHERE user_id IS NOT NULL") as cur:
                async for r in cur:
                    if not r["birthday"]:
                        continue
                    if not force:
                        bday = parse_iso_date(r["birthday"])
                        in_window, _, _ = within_100_day_window(bday, now.date())
                        if not in_window or r["completed_on"]:
                            continue
                    await send_single_reminder(context, db, r, now, mark=not force)
                    count += 1
            await update.message.reply_text(f"Sent reminders to {count} user(s){' (forced)' if force else ''}.")
            return

        # Specific IDs
        ok, skipped = 0, []
        for sid in ids:
            if not sid.isdigit():
                skipped.append((sid, "not a number"))
                continue
            uid = int(sid)
            async with db.execute("SELECT * FROM persons WHERE user_id=?", (uid,)) as cur:
                r = await cur.fetchone()
            if not r:
                skipped.append((sid, "not verified/linked"))
                continue
            if not force:
                bday = parse_iso_date(r["birthday"])
                in_window, _, _ = within_100_day_window(bday, now.date())
                if not in_window or r["completed_on"]:
                    skipped.append((sid, "not in window or already completed"))
                    continue
            await send_single_reminder(context, db, r, now, mark=not force)
            ok += 1

        msg = f"Sent to {ok} user(s){' (forced)' if force else ''}."
        if skipped:
            msg += "\nSkipped: " + ", ".join([f"{sid} ({why})" for sid, why in skipped])
        await update.message.reply_text(msg)

# =========================
# App bootstrap
# =========================
def build_app() -> Application:
    builder = ApplicationBuilder().token(SET.BOT_TOKEN)
    if _AIORateLimiter is not None:
        try:
            builder = builder.rate_limiter(_AIORateLimiter())
        except RuntimeError as e:
            log.warning("RateLimiter not available: %s", e)

    app = builder.build()

    # User handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))          # alias to show help
    app.add_handler(CommandHandler("verify", verify))
    app.add_handler(CommandHandler("setname", setname))
    app.add_handler(CommandHandler("summary", summary))
    app.add_handler(CommandHandler("complete", complete))

    # Admin handlers
    app.add_handler(CommandHandler("export", export_csv))
    app.add_handler(CommandHandler("import", import_csv))
    app.add_handler(CommandHandler("admin_complete", admin_complete))
    app.add_handler(CommandHandler("notify_now", notify_now))

    # Schedule reminders if JobQueue exists; else fallback loop in main()
    if app.job_queue is not None:
        from datetime import timedelta as _td
        app.job_queue.run_repeating(reminder_tick, interval=_td(hours=24), first=10)
    else:
        log.warning("JobQueue not available. Fallback reminder loop will be started in main().")

    return app

async def main():
    app = build_app()
    log.info("Starting bot…")
    await app.initialize()
    await app.start()
    # Start polling
    await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)

    # Fallback scheduler if JobQueue isn't available
    if app.job_queue is None:
        asyncio.create_task(reminder_loop_fallback(app, interval_hours=24))

    # Keep the process alive
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
