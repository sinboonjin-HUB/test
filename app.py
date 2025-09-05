import asyncio
import logging
import os
import re
import json
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
# Optional extras (guards)
# =========================
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
# Settings
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
# DB schema (SQLite)
# =========================
SCHEMA = """
-- Personnel roster (admin-managed)
CREATE TABLE IF NOT EXISTS persons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT UNIQUE NOT NULL,     -- e.g., 001A
    birthday TEXT NOT NULL,               -- YYYY-MM-DD (from roster)
    grp TEXT,                             -- optional grouping tag
    user_id INTEGER UNIQUE,               -- linked Telegram account
    chat_id INTEGER,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    display_name TEXT,
    verified_at TEXT,                     -- ISO datetime
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_persons_user_id ON persons(user_id);
CREATE INDEX IF NOT EXISTS idx_persons_external_id ON persons(external_id);
CREATE INDEX IF NOT EXISTS idx_persons_grp ON persons(grp);

-- Per-year completion
CREATE TABLE IF NOT EXISTS completions (
    external_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    completed_on TEXT,                    -- YYYY-MM-DD
    updated_at TEXT NOT NULL,
    PRIMARY KEY (external_id, year),
    FOREIGN KEY (external_id) REFERENCES persons(external_id) ON DELETE CASCADE
);

-- Per-year defer reason
CREATE TABLE IF NOT EXISTS defers (
    external_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    reason TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (external_id, year),
    FOREIGN KEY (external_id) REFERENCES persons(external_id) ON DELETE CASCADE
);

-- Per-year cycle-level reason (admin note)
CREATE TABLE IF NOT EXISTS cycle_reasons (
    external_id TEXT NOT NULL,
    year INTEGER NOT NULL,
    reason TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (external_id, year),
    FOREIGN KEY (external_id) REFERENCES persons(external_id) ON DELETE CASCADE
);

-- Admin audit log
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    actor_user_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    payload TEXT NOT NULL
);
"""

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
    # SG is UTC+8, no DST
    return (datetime.utcnow() + timedelta(hours=8)).date()

def within_100_day_window(bday: date, ref: date) -> Tuple[bool, date, date]:
    bday_this_year = bday.replace(year=ref.year)
    start = bday_this_year
    end = bday_this_year + timedelta(days=100)
    return (start <= ref <= end, start, end)

def window_for_year(bday: date, year: int) -> Tuple[date, date]:
    start = bday.replace(year=year)
    end = start + timedelta(days=100)
    return (start, end)

def parse_iso_date(s: str) -> date:
    return isoparse(s).date()

def parse_date_arg(text: str) -> Optional[date]:
    m = DATE_RE.search(text or "")
    if not m: return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None

def get_year_arg(parts: List[str], default_year: int) -> int:
    for p in parts:
        if p.isdigit() and len(p) == 4:
            y = int(p)
            if 1900 <= y <= 3000:
                return y
    return default_year

def is_admin(uid: int) -> bool:
    return uid in SET.ADMIN_IDS

def tokens_from_text(s: str) -> List[str]:
    # split by comma or whitespace, drop empties
    raw = re.split(r"[,\s]+", s.strip())
    return [x for x in raw if x]

async def audit(actor_uid: int, action: str, payload: dict):
    try:
        async with open_db() as db:
            await db.execute(
                "INSERT INTO audit_log (ts, actor_user_id, action, payload) VALUES (?,?,?,?)",
                (datetime.utcnow().isoformat(), actor_uid, action, json.dumps(payload))
            )
            await db.commit()
    except Exception as e:
        log.warning("Audit log failed: %s", e)

# =========================
# DB ops
# =========================
async def get_person_by_user(db: aiosqlite.Connection, user_id: int):
    async with db.execute("SELECT * FROM persons WHERE user_id=?", (user_id,)) as cur:
        return await cur.fetchone()

async def get_person_by_external(db: aiosqlite.Connection, external_id: str):
    async with db.execute("SELECT * FROM persons WHERE external_id=?", (external_id,)) as cur:
        return await cur.fetchone()

async def create_person(db: aiosqlite.Connection, external_id: str, bday: date, grp: Optional[str]):
    now = datetime.utcnow().isoformat()
    await db.execute(
        "INSERT INTO persons (external_id, birthday, grp, created_at, updated_at) VALUES (?,?,?,?,?)",
        (external_id, bday.isoformat(), grp, now, now)
    )
    await db.commit()

async def update_birthday_row(db: aiosqlite.Connection, external_id: str, bday: date):
    now = datetime.utcnow().isoformat()
    await db.execute(
        "UPDATE persons SET birthday=?, updated_at=? WHERE external_id=?",
        (bday.isoformat(), now, external_id)
    )
    await db.commit()

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
        SET user_id=?, chat_id=?, username=?, first_name=?, last_name=?, verified_at=?, updated_at=?
        WHERE external_id=?
        """,
        (user_id, chat_id, username, first_name, last_name, now, now, external_id),
    )
    await db.commit()

async def unlink_user(db: aiosqlite.Connection, external_id: Optional[str]=None, user_id: Optional[int]=None):
    now = datetime.utcnow().isoformat()
    if external_id:
        await db.execute("UPDATE persons SET user_id=NULL, chat_id=NULL, updated_at=? WHERE external_id=?",
                         (now, external_id))
    elif user_id:
        await db.execute("UPDATE persons SET user_id=NULL, chat_id=NULL, updated_at=? WHERE user_id=?",
                         (now, user_id))
    await db.commit()

async def remove_person(db: aiosqlite.Connection, external_id: str):
    await db.execute("DELETE FROM persons WHERE external_id=?", (external_id,))
    await db.commit()

# completions (per year)
async def set_completion_year(db: aiosqlite.Connection, external_id: str, year: int, when: date):
    now = datetime.utcnow().isoformat()
    await db.execute("""
        INSERT INTO completions (external_id, year, completed_on, updated_at)
        VALUES (?,?,?,?)
        ON CONFLICT(external_id, year) DO UPDATE SET
          completed_on=excluded.completed_on,
          updated_at=excluded.updated_at
    """, (external_id, year, when.isoformat(), now))
    await db.commit()

async def clear_completion_year(db: aiosqlite.Connection, external_id: str, year: int):
    now = datetime.utcnow().isoformat()
    await db.execute("""
        INSERT INTO completions (external_id, year, completed_on, updated_at)
        VALUES (?,?,NULL,?)
        ON CONFLICT(external_id, year) DO UPDATE SET
          completed_on=NULL,
          updated_at=excluded.updated_at
    """, (external_id, year, now))
    await db.commit()

async def get_completion_year(db: aiosqlite.Connection, external_id: str, year: int) -> Optional[str]:
    async with db.execute("SELECT completed_on FROM completions WHERE external_id=? AND year=?",
                          (external_id, year)) as cur:
        row = await cur.fetchone()
        return row["completed_on"] if row else None

# defers (per year)
async def set_defer_reason(db: aiosqlite.Connection, external_id: str, year: int, reason: str):
    now = datetime.utcnow().isoformat()
    await db.execute("""
        INSERT INTO defers (external_id, year, reason, updated_at)
        VALUES (?,?,?,?)
        ON CONFLICT(external_id, year) DO UPDATE SET
          reason=excluded.reason,
          updated_at=excluded.updated_at
    """, (external_id, year, reason.strip(), now))
    await db.commit()

async def clear_defer(db: aiosqlite.Connection, external_id: str, year: int):
    await db.execute("DELETE FROM defers WHERE external_id=? AND year=?", (external_id, year))
    await db.commit()

async def get_defer(db: aiosqlite.Connection, external_id: str, year: int) -> Optional[str]:
    async with db.execute("SELECT reason FROM defers WHERE external_id=? AND year=?",
                          (external_id, year)) as cur:
        row = await cur.fetchone()
        return row["reason"] if row else None

# cycle reasons (per year)
async def set_cycle_reason(db: aiosqlite.Connection, external_id: str, year: int, reason: str):
    now = datetime.utcnow().isoformat()
    await db.execute("""
        INSERT INTO cycle_reasons (external_id, year, reason, updated_at)
        VALUES (?,?,?,?)
        ON CONFLICT(external_id, year) DO UPDATE SET
          reason=excluded.reason,
          updated_at=excluded.updated_at
    """, (external_id, year, reason.strip(), now))
    await db.commit()

async def clear_cycle_reason(db: aiosqlite.Connection, external_id: str, year: int):
    await db.execute("DELETE FROM cycle_reasons WHERE external_id=? AND year=?", (external_id, year))
    await db.commit()

async def get_cycle_reason(db: aiosqlite.Connection, external_id: str, year: int) -> Optional[str]:
    async with db.execute("SELECT reason FROM cycle_reasons WHERE external_id=? AND year=?",
                          (external_id, year)) as cur:
        row = await cur.fetchone()
        return row["reason"] if row else None

# =========================
# Reminders
# =========================
def display_name(row) -> str:
    if row["display_name"]:
        return row["display_name"]
    parts = []
    if row["first_name"]: parts.append(row["first_name"])
    if row["last_name"]: parts.append(row["last_name"])
    if parts: return " ".join(parts)
    return row["username"] or row["external_id"]

def build_reminder_message(person_row, ref_dt: datetime, completed_on: Optional[str], defer_reason: Optional[str]) -> str:
    who = display_name(person_row)
    bday = parse_iso_date(person_row["birthday"])
    _, _, end = within_100_day_window(bday, ref_dt.date())
    days_left = (end - ref_dt.date()).days
    if completed_on:
        status_line = f"Status: ✅ Completed on {completed_on}\n"
    elif defer_reason:
        status_line = f"Status: ⏸️ Deferred – {defer_reason}\n"
    else:
        status_line = "Status: 🔔 Pending\n"
    return (
        f"👋 Hi {who}! Your 100-day IPPT window ends on {end.strftime('%Y-%m-%d')}.\n"
        f"Days left: {days_left}.\n{status_line}\n"
        f"Reply /complete to mark done, or /summary to see details."
    )

async def send_single_reminder(context: ContextTypes.DEFAULT_TYPE, db: aiosqlite.Connection, person_row, ref_dt: datetime, year: int):
    completed_on = await get_completion_year(db, person_row["external_id"], year)
    defer_reason = await get_defer(db, person_row["external_id"], year)
    msg = build_reminder_message(person_row, ref_dt, completed_on, defer_reason)
    try:
        await context.bot.send_message(chat_id=person_row["chat_id"], text=msg)
        await audit(0, "auto_remind", {"external_id": person_row["external_id"], "year": year})
    except Exception as e:
        log.warning("Send failed to %s: %s", person_row["chat_id"], e)

async def due_for_reminder(db: aiosqlite.Connection, ref_dt: datetime, year: int):
    """Verified users in-window, not completed, not deferred."""
    due = []
    async with db.execute("SELECT * FROM persons WHERE user_id IS NOT NULL") as cur:
        async for r in cur:
            if not r["birthday"]:
                continue
            bday = parse_iso_date(r["birthday"])
            ref = ref_dt.date()
            in_window, _, _ = within_100_day_window(bday, ref)
            if not in_window:
                continue
            if await get_completion_year(db, r["external_id"], year):
                continue
            if await get_defer(db, r["external_id"], year):
                continue
            due.append(r)
    return due

async def reminder_tick(context: ContextTypes.DEFAULT_TYPE):
    now = datetime.utcnow()
    year = now.year
    async with open_db() as db:
        rows = await due_for_reminder(db, now, year)
        for r in rows:
            await send_single_reminder(context, db, r, now, year)

# Fallback loop if JobQueue not available
async def reminder_loop_fallback(app: Application, interval_hours: int = 24):
    while True:
        now = datetime.utcnow()
        year = now.year
        try:
            async with open_db() as db:
                rows = await due_for_reminder(db, now, year)
                for r in rows:
                    completed_on = await get_completion_year(db, r["external_id"], year)
                    defer_reason = await get_defer(db, r["external_id"], year)
                    msg = build_reminder_message(r, now, completed_on, defer_reason)
                    try:
                        await app.bot.send_message(chat_id=r["chat_id"], text=msg)
                        await audit(0, "auto_remind", {"external_id": r["external_id"], "year": year})
                    except Exception as e:
                        log.warning("Send failed to %s: %s", r["chat_id"], e)
        except Exception as e:
            log.exception("reminder_loop_fallback tick failed: %s", e)
        await asyncio.sleep(interval_hours * 3600)

# =========================
# Guards
# =========================
def require_admin(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id if update.effective_user else 0
        if not is_admin(uid):
            await update.effective_message.reply_text("Admin only.")
            return
        return await func(update, context)
    return wrapper

def require_verified(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        async with open_db() as db:
            row = await get_person_by_user(db, update.effective_user.id)
        if not row:
            await update.effective_message.reply_text(
                "You are not verified yet. Use:\n"
                "<code>/verify &lt;ID&gt; &lt;YYYY-MM-DD&gt;</code>\n"
                "Example: <code>/verify 001A 1997-05-03</code>",
                parse_mode=ParseMode.HTML
            )
            return
        return await func(update, context)
    return wrapper

# =========================
# Help text (HTML to preserve underscores)
# =========================
HELP_TEXT = (
    "<b>IPPT Reminder Bot</b>\n\n"
    "<b>User:</b>\n"
    "• <code>/start</code> – get started\n"
    "• <code>/verify &lt;ID&gt; &lt;YYYY-MM-DD&gt;</code> – verify against roster\n"
    "• <code>/setname &lt;name&gt;</code> – set how I address you\n"
    "• <code>/summary</code> – see your status &amp; window\n"
    "• <code>/complete [YYYY-MM-DD]</code> – mark IPPT completed (today if no date)\n\n"
    "<b>Admin:</b>\n"
    "• <code>/whoami</code>\n"
    "• <code>/add_personnel &lt;ID&gt; &lt;YYYY-MM-DD&gt; [GROUP]</code>\n"
    "• <code>/update_birthday &lt;ID&gt; &lt;YYYY-MM-DD&gt;</code>\n"
    "• <code>/import_csv</code> (reply with CSV: <i>external_id,birthday[,group]</i>)\n"
    "• <code>/report [GROUP] [YEAR]</code>\n"
    "• <code>/export</code>\n"
    "• <code>/defer_reason  &lt;tokens&gt; [YEAR] -- &lt;reason&gt;</code>\n"
    "• <code>/defer_reset   &lt;tokens&gt; [YEAR]</code>\n"
    "• <code>/admin_complete   &lt;tokens&gt; [YEAR] [--date YYYY-MM-DD]</code>\n"
    "• <code>/admin_uncomplete &lt;tokens&gt; [YEAR]</code>\n"
    "• <code>/cycle_reason &lt;tokens&gt; [YEAR] -- &lt;reason&gt;</code>\n"
    "• <code>/cycle_reason_clear &lt;tokens&gt; [YEAR]</code>\n"
    "• <code>/unlink_user &lt;tokens&gt;</code>\n"
    "• <code>/remove_personnel &lt;ID[,ID,...]&gt;</code>\n"
    "• <code>/defer_audit [LIMIT]</code>\n"
    "• <code>/notify_now [all|&lt;ID&gt; ...] [--force]</code>\n"
)

# =========================
# User commands
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Welcome!\n\n"
        "Please verify yourself with:\n"
        "<code>/verify &lt;ID&gt; &lt;YYYY-MM-DD&gt;</code>\n"
        "Example: <code>/verify 001A 1997-05-03</code>\n\n" + HELP_TEXT,
        parse_mode=ParseMode.HTML
    )

async def verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    parts = (update.message.text or "").split()
    if len(parts) != 3:
        await update.message.reply_text(
            "Usage: <code>/verify &lt;ID&gt; &lt;YYYY-MM-DD&gt;</code>\nExample: <code>/verify 001A 1997-05-03</code>",
            parse_mode=ParseMode.HTML
        )
        return
    external_id = parts[1].strip()
    bday = parse_date_arg(parts[2])
    if not bday:
        await update.message.reply_text("Invalid date. Use YYYY-MM-DD.")
        return
    async with open_db() as db:
        person = await get_person_by_external(db, external_id)
        if not person or parse_iso_date(person["birthday"]) != bday:
            await update.message.reply_text("No matching record. Check your ID & birthday.")
            return
        if person["user_id"] and person["user_id"] != update.effective_user.id:
            await update.message.reply_text("This ID is already verified by another Telegram account.")
            return
        await link_user_to_person(
            db, external_id, update.effective_user.id, update.effective_chat.id,
            update.effective_user.username, update.effective_user.first_name, update.effective_user.last_name
        )
        await audit(update.effective_user.id, "verify", {"external_id": external_id})
    await update.message.reply_text("Verification successful ✅\nYou can now <code>/setname</code>, <code>/summary</code>, and <code>/complete</code>.", parse_mode=ParseMode.HTML)

@require_verified
async def setname(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    name = update.message.text.partition(" ")[2].strip()
    if not name:
        await update.message.reply_text("Usage: <code>/setname &lt;your preferred name&gt;</code>", parse_mode=ParseMode.HTML)
        return
    if len(name) > 50:
        await update.message.reply_text("Name too long. Keep within 50 characters.")
        return
    async with open_db() as db:
        now = datetime.utcnow().isoformat()
        await db.execute("UPDATE persons SET display_name=?, updated_at=? WHERE user_id=?",
                         (name, now, update.effective_user.id))
        await db.commit()
    await update.message.reply_text(f"Okay! I’ll call you <b>{name}</b> ✅", parse_mode=ParseMode.HTML)

@require_verified
async def summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with open_db() as db:
        row = await get_person_by_user(db, update.effective_user.id)
        bday = parse_iso_date(row["birthday"])
        ref = today_sgt()
        in_window, start_d, end_d = within_100_day_window(bday, ref)
        year = ref.year
        completed_on = await get_completion_year(db, row["external_id"], year)
        defer_reason = await get_defer(db, row["external_id"], year)
    status = (
        "✅ Completed" if completed_on else
        ("⏸️ Deferred" if defer_reason else ("🟡 In window" if in_window else "🕒 Out of window"))
    )
    who = display_name(row)
    completed = completed_on or "—"
    defer_txt = defer_reason or "—"
    txt = (
        f"<b>Your status</b>\n"
        f"ID: {row['external_id']}\n"
        f"Name: {who}\n"
        f"Birthday (roster): {bday.strftime('%Y-%m-%d')}\n"
        f"Window: {start_d.strftime('%Y-%m-%d')} → {end_d.strftime('%Y-%m-%d')}\n"
        f"Today: {ref.strftime('%Y-%m-%d')}\n"
        f"Completed on ({year}): {completed}\n"
        f"Defer reason ({year}): {defer_txt}\n"
        f"Status: {status}"
    )
    await update.message.reply_text(txt, parse_mode=ParseMode.HTML)

@require_verified
async def complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    d = parse_date_arg(update.message.text) or today_sgt()
    year = d.year
    async with open_db() as db:
        person = await get_person_by_user(db, update.effective_user.id)
        await set_completion_year(db, person["external_id"], year, d)
        await audit(update.effective_user.id, "complete_self", {"external_id": person["external_id"], "year": year, "date": d.isoformat()})
    await update.message.reply_text(f"Marked completed on {d.isoformat()} ✅")

# =========================
# Admin commands
# =========================
@require_admin
async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"Admin ✅\nYour Telegram ID: {update.effective_user.id}")

@require_admin
async def add_personnel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = (update.message.text or "").split()
    if len(parts) < 3:
        await update.message.reply_text("Usage: <code>/add_personnel &lt;ID&gt; &lt;YYYY-MM-DD&gt; [GROUP]</code>", parse_mode=ParseMode.HTML)
        return
    external_id = parts[1]
    bday = parse_date_arg(parts[2])
    grp = parts[3] if len(parts) >= 4 else None
    if not bday:
        await update.message.reply_text("Invalid date. Use YYYY-MM-DD.")
        return
    async with open_db() as db:
        if await get_person_by_external(db, external_id):
            await update.message.reply_text("That ID already exists.")
            return
        await create_person(db, external_id, bday, grp)
        await audit(update.effective_user.id, "add_personnel", {"external_id": external_id, "birthday": bday.isoformat(), "group": grp})
    await update.message.reply_text(f"Added {external_id} (birthday {bday.isoformat()}{', group '+grp if grp else ''}) ✅")

@require_admin
async def update_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = (update.message.text or "").split()
    if len(parts) != 3:
        await update.message.reply_text("Usage: <code>/update_birthday &lt;ID&gt; &lt;YYYY-MM-DD&gt;</code>", parse_mode=ParseMode.HTML)
        return
    external_id = parts[1]
    bday = parse_date_arg(parts[2])
    if not bday:
        await update.message.reply_text("Invalid date.")
        return
    async with open_db() as db:
        if not await get_person_by_external(db, external_id):
            await update.message.reply_text("No such ID.")
            return
        await update_birthday_row(db, external_id, bday)
        await audit(update.effective_user.id, "update_birthday", {"external_id": external_id, "birthday": bday.isoformat()})
    await update.message.reply_text(f"Updated {external_id} birthday → {bday.isoformat()} ✅")

@require_admin
async def import_csv(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Reply to a CSV with headers: external_id,birthday[,group]
    """
    if not update.message or not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text("Reply to a CSV file with <code>/import_csv</code>.\nHeaders: <i>external_id,birthday[,group]</i>", parse_mode=ParseMode.HTML)
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
            bday_s = (row.get("birthday") or "").strip()
            grp = (row.get("group") or "").strip() or None
            if not ext or not bday_s:
                continue
            try:
                bday = parse_iso_date(bday_s)
            except Exception:
                continue
            exists = await get_person_by_external(db, ext)
            if exists:
                await db.execute("UPDATE persons SET birthday=?, grp=?, updated_at=? WHERE external_id=?",
                                 (bday.isoformat(), grp, now, ext))
            else:
                await db.execute("INSERT INTO persons (external_id, birthday, grp, created_at, updated_at) VALUES (?,?,?,?,?)",
                                 (ext, bday.isoformat(), grp, now, now))
            count += 1
        await db.commit()
    await audit(update.effective_user.id, "import_csv", {"count": count})
    await update.message.reply_text(f"Imported/updated {count} personnel ✅")

@require_admin
async def report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /report [GROUP] [YEAR]
    Generates CSV: external_id,group,name,birthday,window_start,window_end,completed_on,defer_reason,cycle_reason,status
    """
    parts = (update.message.text or "").split()
    group = None
    year = today_sgt().year
    for p in parts[1:]:
        if p.isdigit() and len(p) == 4:
            year = int(p)
        else:
            group = p

    import csv
    from io import StringIO
    out = StringIO()
    w = csv.writer(out)
    w.writerow(["external_id","group","name","birthday","window_start","window_end","completed_on","defer_reason","cycle_reason","status"])

    async with open_db() as db:
        q = "SELECT * FROM persons"
        params = []
        if group:
            q += " WHERE grp=?"
            params.append(group)
        async with db.execute(q, params) as cur:
            async for r in cur:
                bday = parse_iso_date(r["birthday"])
                win_start, win_end = window_for_year(bday, year)
                comp = await get_completion_year(db, r["external_id"], year)
                defer = await get_defer(db, r["external_id"], year)
                cyc = await get_cycle_reason(db, r["external_id"], year)
                status = "COMPLETED" if comp else ("DEFERRED" if defer else "DUE")
                w.writerow([
                    r["external_id"], r["grp"] or "", display_name(r), r["birthday"],
                    win_start.isoformat(), win_end.isoformat(), comp or "", defer or "", cyc or "", status
                ])
    out.seek(0)
    await update.message.reply_document(
        document=out.getvalue().encode("utf-8"),
        filename=f"report_{group or 'ALL'}_{year}.csv",
        caption=f"Report ({group or 'ALL'}, {year})"
    )

@require_admin
async def export_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /export → ZIP with: persons.csv, completions.csv, defers.csv, cycle_reasons.csv
    """
    import csv
    import io
    import zipfile

    persons_buf = io.StringIO()
    comp_buf = io.StringIO()
    defer_buf = io.StringIO()
    cycle_buf = io.StringIO()

    async with open_db() as db:
        # persons
        persons_cols = ["external_id","birthday","grp","user_id","chat_id","username","first_name","last_name","display_name","verified_at","created_at","updated_at"]
        pw = csv.writer(persons_buf)
        pw.writerow(persons_cols)
        async with db.execute(f"SELECT {', '.join(persons_cols)} FROM persons ORDER BY external_id ASC") as cur:
            async for r in cur:
                pw.writerow([r[c] if r[c] is not None else "" for c in persons_cols])

        # completions
        comp_cols = ["external_id","year","completed_on","updated_at"]
        cw = csv.writer(comp_buf)
        cw.writerow(comp_cols)
        async with db.execute(f"SELECT {', '.join(comp_cols)} FROM completions ORDER BY external_id, year") as cur:
            async for r in cur:
                cw.writerow([r[c] if r[c] is not None else "" for c in comp_cols])

        # defers
        defer_cols = ["external_id","year","reason","updated_at"]
        dw = csv.writer(defer_buf)
        dw.writerow(defer_cols)
        async with db.execute(f"SELECT {', '.join(defer_cols)} FROM defers ORDER BY external_id, year") as cur:
            async for r in cur:
                dw.writerow([r[c] if r[c] is not None else "" for c in defer_cols])

        # cycle reasons
        cycle_cols = ["external_id","year","reason","updated_at"]
        cyw = csv.writer(cycle_buf)
        cyw.writerow(cycle_cols)
        async with db.execute(f"SELECT {', '.join(cycle_cols)} FROM cycle_reasons ORDER BY external_id, year") as cur:
            async for r in cur:
                cyw.writerow([r[c] if r[c] is not None else "" for c in cycle_cols])

    # pack ZIP
    persons_buf.seek(0); comp_buf.seek(0); defer_buf.seek(0); cycle_buf.seek(0)
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("persons.csv", persons_buf.getvalue())
        zf.writestr("completions.csv", comp_buf.getvalue())
        zf.writestr("defers.csv", defer_buf.getvalue())
        zf.writestr("cycle_reasons.csv", cycle_buf.getvalue())
    zip_bytes.seek(0)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    await update.message.reply_document(
        document=zip_bytes.getvalue(),
        filename=f"ippt_export_{ts}.zip",
        caption="Exported data"
    )

def _split_reason(text: str) -> Tuple[str, Optional[str]]:
    if " -- " in text:
        before, _, after = text.partition(" -- ")
        return before.strip(), after.strip()
    return text.strip(), None

def _targets_and_year(text: str, default_year: int) -> Tuple[List[str], int]:
    before, _ = _split_reason(text)
    parts = tokens_from_text(before)
    year = get_year_arg(parts, default_year)
    parts = [p for p in parts if not (p.isdigit() and len(p) == 4 and int(p) == year)]
    return parts, year

@require_admin
async def defer_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    default_year = today_sgt().year
    text = (update.message.text or "").removeprefix("/defer_reason").strip()
    before, reason = _split_reason(text)
    if not reason:
        await update.message.reply_text("Usage: <code>/defer_reason &lt;tokens&gt; [YEAR] -- &lt;reason&gt;</code>", parse_mode=ParseMode.HTML)
        return
    tokens, year = _targets_and_year(text, default_year)
    if not tokens:
        await update.message.reply_text("No targets found.")
        return
    async with open_db() as db:
        for tok in tokens:
            if not await get_person_by_external(db, tok):
                continue
            await set_defer_reason(db, tok, year, reason)
            await audit(update.effective_user.id, "defer_reason", {"external_id": tok, "year": year, "reason": reason})
    await update.message.reply_text(f"Set defer reason for {len(tokens)} id(s) in {year} ✅")

@require_admin
async def defer_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    default_year = today_sgt().year
    text = (update.message.text or "").removeprefix("/defer_reset").strip()
    tokens, year = _targets_and_year(text, default_year)
    if not tokens:
        await update.message.reply_text("Usage: <code>/defer_reset &lt;tokens&gt; [YEAR]</code>", parse_mode=ParseMode.HTML)
        return
    async with open_db() as db:
        for tok in tokens:
            if not await get_person_by_external(db, tok):
                continue
            await clear_defer(db, tok, year)
            await audit(update.effective_user.id, "defer_reset", {"external_id": tok, "year": year})
    await update.message.reply_text(f"Cleared defer reason for {len(tokens)} id(s) in {year} ✅")

@require_admin
async def admin_complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /admin_complete <tokens> [YEAR] [--date YYYY-MM-DD]
    """
    text = (update.message.text or "").removeprefix("/admin_complete").strip()
    parts = tokens_from_text(text)
    default_year = today_sgt().year
    year = get_year_arg(parts, default_year)
    # extract date
    m = re.search(r"--date\s+(\d{4}-\d{2}-\d{2})", text)
    d = parse_iso_date(m.group(1)) if m else (parse_date_arg(text) or today_sgt())
    ids = [p for p in parts if not (p.isdigit() and len(p)==4 and int(p)==year) and p != "--date" and not DATE_RE.fullmatch(p)]
    if not ids:
        await update.message.reply_text("Usage: <code>/admin_complete &lt;tokens&gt; [YEAR] [--date YYYY-MM-DD]</code>", parse_mode=ParseMode.HTML)
        return
    async with open_db() as db:
        done = 0
        for tok in ids:
            if not await get_person_by_external(db, tok):
                continue
            await set_completion_year(db, tok, year, d)
            await audit(update.effective_user.id, "admin_complete", {"external_id": tok, "year": year, "date": d.isoformat()})
            done += 1
    await update.message.reply_text(f"Completed {done} id(s) for {year} on {d.isoformat()} ✅")

@require_admin
async def admin_uncomplete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").removeprefix("/admin_uncomplete").strip()
    parts = tokens_from_text(text)
    default_year = today_sgt().year
    year = get_year_arg(parts, default_year)
    ids = [p for p in parts if not (p.isdigit() and len(p)==4 and int(p)==year)]
    if not ids:
        await update.message.reply_text("Usage: <code>/admin_uncomplete &lt;tokens&gt; [YEAR]</code>", parse_mode=ParseMode.HTML)
        return
    async with open_db() as db:
        done = 0
        for tok in ids:
            if not await get_person_by_external(db, tok):
                continue
            await clear_completion_year(db, tok, year)
            await audit(update.effective_user.id, "admin_uncomplete", {"external_id": tok, "year": year})
            done += 1
    await update.message.reply_text(f"Cleared completion for {done} id(s) in {year} ✅")

@require_admin
async def cycle_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    default_year = today_sgt().year
    text = (update.message.text or "").removeprefix("/cycle_reason").strip()
    before, reason = _split_reason(text)
    if not reason:
        await update.message.reply_text("Usage: <code>/cycle_reason &lt;tokens&gt; [YEAR] -- &lt;reason&gt;</code>", parse_mode=ParseMode.HTML)
        return
    tokens, year = _targets_and_year(text, default_year)
    if not tokens:
        await update.message.reply_text("No targets found.")
        return
    async with open_db() as db:
        for tok in tokens:
            if not await get_person_by_external(db, tok):
                continue
            await set_cycle_reason(db, tok, year, reason)
            await audit(update.effective_user.id, "cycle_reason", {"external_id": tok, "year": year, "reason": reason})
    await update.message.reply_text(f"Set cycle reason for {len(tokens)} id(s) in {year} ✅")

@require_admin
async def cycle_reason_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").removeprefix("/cycle_reason_clear").strip()
    parts = tokens_from_text(text)
    default_year = today_sgt().year
    year = get_year_arg(parts, default_year)
    ids = [p for p in parts if not (p.isdigit() and len(p)==4 and int(p)==year)]
    if not ids:
        await update.message.reply_text("Usage: <code>/cycle_reason_clear &lt;tokens&gt; [YEAR]</code>", parse_mode=ParseMode.HTML)
        return
    async with open_db() as db:
        for tok in ids:
            if not await get_person_by_external(db, tok):
                continue
            await clear_cycle_reason(db, tok, year)
            await audit(update.effective_user.id, "cycle_reason_clear", {"external_id": tok, "year": year})
    await update.message.reply_text(f"Cleared cycle reason for {len(ids)} id(s) in {year} ✅")

@require_admin
async def unlink_user_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").removeprefix("/unlink_user").strip()
    tokens = tokens_from_text(text)
    if not tokens:
        await update.message.reply_text("Usage: <code>/unlink_user &lt;external_id|user_id&gt; [more ...]</code>", parse_mode=ParseMode.HTML)
        return
    async with open_db() as db:
        done = 0
        for tok in tokens:
            if tok.isdigit():
                await unlink_user(db, user_id=int(tok)); done += 1
                await audit(update.effective_user.id, "unlink_user_by_telegram", {"user_id": int(tok)})
            else:
                if await get_person_by_external(db, tok):
                    await unlink_user(db, external_id=tok); done += 1
                    await audit(update.effective_user.id, "unlink_user_by_external", {"external_id": tok})
    await update.message.reply_text(f"Unlinked {done} record(s) ✅")

@require_admin
async def remove_personnel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").removeprefix("/remove_personnel").strip()
    ids = tokens_from_text(text)
    if not ids:
        await update.message.reply_text("Usage: <code>/remove_personnel &lt;ID[,ID,...]&gt;</code>", parse_mode=ParseMode.HTML)
        return
    async with open_db() as db:
        done = 0
        for ext in ids:
            if await get_person_by_external(db, ext):
                await remove_person(db, ext); done += 1
                await audit(update.effective_user.id, "remove_personnel", {"external_id": ext})
    await update.message.reply_text(f"Removed {done} personnel record(s) ✅")

@require_admin
async def defer_audit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = (update.message.text or "").split()
    limit = 30
    if len(parts) > 1 and parts[1].isdigit():
        limit = min(200, max(1, int(parts[1])))
    rows = []
    async with open_db() as db:
        async with db.execute("SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)) as cur:
            rows = await cur.fetchall()
    lines = ["Recent audit log:"]
    for r in rows:
        lines.append(f"- {r['ts']} | {r['actor_user_id']} | {r['action']} | {r['payload']}")
    # Ensure we don't blow message size limits
    msg = "\n".join(lines)
    if len(msg) > 3900:
        msg = msg[:3900] + "\n… (truncated)"
    await update.message.reply_text(msg)

@require_admin
async def notify_now(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.utcnow()
    default_year = now.year
    text = (update.message.text or "").removeprefix("/notify_now").strip()
    force = text.endswith("--force")
    text = text.removesuffix("--force").strip()
    ids = tokens_from_text(text)
    async with open_db() as db:
        if not ids:
            rows = await due_for_reminder(db, now, default_year)
            for r in rows:
                await send_single_reminder(context, db, r, now, default_year)
            await update.message.reply_text(f"Triggered reminders for {len(rows)} user(s).")
            return
        if len(ids) == 1 and ids[0].lower() == "all":
            count = 0
            async with db.execute("SELECT * FROM persons WHERE user_id IS NOT NULL") as cur:
                async for r in cur:
                    if not force:
                        bday = parse_iso_date(r["birthday"])
                        in_window, _, _ = within_100_day_window(bday, now.date())
                        if not in_window:
                            continue
                        if await get_completion_year(db, r["external_id"], default_year):
                            continue
                    await send_single_reminder(context, db, r, now, default_year)
                    count += 1
            await update.message.reply_text(f"Sent reminders to {count} user(s){' (forced)' if force else ''}.")
            return
        ok, skipped = 0, []
        for tok in ids:
            r = await get_person_by_external(db, tok)
            if not r:
                skipped.append((tok, "no such ID"))
                continue
            if not force:
                bday = parse_iso_date(r["birthday"])
                in_window, _, _ = within_100_day_window(bday, now.date())
                if not in_window or await get_completion_year(db, r["external_id"], default_year):
                    skipped.append((tok, "not in window or already completed"))
                    continue
            await send_single_reminder(context, db, r, now, default_year)
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

    # User
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", start))
    app.add_handler(CommandHandler("verify", verify))
    app.add_handler(CommandHandler("setname", setname))
    app.add_handler(CommandHandler("summary", summary))
    app.add_handler(CommandHandler("complete", complete))

    # Admin
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("add_personnel", add_personnel))
    app.add_handler(CommandHandler("update_birthday", update_birthday))
    app.add_handler(CommandHandler("import_csv", import_csv))
    app.add_handler(CommandHandler("report", report))
    app.add_handler(CommandHandler("export", export_all))
    app.add_handler(CommandHandler("defer_reason", defer_reason))
    app.add_handler(CommandHandler("defer_reset", defer_reset))
    app.add_handler(CommandHandler("admin_complete", admin_complete))
    app.add_handler(CommandHandler("admin_uncomplete", admin_uncomplete))
    app.add_handler(CommandHandler("cycle_reason", cycle_reason))
    app.add_handler(CommandHandler("cycle_reason_clear", cycle_reason_clear))
    app.add_handler(CommandHandler("unlink_user", unlink_user_cmd))
    app.add_handler(CommandHandler("remove_personnel", remove_personnel))
    app.add_handler(CommandHandler("defer_audit", defer_audit))
    app.add_handler(CommandHandler("notify_now", notify_now))

    # JobQueue if present; otherwise fallback loop will start in main()
    if app.job_queue is not None:
        from datetime import timedelta as _td
        app.job_queue.run_repeating(reminder_tick, interval=_td(hours=24), first=10)
    else:
        log.warning("JobQueue not available. Fallback reminder loop will be started in main().")

    return app

async def main():
    app = build_app()
    log.info("Starting bot…")

    # Start fallback reminder loop if JobQueue is missing
    if app.job_queue is None:
        asyncio.create_task(reminder_loop_fallback(app, interval_hours=24))

    # Run polling (handles initialize/start/stop internally)
    await app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
