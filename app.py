
import csv
import io
import os
import sqlite3
from contextlib import closing
from datetime import date, datetime, timedelta, time
from typing import Iterable, Tuple, Optional, Any

import pytz
from dotenv import load_dotenv
from telegram import Update, InputFile
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

# ----------------------
# Env & Globals
# ----------------------
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN missing. Set it in env (.env)")

ADMIN_IDS = set()
for part in (os.getenv("ADMIN_IDS") or "").split(","):
    part = part.strip()
    if part:
        try:
            ADMIN_IDS.add(int(part))
        except ValueError:
            pass

TZ = os.getenv("TZ", "Asia/Singapore")
TZINFO = pytz.timezone(TZ)
DB_PATH = os.getenv("DB_PATH", "ippt.db")
REMINDER_HOUR = 9  # 09:00 local time
REMINDER_INTERVAL_DAYS = int(os.getenv("REMINDER_INTERVAL_DAYS", "10"))

# ----------------------
# DB Helpers
# ----------------------
def db_connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db():
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS personnel (
              personnel_id TEXT PRIMARY KEY,
              birthday     TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              telegram_id     INTEGER PRIMARY KEY,
              personnel_id    TEXT UNIQUE,
              verified_at     TEXT,
              completed_year  INTEGER,
              completed_at    TEXT,
              FOREIGN KEY (personnel_id) REFERENCES personnel(personnel_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS completions (
              id            INTEGER PRIMARY KEY AUTOINCREMENT,
              telegram_id   INTEGER NOT NULL,
              year          INTEGER NOT NULL,
              completed_at  TEXT NOT NULL
            )
            """
        )
        conn.commit()

# ----------------------
# Utils
# ----------------------
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def parse_date_strict(yyyy_mm_dd: str) -> date:
    return datetime.strptime(yyyy_mm_dd, "%Y-%m-%d").date()


def parse_birthday_any(val: Any) -> Optional[date]:
    """Accept date/datetime or string in strict YYYY-MM-DD (plus trims)."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, (int, float)):
        return None
    s = str(val).strip()
    if not s:
        return None
    s = s.replace("\u200b", "").replace("\ufeff", "")  # zero-width & BOM
    try:
        return parse_date_strict(s)
    except Exception:
        return None


def format_date(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def adjusted_birthday_for_year(bday: date, year: int) -> date:
    """Return this year's birthday date; if Feb 29 and not leap year, use Feb 28."""
    try:
        return date(year, bday.month, bday.day)
    except ValueError:
        if bday.month == 2 and bday.day == 29:
            return date(year, 2, 28)
        raise


def current_local_date() -> date:
    return datetime.now(TZINFO).date()


def today_in_window(bday: date, check: date | None = None) -> tuple[bool, date, date]:
    """Is 'check' within the 100-day window from this year's adjusted birthday?"""
    if check is None:
        check = current_local_date()
    start = adjusted_birthday_for_year(bday, check.year)
    end = start + timedelta(days=100)
    in_window = (start <= check <= end)
    return in_window, start, end


def next_reminder_date(start: date, end: date, today: date, interval: int) -> Optional[date]:
    """Compute the next reminder date on the interval grid [start, end]."""
    if today < start:
        return start
    if today > end:
        return None
    # Find the next multiple of 'interval' days from start (including today)
    days_since_start = (today - start).days
    remainder = days_since_start % interval
    if remainder == 0:
        next_date = today  # today is a reminder day
    else:
        next_date = today + timedelta(days=interval - remainder)
    return next_date if next_date <= end else None


def get_personnel_and_user(conn: sqlite3.Connection, telegram_id: int):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT u.telegram_id, u.personnel_id, u.verified_at, u.completed_year, u.completed_at,
               p.birthday
          FROM users u LEFT JOIN personnel p ON u.personnel_id = p.personnel_id
         WHERE u.telegram_id = ?
        """,
        (telegram_id,),
    )
    return cur.fetchone()

# ----------------------
# Command Handlers (User)
# ----------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        """
Hi! I'm the IPPT Reminder Bot. Here's what I can do:

• /verify <PERSONNEL_ID> <YYYY-MM-DD> — verify yourself
• /status — view your current year's window & status
• /complete — mark this year's IPPT as completed

Admins can use /admin_help for management commands.
        """.strip()
    )


async def verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    parts = msg.text.split()
    if len(parts) != 3:
        return await msg.reply_text(
            "Usage: /verify <PERSONNEL_ID> <YYYY-MM-DD>\nExample: /verify A12345 1995-07-14"
        )
    personnel_id = parts[1].strip()
    try:
        dob = parse_date_strict(parts[2].strip())
    except ValueError:
        return await msg.reply_text("Invalid date. Use YYYY-MM-DD (e.g., 1995-07-14).")

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT birthday FROM personnel WHERE personnel_id = ?",
            (personnel_id,),
        )
        row = cur.fetchone()
        if not row:
            return await msg.reply_text("No such personnel ID. Please check with your admin.")
        db_dob = parse_date_strict(row[0])
        if db_dob != dob:
            return await msg.reply_text(
                "ID and birthday do not match our records. Please try again or contact admin."
            )
        now = datetime.now(TZINFO).isoformat()
        cur.execute(
            """
            INSERT INTO users (telegram_id, personnel_id, verified_at)
            VALUES (?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                personnel_id = excluded.personnel_id,
                verified_at = excluded.verified_at
            """,
            (msg.from_user.id, personnel_id, now),
        )
        conn.commit()

    await msg.reply_text("✅ Verified successfully! Use /status to view your IPPT window.")


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    with closing(db_connect()) as conn:
        data = get_personnel_and_user(conn, msg.from_user.id)
    if not data or not data[1]:
        return await msg.reply_text("You're not verified yet. Use /verify first.")

    _, personnel_id, verified_at, completed_year, completed_at, birthday_str = data
    bday = parse_date_strict(birthday_str)
    today = current_local_date()
    in_window, start, end = today_in_window(bday, today)

    current_year = today.year
    done_this_year = (completed_year == current_year)

    # Compute next reminder on the 10-day grid
    nrd = next_reminder_date(start, end, today, REMINDER_INTERVAL_DAYS)

    lines = [
        f"Personnel ID: <code>{personnel_id}</code>",
        f"Birthday: <b>{format_date(bday)}</b>",
        f"This year's IPPT window: <b>{format_date(start)}</b> → <b>{format_date(end)}</b>",
        f"Today: <b>{format_date(today)}</b> — {'✅ In window' if in_window else '🕒 Outside window'}",
        f"Status {current_year}: {'✅ Completed' if done_this_year else '❌ Not completed'}",
        f"Reminder interval: every <b>{REMINDER_INTERVAL_DAYS}</b> days",
    ]
    if not done_this_year:
        if nrd:
            tag = " (today)" if nrd == today and in_window else ""
            lines.append(f"Next reminder: <b>{format_date(nrd)}</b>{tag}")
        else:
            lines.append(f"Next reminder: <i>none (window ends {format_date(end)})</i>")
    if completed_at:
        lines.append(f"Last completion recorded at: <code>{completed_at}</code>")

    await msg.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    today = current_local_date()
    now_iso = datetime.now(TZINFO).isoformat()
    with closing(db_connect()) as conn:
        data = get_personnel_and_user(conn, msg.from_user.id)
        if not data or not data[1]:
            return await msg.reply_text("You're not verified yet. Use /verify first.")
        _, personnel_id, _, completed_year, _, birthday_str = data
        bday = parse_date_strict(birthday_str)
        in_window, start, end = today_in_window(bday, today)
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
               SET completed_year = ?, completed_at = ?
             WHERE telegram_id = ?
            """,
            (today.year, now_iso, msg.from_user.id),
        )
        cur.execute(
            "INSERT INTO completions (telegram_id, year, completed_at) VALUES (?, ?, ?)",
            (msg.from_user.id, today.year, now_iso),
        )
        conn.commit()

    await msg.reply_text(
        f"✅ Recorded as completed for {today.year}. No more reminders this year.\n"
        f"(Window was {format_date(start)} → {format_date(end)}.)"
    )

# ----------------------
# Admin Commands
# ----------------------
async def admin_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not is_admin(msg.from_user.id):
        return await msg.reply_text("Admins only.")
    await msg.reply_text(
        f"""
Admin commands:

• /add_personnel <PERSONNEL_ID> <YYYY-MM-DD>
• /import_csv  — send a CSV/XLSX file (at least: personnel_id,birthday)
• /report      — get CSV of completed vs outstanding for current year
• /whoami      — check your numeric Telegram ID (for ADMIN_IDS)

Reminder interval is every {REMINDER_INTERVAL_DAYS} days. Change it via env var REMINDER_INTERVAL_DAYS.
        """.strip()
    )


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    await msg.reply_text(f"Your Telegram ID: <code>{msg.from_user.id}</code>", parse_mode=ParseMode.HTML)


async def add_personnel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not is_admin(msg.from_user.id):
        return await msg.reply_text("Admins only.")

    parts = msg.text.split()
    if len(parts) != 3:
        return await msg.reply_text(
            "Usage: /add_personnel <PERSONNEL_ID> <YYYY-MM-DD>\nExample: /add_personnel A12345 1995-07-14"
        )
    personnel_id = parts[1].strip()
    try:
        dob = parse_date_strict(parts[2].strip())
    except ValueError:
        return await msg.reply_text("Invalid date. Use YYYY-MM-DD.")

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO personnel (personnel_id, birthday) VALUES (?, ?)",
                (personnel_id, format_date(dob)),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            return await msg.reply_text("That PERSONNEL_ID already exists.")

    await msg.reply_text("✅ Added.")


async def import_csv_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not is_admin(msg.from_user.id):
        return await msg.reply_text("Admins only.")
    await msg.reply_text(
        "Send me a CSV or XLSX file. I only need columns: personnel_id, birthday (YYYY-MM-DD). Extra columns are ignored."
    )


def _normalize_header(name: str) -> str:
    return (name or "").strip().lstrip("\ufeff").replace("\u200b", "").lower()


def _extract_records_from_csv_bytes(b: bytes) -> Iterable[Tuple[str, str]]:
    text = b.decode("utf-8-sig")  # strips BOM automatically
    reader = csv.DictReader(io.StringIO(text))
    fieldmap = { _normalize_header(h): h for h in (reader.fieldnames or []) }
    pid_key = fieldmap.get("personnel_id")
    dob_key = fieldmap.get("birthday")
    if not pid_key or not dob_key:
        for h in (reader.fieldnames or []):
            nh = _normalize_header(h)
            if not pid_key and "personnel" in nh and "id" in nh:
                pid_key = h
            if not dob_key and ("birthday" in nh or nh in ("dob","dateofbirth")):
                dob_key = h
    if not pid_key or not dob_key:
        return []
    for row in reader:
        yield str(row.get(pid_key, "")).strip(), str(row.get(dob_key, "")).strip()


def _extract_records_from_xlsx_bytes(b: bytes) -> Iterable[Tuple[str, str]]:
    from openpyxl import load_workbook
    bio = io.BytesIO(b)
    wb = load_workbook(bio, data_only=True, read_only=True)
    ws = wb.worksheets[0]
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []
    headers = [ _normalize_header(str(h) if h is not None else "") for h in rows[0] ]

    def find_idx(keys):
        for i, h in enumerate(headers):
            if h in keys:
                return i
        return None

    pid_idx = find_idx({"personnel_id","personnel id","id"})
    dob_idx = find_idx({"birthday","dob","dateofbirth","date of birth"})
    if pid_idx is None or dob_idx is None:
        return []
    for r in rows[1:]:
        if r is None:
            continue
        pid = r[pid_idx] if pid_idx < len(r) else None
        dob = r[dob_idx] if dob_idx < len(r) else None
        pid_s = "" if pid is None else str(pid).strip()
        d = parse_birthday_any(dob)
        dob_s = format_date(d) if d else (str(dob).strip() if dob is not None else "")
        yield pid_s, dob_s


async def import_csv_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.document:
        return
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")

    doc = update.message.document
    name = (doc.file_name or "").lower()
    if not (name.endswith(".csv") or name.endswith(".xlsx")):
        return await update.message.reply_text("Please upload a .csv or .xlsx file.")

    tgfile = await doc.get_file()
    file_bytes = await tgfile.download_as_bytearray()

    try:
        if name.endswith(".csv"):
            records = _extract_records_from_csv_bytes(file_bytes)
        else:
            records = _extract_records_from_xlsx_bytes(file_bytes)
    except Exception as e:
        return await update.message.reply_text(f"Failed to read file: {e}")

    added, updated, skipped = 0, 0, 0
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for pid, bday in records:
            if not pid or not bday:
                skipped += 1
                continue
            try:
                dob = parse_date_strict(bday)
            except Exception:
                skipped += 1
                continue
            # UPSERT: insert or update birthday
            cur.execute("""
                INSERT INTO personnel (personnel_id, birthday)
                VALUES (?, ?)
                ON CONFLICT(personnel_id) DO UPDATE SET
                    birthday = excluded.birthday
            """, (pid, format_date(dob)))
            if cur.rowcount == 1 and cur.lastrowid is not None:
                # SQLite can't directly tell add vs update here; infer by checking change in existing row
                # Simpler approach: try an update first to categorize
                pass
        # For accurate Added/Updated, re-process with update-first approach:
    # Re-run categorization (accurate counts)
    added, updated, skipped = 0, 0, skipped
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        if name.endswith(".csv"):
            records = _extract_records_from_csv_bytes(file_bytes)
        else:
            records = _extract_records_from_xlsx_bytes(file_bytes)
        for pid, bday in records:
            if not pid or not bday:
                continue
            try:
                dob = parse_date_strict(bday)
            except Exception:
                continue
            cur.execute("UPDATE personnel SET birthday=? WHERE personnel_id=?", (format_date(dob), pid))
            if cur.rowcount:
                updated += 1
            else:
                cur.execute("INSERT INTO personnel (personnel_id, birthday) VALUES (?, ?)", (pid, format_date(dob)))
                added += 1
        conn.commit()

    await update.message.reply_text(f"Import done. ✅ Added: {added} | ✏️ Updated: {updated} | ⏭️ Skipped: {skipped}")


async def report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not is_admin(msg.from_user.id):
        return await msg.reply_text("Admins only.")

    today = current_local_date()
    this_year = today.year

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT p.personnel_id, p.birthday,
                   u.telegram_id, u.completed_year, u.completed_at
              FROM personnel p
              LEFT JOIN users u ON p.personnel_id = u.personnel_id
            """
        )
        rows = cur.fetchall()

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow([
        "personnel_id",
        "birthday",
        "verified",
        "window_start",
        "window_end",
        "completed_this_year",
        "completed_at",
    ])

    completed_count = 0
    outstanding_count = 0

    for pid, bday_str, telegram_id, completed_year, completed_at in rows:
        bday = parse_date_strict(bday_str)
        start = adjusted_birthday_for_year(bday, this_year)
        end = start + timedelta(days=100)
        done = (completed_year == this_year)
        if done:
            completed_count += 1
        else:
            outstanding_count += 1
        writer.writerow([
            pid,
            bday_str,
            "yes" if telegram_id else "no",
            format_date(start),
            format_date(end),
            "yes" if done else "no",
            completed_at or "",
        ])

    csv_bytes = out.getvalue().encode("utf-8")
    out.close()

    caption = (
        f"Report for {this_year}\n"
        f"Completed: {completed_count}\n"
        f"Outstanding: {outstanding_count}"
    )
    await msg.reply_document(
        document=InputFile(io.BytesIO(csv_bytes), filename=f"ippt_report_{this_year}.csv"),
        caption=caption,
    )

# ----------------------
# Daily Reminder Job
# ----------------------
async def daily_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    today = current_local_date()
    this_year = today.year

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.telegram_id, p.birthday, u.completed_year
              FROM users u
              JOIN personnel p ON u.personnel_id = p.personnel_id
            """
        )
        rows = cur.fetchall()

    for telegram_id, bday_str, completed_year in rows:
        try:
            bday = parse_date_strict(bday_str)
            in_window, start, end = today_in_window(bday, today)
            done = (completed_year == this_year)
            if in_window and not done:
                days_since_start = (today - start).days
                if days_since_start % REMINDER_INTERVAL_DAYS == 0:
                    remaining = (end - today).days
                    text = (
                        "⚠️ IPPT Reminder\n"
                        f"Your window is <b>{format_date(start)}</b> → <b>{format_date(end)}</b>.\n"
                        f"Days left: <b>{remaining}</b>.\n"
                        f"Interval: every <b>{REMINDER_INTERVAL_DAYS}</b> days.\n\n"
                        "Reply /complete once you've done it to stop reminders."
                    )
                    try:
                        await context.bot.send_message(chat_id=telegram_id, text=text, parse_mode=ParseMode.HTML)
                    except Exception:
                        pass
        except Exception:
            continue

# ----------------------
# App Bootstrap
# ----------------------
def setup_handlers(app: Application):
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("verify", verify))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("complete", complete))

    # Admin
    app.add_handler(CommandHandler("admin_help", admin_help))
    app.add_handler(CommandHandler("add_personnel", add_personnel))
    app.add_handler(CommandHandler("import_csv", import_csv_start))
    app.add_handler(CommandHandler("report", report))
    app.add_handler(CommandHandler("whoami", whoami))

    # CSV/XLSX upload after /import_csv
    app.add_handler(MessageHandler(
        filters.Document.FileExtension("csv") | filters.Document.FileExtension("xlsx"),
        import_csv_file
    ))


def schedule_jobs(app: Application):
    if not getattr(app, "job_queue", None):
        print("⚠️ JobQueue not available. Did you install python-telegram-bot[job-queue]?")
        return
    app.job_queue.run_daily(
        daily_reminder_job,
        time=time(hour=REMINDER_HOUR, minute=0, tzinfo=TZINFO),
        name="daily_reminder",
        days=(0, 1, 2, 3, 4, 5, 6),
    )


def main():
    init_db()
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .build()
    )
    setup_handlers(app)
    schedule_jobs(app)

    print("Bot is running…")
    app.run_polling()


if __name__ == "__main__":
    main()
