
import os
import io
import re
import csv
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time
from zoneinfo import ZoneInfo

from telegram import Update, InputFile
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
)

# ----------------------
# Config
# ----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x.strip().isdigit()}
DB_PATH = os.getenv("DB_PATH", "ippt.db")
TZ_NAME = os.getenv("TZ", "Asia/Singapore")
TZINFO = ZoneInfo(TZ_NAME)
WINDOW_DAYS = 100
REMINDER_INTERVAL_DAYS = int(os.getenv("REMINDER_INTERVAL_DAYS", "10"))

# ----------------------
# DB helpers
# ----------------------
def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    with conn:
        conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        # Core tables
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS personnel (
              personnel_id TEXT PRIMARY KEY,
              birthday     TEXT NOT NULL,
              group_name   TEXT
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
        # Preferred (new) deferments table (personnel_id-based)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS deferments (
              id           INTEGER PRIMARY KEY AUTOINCREMENT,
              personnel_id TEXT NOT NULL,
              year         INTEGER NOT NULL,
              reason       TEXT,
              status       TEXT CHECK (status IN ('approved')) DEFAULT 'approved',
              created_at   TEXT NOT NULL,
              UNIQUE (personnel_id, year),
              FOREIGN KEY (personnel_id) REFERENCES personnel(personnel_id)
            )
            """
        )
        # Migration: telegram_id-based -> personnel_id-based (if needed)
        cur.execute("PRAGMA table_info('deferments')")
        cols = [r[1].lower() for r in cur.fetchall()]
        if 'telegram_id' in cols and 'personnel_id' not in cols:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS deferment_migration_audit (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  old_telegram_id INTEGER,
                  new_personnel_id TEXT,
                  year INTEGER,
                  reason TEXT,
                  status TEXT,
                  created_at TEXT,
                  migrated_at TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS deferments_new (
                  id           INTEGER PRIMARY KEY AUTOINCREMENT,
                  personnel_id TEXT NOT NULL,
                  year         INTEGER NOT NULL,
                  reason       TEXT,
                  status       TEXT CHECK (status IN ('approved')) DEFAULT 'approved',
                  created_at   TEXT NOT NULL,
                  UNIQUE (personnel_id, year),
                  FOREIGN KEY (personnel_id) REFERENCES personnel(personnel_id)
                )
                """
            )
            cur.execute("SELECT telegram_id, year, reason, status, created_at FROM deferments")
            for tid, yr, reason, status, created_at in cur.fetchall():
                cur.execute("SELECT personnel_id FROM users WHERE telegram_id=?", (tid,))
                r = cur.fetchone()
                if not r or not r[0]:
                    continue
                pid = r[0]
                try:
                    cur.execute(
                        "INSERT OR IGNORE INTO deferments_new (personnel_id, year, reason, status, created_at) VALUES (?, ?, ?, 'approved', ?)",
                        (pid, yr, reason, created_at)
                    )
                    cur.execute(
                        "INSERT INTO deferment_migration_audit (old_telegram_id, new_personnel_id, year, reason, status, created_at, migrated_at) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
                        (tid, pid, yr, reason, status, created_at)
                    )
                except Exception:
                    pass
            cur.execute("DROP TABLE deferments")
            cur.execute("ALTER TABLE deferments_new RENAME TO deferments")
        conn.commit()

# ----------------------
# Utils
# ----------------------
def format_date(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def parse_date_strict(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def adjusted_birthday_for_year(bday: date, year: int) -> date:
    try:
        return date(year, bday.month, bday.day)
    except ValueError:
        if bday.month == 2 and bday.day == 29:
            return date(year, 2, 28)
        raise

def current_local_date() -> date:
    return datetime.now(TZINFO).date()

def today_in_window(bday: date, today: date):
    """Return (in_window, start, end) for today's 100-day window based on bday."""
    start = adjusted_birthday_for_year(bday, today.year)
    end = start + timedelta(days=WINDOW_DAYS)
    if start <= today <= end:
        return True, start, end
    # If today before this year's birthday, check previous year's window
    prev_start = adjusted_birthday_for_year(bday, today.year - 1)
    prev_end = prev_start + timedelta(days=WINDOW_DAYS)
    if prev_start <= today <= prev_end:
        return True, prev_start, prev_end
    # Otherwise, it's either before window (this year's upcoming) or after (next year's upcoming)
    return False, start, end

def window_for_date(bday: date, on: date):
    """Return (start, end) for the 100-day window that contains the 'on' date (or nearest)."""
    start = adjusted_birthday_for_year(bday, on.year)
    end = start + timedelta(days=WINDOW_DAYS)
    if start <= on <= end:
        return start, end
    prev_start = adjusted_birthday_for_year(bday, on.year - 1)
    prev_end = prev_start + timedelta(days=WINDOW_DAYS)
    if prev_start <= on <= prev_end:
        return prev_start, prev_end
    next_start = adjusted_birthday_for_year(bday, on.year + 1)
    next_end = next_start + timedelta(days=WINDOW_DAYS)
    return next_start, next_end

def iso_from_local_date(d: date, hour: int = 9, minute: int = 0) -> str:
    dt = datetime.combine(d, time(hour=hour, minute=minute, tzinfo=TZINFO))
    return dt.isoformat()

def is_admin(tid: int) -> bool:
    return tid in ADMIN_IDS

# ----------------------
# Data helpers
# ----------------------
def get_personnel_and_user(conn: sqlite3.Connection, telegram_id: int):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT u.telegram_id, u.personnel_id, u.verified_at, u.completed_year, u.completed_at,
               p.birthday, p.group_name
          FROM users u
          JOIN personnel p ON u.personnel_id = p.personnel_id
         WHERE u.telegram_id = ?
        """,
        (telegram_id,),
    )
    r = cur.fetchone()
    if not r:
        return None
    return (
        r["telegram_id"], r["personnel_id"], r["verified_at"], r["completed_year"], r["completed_at"],
        r["birthday"], r["group_name"]
    )

def get_deferment_by_pid(conn: sqlite3.Connection, personnel_id: str, year: int):
    cur = conn.cursor()
    cur.execute("SELECT reason, status FROM deferments WHERE personnel_id=? AND year=?", (personnel_id, year))
    return cur.fetchone()

# ----------------------
# Command handlers
# ----------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lines = [
        "Welcome to the IPPT Reminder Bot 👋",
        "",
        "User commands:",
        "• /verify <PERSONNEL_ID> <YYYY-MM-DD>",
        "• /status",
        "• /complete [YYYY-MM-DD] — mark this window's IPPT as completed (date optional, must be within current window)",
        "• /uncomplete — clear your completion for this window",
        "",
        "Admins: /admin_help",
    ]
    await update.message.reply_text("\n".join(lines))

async def admin_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    lines = [
        "Admin commands:",
        "• /add_personnel <ID> <YYYY-MM-DD> [GROUP]",
        "• /update_birthday <PERSONNEL_ID> <YYYY-MM-DD>",
        "• /import_csv (then upload CSV/XLSX with personnel_id,birthday[,group])",
        "• /report — Excel (All + per-group). Red = not completed & no active deferment. Includes days_left/days_overdue.",
        "• /report_group <GROUP> — single-group Excel (use 'No Group' for empty).",
        "• /defer_reason  <tokens> [WINDOW_START_YEAR] -- <reason text>  (user cannot set reasons themselves)",
        "• /defer_reset   <tokens> [WINDOW_START_YEAR] — clear deferment for that window",
        "• /admin_complete <tokens> [WINDOW_START_YEAR] [--date YYYY-MM-DD]  (overrides existing date for that window)",
        "• /admin_uncomplete <tokens> [WINDOW_START_YEAR]",
        "• /unlink_user <tokens>",
        "• /remove_personnel <ID or comma-list>",
        "• /defer_audit — export a CSV of migrated/current deferments",
        "• /whoami",
    ]
    await update.message.reply_text("\n".join(lines))

async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tid = update.message.from_user.id
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT personnel_id FROM users WHERE telegram_id=?", (tid,))
        r = cur.fetchone()
    pid = r[0] if r and r[0] else "(not linked)"
    await update.message.reply_text(f"Telegram ID: {tid}\nLinked personnel_id: {pid}")

async def verify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = update.message.text.split()
    if len(parts) != 3:
        return await update.message.reply_text("Usage: /verify <PERSONNEL_ID> <YYYY-MM-DD>")
    pid = parts[1].strip()
    try:
        dob = parse_date_strict(parts[2].strip())
    except Exception:
        return await update.message.reply_text("Invalid date. Use YYYY-MM-DD.")
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT birthday FROM personnel WHERE personnel_id=?", (pid,))
        r = cur.fetchone()
        if not r:
            return await update.message.reply_text("No such PERSONNEL_ID. Ask admin to add you.")
        if r[0] != format_date(dob):
            return await update.message.reply_text("Birthday does not match our records.")
        cur.execute(
            """
            INSERT INTO users (telegram_id, personnel_id, verified_at)
            VALUES (?, ?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET
              personnel_id=excluded.personnel_id,
              verified_at=excluded.verified_at
            """,
            (update.message.from_user.id, pid, datetime.now(TZINFO).isoformat())
        )
        conn.commit()
    await update.message.reply_text("✅ Verified and linked. Use /status.")

async def add_personnel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    parts = update.message.text.split()
    if len(parts) < 3:
        return await update.message.reply_text("Usage: /add_personnel <ID> <YYYY-MM-DD> [GROUP]")
    pid = parts[1].strip()
    try:
        dob = parse_date_strict(parts[2].strip())
    except Exception:
        return await update.message.reply_text("Invalid date. Use YYYY-MM-DD.")
    group = " ".join(parts[3:]).strip() if len(parts) > 3 else None
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO personnel (personnel_id, birthday, group_name) VALUES (?, ?, ?)",
            (pid, format_date(dob), group)
        )
        conn.commit()
    await update.message.reply_text(f"✅ Added/updated {pid}.")

async def update_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    parts = update.message.text.split()
    if len(parts) != 3:
        return await update.message.reply_text("Usage: /update_birthday <PERSONNEL_ID> <YYYY-MM-DD>")
    pid = parts[1].strip()
    try:
        dob = parse_date_strict(parts[2].strip())
    except Exception:
        return await update.message.reply_text("Invalid date. Use YYYY-MM-DD.")
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("UPDATE personnel SET birthday=? WHERE personnel_id=?", (format_date(dob), pid))
        if cur.rowcount == 0:
            return await update.message.reply_text("No such PERSONNEL_ID.")
        conn.commit()
    await update.message.reply_text(f"✅ Updated {pid} birthday to {format_date(dob)}.")

# ---- import CSV/XLSX ----
async def import_csv_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    context.user_data["awaiting_import"] = True
    await update.message.reply_text("Please upload your CSV/XLSX file now. Columns: personnel_id,birthday[,group].")

async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("awaiting_import"):
        return
    doc = update.message.document
    if not doc or not doc.file_name:
        return
    lower = doc.file_name.lower()
    if not (lower.endswith(".csv") or lower.endswith(".xlsx")):
        return await update.message.reply_text("Unsupported file type. Please upload .csv or .xlsx.")
    file = await doc.get_file()
    tmp_path = os.path.join("/tmp", doc.file_unique_id + ("_xlsx" if lower.endswith(".xlsx") else "_csv"))
    await file.download_to_drive(tmp_path)
    count = 0
    if lower.endswith(".csv"):
        with open(tmp_path, "rb") as f:
            data = f.read()
        # Strip BOM
        if data.startswith(b"\xef\xbb\xbf"):
            data = data[3:]
        import io as _io
        reader = csv.DictReader(_io.StringIO(data.decode("utf-8", errors="replace")))
        for row in reader:
            pid = (row.get("personnel_id") or row.get("id") or "").strip()
            bday = (row.get("birthday") or row.get("dob") or "").strip()
            group = (row.get("group") or row.get("group_name") or "").strip() or None
            if not pid or not bday:
                continue
            try:
                dob = parse_date_strict(bday)
            except Exception:
                continue
            with closing(db_connect()) as conn:
                cur = conn.cursor()
                cur.execute("INSERT OR REPLACE INTO personnel (personnel_id, birthday, group_name) VALUES (?, ?, ?)",
                            (pid, format_date(dob), group))
                conn.commit()
                count += 1
    else:
        # XLSX via openpyxl
        from openpyxl import load_workbook
        wb = load_workbook(tmp_path)
        ws = wb.active
        # Find headers
        headers = [str(c.value).strip().lower() if c.value is not None else "" for c in next(ws.iter_rows(min_row=1, max_row=1))]
        # Map
        def get_col(name_opts):
            for idx, h in enumerate(headers):
                if h in name_opts:
                    return idx
            return None
        pid_col = get_col({"personnel_id", "id"})
        bday_col = get_col({"birthday", "dob"})
        group_col = get_col({"group", "group_name"})
        if pid_col is None or bday_col is None:
            return await update.message.reply_text("XLSX must include 'personnel_id' and 'birthday' headers.")
        for row in ws.iter_rows(min_row=2):
            pid = str(row[pid_col].value).strip() if row[pid_col].value is not None else ""
            bday = str(row[bday_col].value).strip() if row[bday_col].value is not None else ""
            group = str(row[group_col].value).strip() if (group_col is not None and row[group_col].value is not None) else None
            if not pid or not bday:
                continue
            try:
                # Excel might give datetime
                if isinstance(row[bday_col].value, datetime):
                    dob = row[bday_col].value.date()
                else:
                    dob = parse_date_strict(bday)
            except Exception:
                continue
            with closing(db_connect()) as conn:
                cur = conn.cursor()
                cur.execute("INSERT OR REPLACE INTO personnel (personnel_id, birthday, group_name) VALUES (?, ?, ?)",
                            (pid, format_date(dob), group if group else None))
                conn.commit()
                count += 1
    context.user_data["awaiting_import"] = False
    await update.message.reply_text(f"✅ Imported {count} rows.")

# ---- status ----
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current IPPT status with friendlier post-window messaging."""
    msg = update.message
    today = current_local_date()

    # Must be verified
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.telegram_id, u.personnel_id, u.verified_at, u.completed_year, u.completed_at,
                   p.birthday, p.group_name
              FROM users u
              JOIN personnel p ON u.personnel_id = p.personnel_id
             WHERE u.telegram_id = ?
            """,
            (msg.from_user.id,),
        )
        row = cur.fetchone()

    if not row:
        return await msg.reply_text("Please /verify first.")

    telegram_id, personnel_id, verified_at, completed_year, completed_at, birthday_str, group_name = row
    bday = parse_date_strict(birthday_str)

    # Window & deferment
    in_window, start, end = today_in_window(bday, today)
    window_key = start.year
    next_start = adjusted_birthday_for_year(bday, start.year + 1)

    with closing(db_connect()) as conn:
        d = get_deferment_by_pid(conn, personnel_id, window_key)
    defer_reason, defer_status = (d[0], d[1]) if d else (None, None)

    # Compute status line
    if defer_status == "approved":
        status_line = f"IPPT Status: ⛔️ Defer — {defer_reason}"
    elif completed_year == window_key:
        status_line = "IPPT Status: ✅ Completed"
    else:
        if today < start:
            status_line = f"IPPT Status: 💤 Window not open yet — starts {format_date(start)}"
        elif start <= today <= end:
            days_left = (end - today).days
            status_line = f"IPPT Status: ⏳ {days_left} day(s) left to complete"
        else:
            status_line = f"IPPT Status: Window closed — next window starts {format_date(next_start)}"

    lines = [
        status_line,
        f"Window: {format_date(start)} → {format_date(end)}",
        f"Today:  {format_date(today)}",
    ]
    if group_name:
        lines.append(f"Group:  {group_name}")
    lines.append(f"ID:     {personnel_id}")

    if in_window and defer_status != "approved" and completed_year != window_key:
        interval = int(os.getenv("REMINDER_INTERVAL_DAYS", "10"))
        lines.append(f"Reminder cadence: every {interval} day(s) while in-window")

    await msg.reply_text("\n".join(lines))

# ---- complete/uncomplete ----
async def complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    today = current_local_date()

    parts = msg.text.split()
    given_date = None
    if len(parts) == 2:
        try:
            given_date = parse_date_strict(parts[1].strip())
        except Exception:
            return await msg.reply_text("Invalid date. Use YYYY-MM-DD, e.g., /complete 2025-01-10")

    with closing(db_connect()) as conn:
        data = get_personnel_and_user(conn, msg.from_user.id)
    if not data or not data[1]:
        return await msg.reply_text("You're not verified yet. Use /verify first.")

    _, personnel_id, _, completed_year, _, birthday_str, _ = data
    bday = parse_date_strict(birthday_str)
    in_window, start, end = today_in_window(bday, today)
    if not in_window:
        return await msg.reply_text(f"You're outside your current window. Window: {format_date(start)} → {format_date(end)}")

    completion_date = today if given_date is None else given_date
    if not (start <= completion_date <= end):
        return await msg.reply_text(f"The supplied date must be within your current window {format_date(start)} to {format_date(end)}.")

    window_key = start.year
    now_iso = iso_from_local_date(completion_date, hour=9, minute=0)

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
               SET completed_year = ?, completed_at = ?
             WHERE telegram_id = ?
            """,
            (window_key, now_iso, msg.from_user.id),
        )
        # Replace history row for (telegram_id, year)
        cur.execute("DELETE FROM completions WHERE telegram_id=? AND year=?", (msg.from_user.id, window_key))
        cur.execute("INSERT INTO completions (telegram_id, year, completed_at) VALUES (?, ?, ?)", (msg.from_user.id, window_key, now_iso))
        conn.commit()

    await msg.reply_text(
        f"✅ Recorded as completed for the {WINDOW_DAYS}-day window starting {format_date(start)}.\n"
        f"(Window end: {format_date(end)}; date recorded: {format_date(completion_date)})"
    )

async def uncomplete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    with closing(db_connect()) as conn:
        data = get_personnel_and_user(conn, msg.from_user.id)
    if not data or not data[1]:
        return await msg.reply_text("You're not verified yet. Use /verify first.")
    _, personnel_id, _, completed_year, _, birthday_str, _ = data
    bday = parse_date_strict(birthday_str)
    _, start, _ = today_in_window(bday, current_local_date())
    window_key = start.year
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET completed_year=NULL, completed_at=NULL WHERE telegram_id=? AND completed_year=?", (msg.from_user.id, window_key))
        cur.execute("DELETE FROM completions WHERE telegram_id=? AND year=?", (msg.from_user.id, window_key))
        conn.commit()
    await msg.reply_text("🧹 Completion cleared for this window.")

# ---- admin complete/uncomplete ----
async def _resolve_tokens_to_tids(tokens):
    tids = set()
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for t in tokens:
            t = t.strip()
            if not t:
                continue
            # direct telegram id?
            if t.isdigit():
                tids.add(int(t))
                continue
            # else treat as personnel_id -> find linked telegram
            cur.execute("SELECT telegram_id FROM users WHERE personnel_id=?", (t,))
            for r in cur.fetchall():
                if r[0] is not None:
                    tids.add(int(r[0]))
    return tids

async def _resolve_tokens_to_pids(tokens):
    pids = set()
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for t in tokens:
            t = t.strip()
            if not t:
                continue
            cur.execute("SELECT 1 FROM personnel WHERE personnel_id=?", (t,))
            if cur.fetchone():
                pids.add(t); continue
            # else numeric telegram -> find linked pid
            if t.isdigit():
                cur.execute("SELECT personnel_id FROM users WHERE telegram_id=?", (int(t),))
                r = cur.fetchone()
                if r and r[0]:
                    pids.add(r[0])
    return pids

async def admin_complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: mark completion for users. /admin_complete <tokens> [YEAR] [--date YYYY-MM-DD]"""
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Usage: /admin_complete <tokens> [WINDOW_START_YEAR] [--date YYYY-MM-DD]")

    tail = parts[1].strip()

    m = re.search(r"--date\s+(\d{4}-\d{2}-\d{2})", tail)
    date_override = None
    if m:
        try:
            date_override = parse_date_strict(m.group(1))
        except Exception:
            return await update.message.reply_text("Invalid --date. Use YYYY-MM-DD.")
        tail = (tail[:m.start()] + tail[m.end():]).strip()

    tokens = [t for t in re.split(r"[,\s]+", tail) if t]
    year = None
    if tokens and re.fullmatch(r"\d{4}", tokens[-1] or "") and date_override is None:
        year = int(tokens[-1]); tokens = tokens[:-1]
    if not tokens:
        return await update.message.reply_text("No IDs provided.")

    tids = await _resolve_tokens_to_tids(tokens)
    if not tids:
        return await update.message.reply_text("No verified users matched these tokens.")

    updated = 0
    replaced = 0
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for tid in tids:
            cur.execute("SELECT p.birthday FROM users u JOIN personnel p ON u.personnel_id=p.personnel_id WHERE u.telegram_id=?", (tid,))
            r = cur.fetchone()
            if not r:
                continue
            bday = parse_date_strict(r[0])

            if date_override is not None:
                start_by_date, end_by_date = window_for_date(bday, date_override)
                if not (start_by_date <= date_override <= end_by_date):
                    return await update.message.reply_text(f"--date {format_date(date_override)} is not within the 100-day window for at least one user.")
                target_year = start_by_date.year
                completion_iso = iso_from_local_date(date_override, hour=9, minute=0)
            else:
                if year is None:
                    _, start, _ = today_in_window(bday, current_local_date())
                    target_year = start.year
                else:
                    start = adjusted_birthday_for_year(bday, year)
                    target_year = start.year
                completion_iso = datetime.now(TZINFO).isoformat()

            cur.execute("UPDATE users SET completed_year=?, completed_at=? WHERE telegram_id=?", (target_year, completion_iso, tid))
            updated += cur.rowcount
            cur.execute("DELETE FROM completions WHERE telegram_id=? AND year=?", (tid, target_year))
            cur.execute("INSERT INTO completions (telegram_id, year, completed_at) VALUES (?, ?, ?)", (tid, target_year, completion_iso))
            replaced += 1
        conn.commit()

    return await update.message.reply_text(f"✅ Admin completed (override). Users updated: {updated}, history rows replaced: {replaced}.")

async def admin_uncomplete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Usage: /admin_uncomplete <tokens> [WINDOW_START_YEAR]")
    tail = parts[1].strip()
    tokens = [t for t in re.split(r"[,\s]+", tail) if t]
    year = None
    if tokens and re.fullmatch(r"\d{4}", tokens[-1] or ""):
        year = int(tokens[-1]); tokens = tokens[:-1]
    if not tokens:
        return await update.message.reply_text("No IDs provided.")
    tids = await _resolve_tokens_to_tids(tokens)
    cleared = 0
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for tid in tids:
            if year is None:
                cur.execute("SELECT p.birthday FROM users u JOIN personnel p ON u.personnel_id=p.personnel_id WHERE u.telegram_id=?", (tid,))
                r = cur.fetchone()
                if not r:
                    continue
                bday = parse_date_strict(r[0])
                _, start, _ = today_in_window(bday, current_local_date())
                target_year = start.year
            else:
                target_year = year
            cur.execute("UPDATE users SET completed_year=NULL, completed_at=NULL WHERE telegram_id=? AND completed_year=?", (tid, target_year))
            cur.execute("DELETE FROM completions WHERE telegram_id=? AND year=?", (tid, target_year))
            cleared += cur.rowcount
        conn.commit()
    return await update.message.reply_text(f"🧹 Cleared completion for {cleared} user(s).")

# ---- deferment (admin-only, personnel_id based) ----
async def defer_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    raw = update.message.text
    if " -- " not in raw:
        return await update.message.reply_text("Usage: /defer_reason <tokens> [WINDOW_START_YEAR] -- <reason text>")
    head, reason = raw.split(" -- ", 1)
    parts = head.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Usage: /defer_reason <tokens> [WINDOW_START_YEAR] -- <reason text>")
    tail = parts[1].strip()
    tokens = [t for t in re.split(r"[,\s]+", tail) if t]
    year = None
    if tokens and re.fullmatch(r"\d{4}", tokens[-1] or ""):
        year = int(tokens[-1]); tokens = tokens[:-1]
    if not tokens:
        return await update.message.reply_text("No IDs provided.")
    pids = await _resolve_tokens_to_pids(tokens)
    now = datetime.now(TZINFO).isoformat()
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for pid in pids:
            win_year = year
            if win_year is None:
                cur.execute("SELECT birthday FROM personnel WHERE personnel_id=?", (pid,))
                r = cur.fetchone()
                if not r:
                    continue
                bday = parse_date_strict(r[0])
                start, _ = window_for_date(bday, current_local_date())
                win_year = start.year
            cur.execute(
                """
                INSERT INTO deferments (personnel_id, year, reason, status, created_at)
                VALUES (?, ?, ?, 'approved', ?)
                ON CONFLICT(personnel_id, year) DO UPDATE SET
                  reason=excluded.reason,
                  status='approved'
                """,
                (pid, win_year, reason.strip(), now),
            )
        conn.commit()
    await update.message.reply_text(f"📝 Reason set for {len(pids)} user(s).")

async def defer_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Usage: /defer_reset <tokens> [WINDOW_START_YEAR]")
    tail = parts[1].strip()
    tokens = [t for t in re.split(r"[,\s]+", tail) if t]
    year = None
    if tokens and re.fullmatch(r"\d{4}", tokens[-1] or ""):
        year = int(tokens[-1]); tokens = tokens[:-1]
    if not tokens:
        return await update.message.reply_text("No IDs provided.")
    pids = await _resolve_tokens_to_pids(tokens)
    deleted = 0
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for pid in pids:
            win_year = year
            if win_year is None:
                cur.execute("SELECT birthday FROM personnel WHERE personnel_id=?", (pid,))
                r = cur.fetchone()
                if not r:
                    continue
                bday = parse_date_strict(r[0])
                start, _ = window_for_date(bday, current_local_date())
                win_year = start.year
            cur.execute("DELETE FROM deferments WHERE personnel_id=? AND year=?", (pid, win_year))
            deleted += cur.rowcount
        conn.commit()
    return await update.message.reply_text(f"🧹 Deferments cleared: {deleted} row(s).")

# ---- unlink / remove ----
async def unlink_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Usage: /unlink_user <tokens>")
    tokens = [t for t in re.split(r"[,\s]+", parts[1].strip()) if t]
    tids = await _resolve_tokens_to_tids(tokens)
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for tid in tids:
            cur.execute("UPDATE users SET personnel_id=NULL WHERE telegram_id=?", (tid,))
        conn.commit()
    return await update.message.reply_text(f"🔗 Unlinked {len(tids)} user(s).")

async def remove_personnel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Usage: /remove_personnel <ID or comma-list>")
    ids = [x.strip() for x in re.split(r"[,\s]+", parts[1]) if x.strip()]
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for pid in ids:
            cur.execute("DELETE FROM deferments WHERE personnel_id=?", (pid,))
            cur.execute("UPDATE users SET personnel_id=NULL WHERE personnel_id=?", (pid,))
            cur.execute("DELETE FROM personnel WHERE personnel_id=?", (pid,))
        conn.commit()
    return await update.message.reply_text(f"🗑️ Removed {len(ids)} personnel record(s) and unlinked any users.")

# ---- reports ----
async def report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Excel export: All + per-group sheets; red rows = not completed & no active deferment; includes days_left/days_overdue."""
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill

    today = current_local_date()

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT p.personnel_id, p.birthday, p.group_name,
                   u.telegram_id, u.completed_year, u.completed_at
              FROM personnel p
              LEFT JOIN users u ON p.personnel_id = u.personnel_id
            """
        )
        rows = cur.fetchall()

    def build_row(pid, bday_str, group_name, telegram_id, completed_year, completed_at):
        bday = parse_date_strict(bday_str)
        _, start, end = today_in_window(bday, today)
        window_key = start.year
        done = (completed_year == window_key)
        verified = bool(telegram_id)

        d_status, d_reason = "", ""
        with closing(db_connect()) as conn2:
            d = get_deferment_by_pid(conn2, pid, window_key)
            if d:
                d_reason, d_status = d[0], d[1]

        if done:
            days_left, days_overdue = "", ""
        else:
            if today <= end:
                days_left = (end - today).days
                days_overdue = ""
            else:
                days_left = ""
                days_overdue = (today - end).days

        return {
            "personnel_id": pid,
            "birthday": bday_str,
            "group_name": group_name or "",
            "verified": "yes" if verified else "no",
            "window_start": format_date(start),
            "window_end": format_date(end),
            "completed_this_window": "yes" if done else "no",
            "completed_at": completed_at or "",
            "deferment_status": d_status or "",
            "deferment_reason": d_reason or "",
            "days_left": days_left,
            "days_overdue": days_overdue,
            "_highlight_red": (not done) and (d_status != "approved"),
        }

    data_rows = [build_row(*r) for r in rows]

    headers = [
        "personnel_id","birthday","group_name","verified",
        "window_start","window_end",
        "completed_this_window","completed_at",
        "deferment_status","deferment_reason",
        "days_left","days_overdue"
    ]

    def write_sheet(ws, rows_list):
        ws.append(headers)
        for cell in ws[1]:
            cell.font = Font(bold=True)
        for rec in rows_list:
            ws.append([rec[h] for h in headers])
            if rec["_highlight_red"]:
                for cell in ws[ws.max_row]:
                    cell.fill = PatternFill(start_color="FFFFC0C0", end_color="FFFFC0C0", fill_type="solid")
        # Autosize
        for col in ws.columns:
            try:
                col_letter = col[0].column_letter
            except Exception:
                continue
            max_len = 12
            for c in col:
                max_len = max(max_len, len(str(c.value)) if c.value is not None else 0)
            ws.column_dimensions[col_letter].width = min(40, max_len + 2)

    # Grouping
    groups = {}
    for rec in data_rows:
        g = (rec["group_name"].strip() or "No Group")
        groups.setdefault(g, []).append(rec)

    from openpyxl import Workbook
    wb = Workbook()
    ws_all = wb.active
    ws_all.title = "All"
    write_sheet(ws_all, data_rows)

    def safe_sheet_name(name: str) -> str:
        bad = ['\\', '/', '?', '*', '[', ']']
        for b in bad:
            name = name.replace(b, ' ')
        return (name.strip() or "No Group")[:31]

    for gname, recs in sorted(groups.items(), key=lambda kv: kv[0].lower()):
        ws = wb.create_sheet(title=safe_sheet_name(gname))
        write_sheet(ws, recs)

    out = io.BytesIO()
    wb.save(out); out.seek(0)
    await update.message.reply_document(
        document=InputFile(out, filename="ippt_100day_report.xlsx"),
        caption="100-day window report (All + per-group). Red rows = not completed & no active deferment."
    )

async def report_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await update.message.reply_text("Usage: /report_group <GROUP>\nUse 'No Group' for blank group entries.")
    target = parts[1].strip()
    target_norm = (target or "").strip().lower()

    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill

    today = current_local_date()

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT p.personnel_id, p.birthday, p.group_name,
                   u.telegram_id, u.completed_year, u.completed_at
              FROM personnel p
              LEFT JOIN users u ON p.personnel_id = u.personnel_id
            """
        )
        rows = cur.fetchall()

    def normalize_group(g):
        g = (g or "").strip()
        return g if g else "No Group"

    def build_row(pid, bday_str, group_name, telegram_id, completed_year, completed_at):
        bday = parse_date_strict(bday_str)
        _, start, end = today_in_window(bday, today)
        window_key = start.year
        done = (completed_year == window_key)
        verified = bool(telegram_id)

        d_status, d_reason = "", ""
        with closing(db_connect()) as conn2:
            d = get_deferment_by_pid(conn2, pid, window_key)
            if d:
                d_reason, d_status = d[0], d[1]

        if done:
            days_left, days_overdue = "", ""
        else:
            if today <= end:
                days_left = (end - today).days
                days_overdue = ""
            else:
                days_left = ""
                days_overdue = (today - end).days

        return {
            "personnel_id": pid,
            "birthday": bday_str,
            "group_name": group_name or "",
            "verified": "yes" if verified else "no",
            "window_start": format_date(start),
            "window_end": format_date(end),
            "completed_this_window": "yes" if done else "no",
            "completed_at": completed_at or "",
            "deferment_status": d_status or "",
            "deferment_reason": d_reason or "",
            "days_left": days_left,
            "days_overdue": days_overdue,
            "_highlight_red": (not done) and (d_status != "approved"),
        }

    data_rows = [build_row(*r) for r in rows]
    filtered = [r for r in data_rows if normalize_group(r["group_name"]).lower() == (target_norm or "no group")]

    if not filtered:
        groups = sorted({normalize_group(r["group_name"]) for r in data_rows})
        return await update.message.reply_text("No rows matched that group. Available groups:\n- " + "\n- ".join(groups))

    headers = [
        "personnel_id","birthday","group_name","verified",
        "window_start","window_end",
        "completed_this_window","completed_at",
        "deferment_status","deferment_reason",
        "days_left","days_overdue"
    ]

    wb = Workbook()
    ws = wb.active
    ws.title = (target if target.strip() else "No Group")[:31]

    ws.append(headers)
    for cell in ws[1]:
        cell.font = Font(bold=True)

    for rec in filtered:
        ws.append([rec[h] for h in headers])
        if rec["_highlight_red"]:
            for cell in ws[ws.max_row]:
                cell.fill = PatternFill(start_color="FFFFC0C0", end_color="FFFFC0C0", fill_type="solid")

    # Autosize
    for col in ws.columns:
        try:
            col_letter = col[0].column_letter
        except Exception:
            continue
        max_len = 12
        for c in col:
            max_len = max(max_len, len(str(c.value)) if c.value is not None else 0)
        ws.column_dimensions[col_letter].width = min(40, max_len + 2)

    import re as _re
    slug = _re.sub(r'[^A-Za-z0-9]+', '_', (target or "No Group")).strip('_')

    out = io.BytesIO()
    wb.save(out); out.seek(0)
    await update.message.reply_document(
        document=InputFile(out, filename=f"ippt_100day_report_{slug}.xlsx"),
        caption=f"100-day window report — Group: {target}"
    )

# ---- audit ----
async def defer_audit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["source","personnel_id","year","reason","status","created_at","group_name","linked_telegram_ids","old_telegram_id"])
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        # migrated rows (if table exists)
        has_migrated = False
        try:
            cur.execute("SELECT 1 FROM deferment_migration_audit LIMIT 1")
            has_migrated = True
        except Exception:
            pass
        if has_migrated:
            cur.execute("SELECT new_personnel_id, year, reason, status, created_at, old_telegram_id FROM deferment_migration_audit ORDER BY year, new_personnel_id")
            for pid, yr, reason, status, created_at, old_tid in cur.fetchall():
                cur2 = conn.cursor()
                cur2.execute("SELECT group_name FROM personnel WHERE personnel_id=?", (pid,))
                g = cur2.fetchone()
                writer.writerow(["migrated", pid, yr, reason or "", status or "", created_at or "", (g[0] if g and g[0] else ""), "", old_tid or ""])
        # current snapshot
        cur.execute(
            """
            SELECT d.personnel_id, d.year, d.reason, d.status, d.created_at,
                   p.group_name
              FROM deferments d
              LEFT JOIN personnel p ON p.personnel_id = d.personnel_id
             ORDER BY d.year, d.personnel_id
            """
        )
        for pid, yr, reason, status, created_at, group_name in cur.fetchall():
            cur2 = conn.cursor()
            cur2.execute("SELECT telegram_id FROM users WHERE personnel_id=?", (pid,))
            tids = [str(r[0]) for r in cur2.fetchall() if r and r[0] is not None]
            writer.writerow(["current", pid, yr, reason or "", status or "", created_at or "", group_name or "", " ".join(tids), ""])
    data = out.getvalue().encode("utf-8")
    bio = io.BytesIO(data); bio.seek(0)
    await update.message.reply_document(document=InputFile(bio, filename="deferment_audit.csv"), caption="Deferment audit CSV (migrated + current)")

# ----------------------
# Scheduler / reminders
# ----------------------
async def daily_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    today = current_local_date()
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.telegram_id, u.personnel_id, p.birthday, u.completed_year
              FROM users u
              JOIN personnel p ON u.personnel_id = p.personnel_id
            """
        )
        rows = cur.fetchall()

    for telegram_id, pid, bday_str, completed_year in rows:
        bday = parse_date_strict(bday_str)
        in_window, start, end = today_in_window(bday, today)
        window_key = start.year

        # Deferment check (by personnel_id)
        with closing(db_connect()) as conn:
            d = get_deferment_by_pid(conn, pid, window_key)
        skip = bool(d and d[1] == "approved")
        done = (completed_year == window_key)

        # Auto-reset deferment & completion after window end
        if today > end:
            try:
                with closing(db_connect()) as conn2:
                    c2 = conn2.cursor()
                    c2.execute("DELETE FROM deferments WHERE personnel_id=? AND year=?", (pid, window_key))
                    conn2.commit()
            except Exception:
                pass
            try:
                with closing(db_connect()) as conn3:
                    c3 = conn3.cursor()
                    c3.execute("UPDATE users SET completed_year=NULL, completed_at=NULL WHERE telegram_id=? AND completed_year=?", (telegram_id, window_key))
                    conn3.commit()
            except Exception:
                pass

        if in_window and not done and not skip:
            days_since_start = (today - start).days
            if days_since_start % REMINDER_INTERVAL_DAYS == 0:
                try:
                    await context.bot.send_message(
                        chat_id=telegram_id,
                        text=(
                            f"⏰ Reminder: Your IPPT window is {format_date(start)} → {format_date(end)}.\n"
                            f"Use /complete when done. You can also do /status anytime."
                        ),
                    )
                except Exception:
                    pass

# ----------------------
# Wire-up
# ----------------------
def setup_handlers(app):
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("verify", verify))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("complete", complete))
    app.add_handler(CommandHandler("uncomplete", uncomplete))

    # Admin
    app.add_handler(CommandHandler("admin_help", admin_help))
    app.add_handler(CommandHandler("add_personnel", add_personnel))
    app.add_handler(CommandHandler("update_birthday", update_birthday))
    app.add_handler(CommandHandler("import_csv", import_csv_cmd))
    app.add_handler(CommandHandler("report", report))
    app.add_handler(CommandHandler("report_group", report_group))
    app.add_handler(CommandHandler("defer_reason", defer_reason))
    app.add_handler(CommandHandler("defer_reset", defer_reset))
    app.add_handler(CommandHandler("admin_complete", admin_complete))
    app.add_handler(CommandHandler("admin_uncomplete", admin_uncomplete))
    app.add_handler(CommandHandler("unlink_user", unlink_user))
    app.add_handler(CommandHandler("remove_personnel", remove_personnel))
    app.add_handler(CommandHandler("defer_audit", defer_audit))

    # Document handler only when awaiting import
    app.add_handler(MessageHandler(filters.ATTACHMENT & (~filters.COMMAND), document_handler))

def schedule_jobs(app):
    # 09:00 local time daily
    app.job_queue.run_daily(
        daily_reminder_job,
        time=time(hour=9, minute=0, tzinfo=TZINFO),
        name="daily_reminders",
    )

def main():
    init_db()
    if not BOT_TOKEN:
        raise SystemExit("Missing BOT_TOKEN env var.")
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    setup_handlers(app)
    schedule_jobs(app)
    print("Bot is running…")
    app.run_polling(close_loop=False)

if __name__ == "__main__":
    main()
