
import csv
import io
import os
import re
import sqlite3
from contextlib import closing
from datetime import date, datetime, timedelta, time
from typing import Optional, Any

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
WINDOW_DAYS = 100  # inclusive window size

# ----------------------
# DB Helpers
# ----------------------
def db_connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1].lower() == column.lower() for row in cur.fetchall())



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
        # If an old telegram_id-based deferments table exists, migrate it to personnel_id-based
        cur.execute("PRAGMA table_info('deferments')")
        cols = [r[1].lower() for r in cur.fetchall()]
        if 'telegram_id' in cols and 'personnel_id' not in cols:
            # ensure audit table exists
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
            # create new table
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
            # copy rows
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


def window_for_date(bday: date, check: date | None = None) -> tuple[date, date]:
    """Return the 100-day window (inclusive) for the birthday in the year of 'check'."""
    if check is None:
        check = current_local_date()
    start = adjusted_birthday_for_year(bday, check.year)
    end = start + timedelta(days=WINDOW_DAYS)  # inclusive end
    return start, end


def today_in_window(bday: date, check: date | None = None) -> tuple[bool, date, date]:
    """Is 'check' within the 100-day window of this year's birthday?"""
    if check is None:
        check = current_local_date()
    start, end = window_for_date(bday, check)
    in_window = (start <= check <= end)
    return in_window, start, end


def next_reminder_date(start: date, end: date, today: date, interval: int) -> Optional[date]:
    """Compute the next reminder date on the interval grid [start, end] inclusive."""
    if today < start:
        return start
    if today > end:
        return None
    days_since_start = (today - start).days
    remainder = days_since_start % interval
    if remainder == 0:
        next_date = today
    else:
        next_date = today + timedelta(days=interval - remainder)
    return next_date if next_date <= end else None


def get_personnel_and_user(conn: sqlite3.Connection, telegram_id: int):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT u.telegram_id, u.personnel_id, u.verified_at, u.completed_year, u.completed_at,
               p.birthday, p.group_name
          FROM users u LEFT JOIN personnel p ON u.personnel_id = p.personnel_id
         WHERE u.telegram_id = ?
        """,
        (telegram_id,),
    )
    return cur.fetchone()



def get_deferment_by_pid(conn: sqlite3.Connection, personnel_id: str, year: int):
    cur = conn.cursor()
    cur.execute(
        "SELECT reason, status FROM deferments WHERE personnel_id=? AND year=?",
        (personnel_id, year),
    )
    return cur.fetchone()

# ----------------------

# ----------------------
# Command Handlers (User)
# ----------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        """
Hi! I'm the IPPT Reminder Bot. Here's what I can do:

• /verify <PERSONNEL_ID> <YYYY-MM-DD> — verify yourself
• /status — see your 100‑day IPPT window & status
• /complete — mark this window's IPPT as completed
• /uncomplete — undo your completion for this window
• Deferment: ask your admin to set a reason on your behalf

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

    _, personnel_id, verified_at, completed_year, completed_at, birthday_str, group_name = data
    bday = parse_date_strict(birthday_str)
    today = current_local_date()
    in_window, start, end = today_in_window(bday, today)

    window_key = start.year  # completion/deferment tracked by window start year
    done_this_window = (completed_year == window_key)

    # Deferment status for this window
    defer_reason = None
    with closing(db_connect()) as conn:
        d = get_deferment_by_pid(conn, personnel_id, window_key)
        if d and d[1] == "approved":
            defer_reason = d[0] or ""

    # Compute status string
    status_line = ""
    days_left = None
    days_overdue = None
    if done_this_window:
        status_line = "IPPT Status: ✅ Completed"
    elif defer_reason:
        status_line = f"IPPT Status: ⛔️ Defer — {defer_reason}"
    else:
        if in_window:
            days_left = (end - today).days
            status_line = f"IPPT Status: ⏳ {days_left} day(s) left to complete"
        else:
            if today < start:
                status_line = f"IPPT Status: 🕒 Not in window yet (starts {format_date(start)})"
            else:
                days_overdue = (today - end).days
                status_line = f"IPPT Status: ⚠️ Overdue by {days_overdue} day(s)"

    nrd = None if (done_this_window or defer_reason) else next_reminder_date(start, end, today, REMINDER_INTERVAL_DAYS)

    lines = [
        status_line,
        f"Personnel ID: <code>{personnel_id}</code>",
        f"Group: <b>{group_name or '-'}</b>",
        f"Birthday: <b>{format_date(bday)}</b>",
        f"Window (100 days): <b>{format_date(start)}</b> → <b>{format_date(end)}</b>",
        f"Today: <b>{format_date(today)}</b> — {'✅ In window' if in_window else '🕒 Outside window'}",
    ]
    if days_left is not None:
        lines.append(f"Days left to complete: <b>{days_left}</b>")
    if days_overdue is not None:
        lines.append(f"Overdue by: <b>{days_overdue}</b> day(s)")
    if completed_at:
        lines.append(f"Last completion recorded at: <code>{completed_at}</code>")
    if nrd:
        tag = " (today)" if nrd == today and in_window else ""
        lines.append(f"Next reminder: <b>{format_date(nrd)}</b>{tag}")
    elif not (done_this_window or defer_reason):
        lines.append(f"Next reminder: <i>none (window ended)</i>")

    await msg.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    today = current_local_date()
    now_iso = datetime.now(TZINFO).isoformat()
    with closing(db_connect()) as conn:
        data = get_personnel_and_user(conn, msg.from_user.id)
        if not data or not data[1]:
            return await msg.reply_text("You're not verified yet. Use /verify first.")
        _, personnel_id, _, completed_year, _, birthday_str, _ = data
        bday = parse_date_strict(birthday_str)
        in_window, start, end = today_in_window(bday, today)
        window_key = start.year
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
               SET completed_year = ?, completed_at = ?
             WHERE telegram_id = ?
            """,
            (window_key, now_iso, msg.from_user.id),
        )
        cur.execute(
            "INSERT INTO completions (telegram_id, year, completed_at) VALUES (?, ?, ?)",
            (msg.from_user.id, window_key, now_iso),
        )
        conn.commit()

    await msg.reply_text(
        f"✅ Recorded as completed for the {WINDOW_DAYS}-day window starting {format_date(start)}.\n"
        f"(Window end: {format_date(end)})"
    )


async def uncomplete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Users: undo your completion for the current 100-day window."""
    msg = update.message
    today = current_local_date()
    with closing(db_connect()) as conn:
        data = get_personnel_and_user(conn, msg.from_user.id)
        if not data or not data[1]:
            return await msg.reply_text("You're not verified yet. Use /verify first.")
        bday = parse_date_strict(data[5])
        _, start, _ = today_in_window(bday, today)
        window_key = start.year
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET completed_year=NULL, completed_at=NULL WHERE telegram_id=? AND completed_year=?",
            (msg.from_user.id, window_key),
        )
        cleared = cur.rowcount
        cur.execute(
            "DELETE FROM completions WHERE telegram_id=? AND year=?",
            (msg.from_user.id, window_key),
        )
        conn.commit()
    if cleared:
        await msg.reply_text(f"↩️ Your completion for the window starting {format_date(start)} has been undone.")
    else:
        await msg.reply_text(f"No completion found to undo for this window.")


async def defer_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Users: deferment entry is admin-only now; show guidance."""
    msg = update.message
    return await msg.reply_text(
        "Deferment reasons must be entered by an admin.\n"
        "Please contact your admin to submit `/defer_reason <YOUR_ID> [YEAR] -- <reason>`.",
        parse_mode=ParseMode.MARKDOWN,
    )


# ----------------------
# Admin Commands
# ----------------------

async def update_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: update a personnel's birthday. Usage: /update_birthday <PERSONNEL_ID> <YYYY-MM-DD>"""
    msg = update.message
    if not is_admin(msg.from_user.id):
        return await msg.reply_text("Admins only.")
    parts = msg.text.split()
    if len(parts) != 3:
        return await msg.reply_text("Usage: /update_birthday <PERSONNEL_ID> <YYYY-MM-DD>")
    pid = parts[1].strip()
    try:
        dob = parse_date_strict(parts[2].strip())
    except ValueError:
        return await msg.reply_text("Invalid date. Use YYYY-MM-DD.")
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        cur.execute("UPDATE personnel SET birthday=? WHERE personnel_id=?", (format_date(dob), pid))
        if cur.rowcount == 0:
            return await msg.reply_text("No such PERSONNEL_ID.")
        conn.commit()
    await msg.reply_text(f"✅ Updated {pid} birthday to {format_date(dob)}.")



async def defer_audit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: export a CSV audit of deferments.
    - If migration audit exists, include those rows with source='migrated' (old_telegram_id).
    - Always include current deferments with source='current' plus linked telegram IDs and group_name.
    """
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")

    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(["source","personnel_id","year","reason","status","created_at","group_name","linked_telegram_ids","old_telegram_id"])

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        # migrated rows if table exists
        try:
            cur.execute("SELECT 1 FROM deferment_migration_audit LIMIT 1")
            has_migrated = True
        except Exception:
            has_migrated = False

        if has_migrated:
            cur.execute("SELECT new_personnel_id, year, reason, status, created_at, old_telegram_id FROM deferment_migration_audit ORDER BY year, new_personnel_id")
            for pid, yr, reason, status, created_at, old_tid in cur.fetchall():
                # join group name (current)
                cur2 = conn.cursor()
                cur2.execute("SELECT group_name FROM personnel WHERE personnel_id=?", (pid,))
                g = cur2.fetchone()
                group_name = g[0] if g and g[0] else ""
                writer.writerow(["migrated", pid, yr, reason or "", status or "", created_at or "", group_name, "", old_tid or ""])

        # current snapshot
        cur.execute("""            SELECT d.personnel_id, d.year, d.reason, d.status, d.created_at,
                   p.group_name
              FROM deferments d
              LEFT JOIN personnel p ON p.personnel_id = d.personnel_id
             ORDER BY d.year, d.personnel_id
        """)
        current_rows = cur.fetchall()
        for pid, yr, reason, status, created_at, group_name in current_rows:
            # linked telegram ids
            cur2 = conn.cursor()
            cur2.execute("SELECT telegram_id FROM users WHERE personnel_id=?", (pid,))
            tids = [str(r[0]) for r in cur2.fetchall() if r and r[0] is not None]
            writer.writerow(["current", pid, yr, reason or "", status or "", created_at or "", group_name or "", " ".join(tids), ""])

    data = out.getvalue().encode("utf-8")
    bio = io.BytesIO(data); bio.seek(0)
    await update.message.reply_document(document=InputFile(bio, filename="deferment_audit.csv"), caption="Deferment audit CSV (migrated + current)")

async def admin_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not is_admin(msg.from_user.id):
        return await msg.reply_text("Admins only.")
    await msg.reply_text(
        f"""
Admin commands:

• /add_personnel <PERSONNEL_ID> <YYYY-MM-DD> [GROUP]
• /update_birthday <PERSONNEL_ID> <YYYY-MM-DD>
• /import_csv — upload CSV/XLSX (columns: personnel_id,birthday[,group])
• /report — Excel with red highlight for incomplete (no active deferment). Includes days_left/days_overdue.
• /report_group <GROUP> — export a single-group Excel file (use 'No Group' for empty).
• /defer_audit — export a CSV of migrated/current deferments
• /whoami — show your Telegram ID

• /unlink_user <ID or list> — accepts Telegram IDs or personnel_id(s), mixed
• /remove_personnel <ID or list> — remove personnel + linked users & completions

• /admin_uncomplete <tokens> [WINDOW_START_YEAR]
• /admin_complete <tokens> [WINDOW_START_YEAR]
• /defer_reset <tokens> [WINDOW_START_YEAR]
• /defer_reason  <tokens> [WINDOW_START_YEAR] -- <reason text>  (user cannot set reasons themselves)
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
    if len(parts) < 3:
        return await msg.reply_text(
            "Usage: /add_personnel <PERSONNEL_ID> <YYYY-MM-DD> [GROUP]\nExample: /add_personnel A12345 1995-07-14 Group A"
        )
    personnel_id = parts[1].strip()
    try:
        dob = parse_date_strict(parts[2].strip())
    except ValueError:
        return await msg.reply_text("Invalid date. Use YYYY-MM-DD.")
    group_name = " ".join(parts[3:]).strip() if len(parts) > 3 else None

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "INSERT INTO personnel (personnel_id, birthday, group_name) VALUES (?, ?, ?)",
                (personnel_id, format_date(dob), group_name if group_name else None),
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
        "Send me a CSV or XLSX file. Columns: personnel_id, birthday (YYYY-MM-DD), optional group."
    )


def _normalize_header(name: str) -> str:
    return (name or "").strip().lstrip("\ufeff").replace("\u200b", "").lower()


def _extract_records_from_csv_bytes(b: bytes):
    text = b.decode("utf-8-sig")  # strips BOM automatically
    reader = csv.DictReader(io.StringIO(text))
    fieldmap = { _normalize_header(h): h for h in (reader.fieldnames or []) }
    pid_key = fieldmap.get("personnel_id")
    dob_key = fieldmap.get("birthday")
    grp_key = fieldmap.get("group") or fieldmap.get("group_name") or fieldmap.get("grp")
    if not pid_key or not dob_key:
        for h in (reader.fieldnames or []):
            nh = _normalize_header(h)
            if not pid_key and "personnel" in nh and "id" in nh:
                pid_key = h
            if not dob_key and ("birthday" in nh or nh in ("dob","dateofbirth")):
                dob_key = h
            if not grp_key and nh in ("group","group_name","grp","team"):
                grp_key = h
    if not pid_key or not dob_key:
        return []
    for row in reader:
        pid = str(row.get(pid_key, "")).strip()
        dob = str(row.get(dob_key, "")).strip()
        grp = row.get(grp_key) if grp_key else None
        grp = (str(grp).strip() if grp is not None else None)
        yield pid, dob, (grp or None)


def _extract_records_from_xlsx_bytes(b: bytes):
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
    grp_idx = find_idx({"group","group_name","grp","team"})
    if pid_idx is None or dob_idx is None:
        return []
    for r in rows[1:]:
        if r is None:
            continue
        pid = r[pid_idx] if pid_idx < len(r) else None
        dob = r[dob_idx] if dob_idx < len(r) else None
        grp = r[grp_idx] if (grp_idx is not None and grp_idx < len(r)) else None
        pid_s = "" if pid is None else str(pid).strip()
        d = parse_birthday_any(dob)
        dob_s = format_date(d) if d else (str(dob).strip() if dob is not None else "")
        grp_s = None if grp is None else str(grp).strip()
        yield pid_s, dob_s, (grp_s or None)


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
        for pid, bday, grp in records:
            if not pid or not bday:
                skipped += 1
                continue
            try:
                dob = parse_date_strict(bday)
            except Exception:
                skipped += 1
                continue
            # Update first (also update group if provided & non-empty)
            cur.execute(
                """
                UPDATE personnel
                   SET birthday = ?,
                       group_name = COALESCE(NULLIF(?, ''), group_name)
                 WHERE personnel_id = ?
                """,
                (format_date(dob), grp, pid),
            )
            if cur.rowcount:
                updated += 1
            else:
                try:
                    cur.execute(
                        "INSERT INTO personnel (personnel_id, birthday, group_name) VALUES (?, ?, ?)",
                        (pid, format_date(dob), grp if grp else None),
                    )
                    added += 1
                except sqlite3.IntegrityError:
                    skipped += 1
        conn.commit()

    await update.message.reply_text(f"Import done. ✅ Added: {added} | ✏️ Updated: {updated} | ⏭️ Skipped: {skipped}")



async def report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate an XLSX with red-highlighted rows for 'not completed' (no active deferment).
    Includes days_left (to window end) or days_overdue (past window end).
    Produces an "All" sheet plus one sheet per group.
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill

    msg = update.message
    if not is_admin(msg.from_user.id):
        return await msg.reply_text("Admins only.")

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

    # Build row dicts
    def build_row(pid, bday_str, group_name, telegram_id, completed_year, completed_at):
        bday = parse_date_strict(bday_str)
        _, start, end = today_in_window(bday, today)
        window_key = start.year
        done = (completed_year == window_key)
        verified = bool(telegram_id)

        d_status, d_reason = "", ""
        if verified:
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
            max_len = 0
            col_letter = col[0].column_letter
            for c in col:
                try:
                    max_len = max(max_len, len(str(c.value)))
                except Exception:
                    pass
            ws.column_dimensions[col_letter].width = min(40, max(12, max_len + 2))

    # Grouping
    groups = {}
    for rec in data_rows:
        g = rec["group_name"].strip() or "No Group"
        groups.setdefault(g, []).append(rec)

    # Workbook with All + per group
    wb = Workbook()
    ws_all = wb.active
    ws_all.title = "All"
    write_sheet(ws_all, data_rows)

    # Create one sheet per group (limit sheetname to 31 chars)
    def safe_sheet_name(name: str) -> str:
        bad = ['\\', '/', '?', '*', '[', ']']
        for b in bad:
            name = name.replace(b, ' ')
        name = name.strip() or "No Group"
        return name[:31]

    for gname, recs in sorted(groups.items(), key=lambda kv: kv[0].lower()):
        if gname == "":
            gname = "No Group"
        ws = wb.create_sheet(title=safe_sheet_name(gname))
        write_sheet(ws, recs)

    # Summary
    completed_count = sum(1 for r in data_rows if r["completed_this_window"] == "yes")
    outstanding_count = len(data_rows) - completed_count

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)

    caption = (
        f"100-day window report (All + per-group sheets)\n"
        f"Completed: {completed_count}\n"
        f"Outstanding: {outstanding_count}\n"
        f"Red rows: not completed and no active deferment"
    )
    await msg.reply_document(
        document=InputFile(out, filename="ippt_100day_report.xlsx"),
        caption=caption,
    )

# ----------------------
# Token resolution & Admin ops
# ----------------------
async def report_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Export a single-group XLSX report: /report_group <GROUP> (use 'No Group' for empty group)."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill

    msg = update.message
    if not is_admin(msg.from_user.id):
        return await msg.reply_text("Admins only.")

    parts = msg.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        return await msg.reply_text("Usage: /report_group <GROUP>\nUse 'No Group' for blank group entries.")

    target = parts[1].strip()
    target_norm = target.lower().strip()

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
        if verified:
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
    # Filter by group (case-insensitive); treat blank as 'No Group'
    filtered = [r for r in data_rows if (normalize_group(r["group_name"]).lower() == (target_norm if target_norm else "no group"))]

    if not filtered:
        groups = sorted({normalize_group(r["group_name"]) for r in data_rows})
        return await msg.reply_text("No rows matched that group. Available groups:\n- " + "\n- ".join(groups))

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

    for col in ws.columns:
        max_len = 0
        col_letter = col[0].column_letter
        for c in col:
            try:
                max_len = max(max_len, len(str(c.value)))
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(40, max(12, max_len + 2))

    completed_count = sum(1 for r in filtered if r["completed_this_window"] == "yes")
    outstanding_count = len(filtered) - completed_count

    slug = re.sub(r'[^A-Za-z0-9]+', '_', target.strip() or "No_Group").strip('_')
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)

    caption = (
        f"100-day window report — Group: {target}\n"
        f"Completed: {completed_count}\n"
        f"Outstanding: {outstanding_count}"
    )
    await msg.reply_document(
        document=InputFile(out, filename=f"ippt_100day_report_{slug}.xlsx"),
        caption=caption,
    )

async def _resolve_tokens_to_tids(tokens):
    """Tokens may be numeric Telegram IDs or personnel_id(s). Returns a set of Telegram IDs."""
    tids = set()
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for t in tokens:
            t = t.strip()
            if not t:
                continue
            try:
                tids.add(int(t))
                continue
            except ValueError:
                pass
            cur.execute("SELECT telegram_id FROM users WHERE personnel_id=?", (t,))
            rows = [r[0] for r in cur.fetchall() if r[0] is not None]
            tids.update(rows)
    return tids



async def _resolve_tokens_to_pids(tokens):
    """Tokens may be personnel_id(s) or Telegram IDs. Returns a set of personnel_ids."""
    pids = set()
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for t in tokens:
            t = t.strip()
            if not t:
                continue
            # personnel_id direct
            cur.execute("SELECT 1 FROM personnel WHERE personnel_id=?", (t,))
            if cur.fetchone():
                pids.add(t)
                continue
            # numeric telegram id -> linked personnel_id
            try:
                tid = int(t)
            except ValueError:
                continue
            cur.execute("SELECT personnel_id FROM users WHERE telegram_id=?", (tid,))
            r = cur.fetchone()
            if r and r[0]:
                pids.add(r[0])
    return pids

async def unlink_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    raw = update.message.text
    parts = raw.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Usage: /unlink_user <TELEGRAM_ID or personnel_id [ , more IDs ]>")

    tokens = [t.strip() for t in re.split(r'[,\s]+', parts[1]) if t.strip()]
    tids = await _resolve_tokens_to_tids(tokens)

    total_deleted_users = 0
    total_deleted_completions = 0
    not_found = []

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for tid in tids:
            cur.execute("DELETE FROM completions WHERE telegram_id=?", (tid,))
            total_deleted_completions += cur.rowcount
            cur.execute("DELETE FROM users WHERE telegram_id=?", (tid,))
            if cur.rowcount == 0:
                not_found.append(str(tid))
            else:
                total_deleted_users += 1
        conn.commit()

    lines = ["🧹 Unlink summary"]
    lines.append(f"Requested IDs: {len(tokens)}")
    lines.append(f"Unlinked user rows: {total_deleted_users}")
    lines.append(f"Deleted completion rows: {total_deleted_completions}")
    if not_found:
        lines.append(f"Not found: {', '.join(not_found)}")

    return await update.message.reply_text("\n".join(lines))


async def remove_personnel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    raw = update.message.text
    parts = raw.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Usage: /remove_personnel <PERSONNEL_ID [ , more IDs ]>")
    ids_str = parts[1]
    candidates = [p.strip() for p in re.split(r'[,\s]+', ids_str) if p.strip()]
    if not candidates:
        return await update.message.reply_text("No valid IDs provided. Example: /remove_personnel 719B, 123B")

    removed_personnel = 0
    total_unlinked_users = 0
    total_deleted_completions = 0
    not_found = []

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for pid in candidates:
            # Find linked telegram_ids
            cur.execute("SELECT telegram_id FROM users WHERE personnel_id=?", (pid,))
            tids = [r[0] for r in cur.fetchall()]

            deleted_completions = 0
            for tid in tids:
                cur.execute("DELETE FROM completions WHERE telegram_id=?", (tid,))
                deleted_completions += cur.rowcount

            cur.execute("DELETE FROM users WHERE personnel_id=?", (pid,))
            deleted_users = cur.rowcount

            cur.execute("DELETE FROM personnel WHERE personnel_id=?", (pid,))
            deleted_p = cur.rowcount

            if deleted_p:
                removed_personnel += 1
                total_unlinked_users += deleted_users
                total_deleted_completions += deleted_completions
            else:
                not_found.append(pid)

        conn.commit()

    lines = [f"🗑️ Remove personnel summary"]
    lines.append(f"Requested: {len(candidates)} ID(s)")
    lines.append(f"Removed personnel rows: {removed_personnel}")
    lines.append(f"Unlinked user rows: {total_unlinked_users}")
    lines.append(f"Deleted completion rows: {total_deleted_completions}")
    if not_found:
        lines.append(f"Not found: {', '.join(not_found)}")

    return await update.message.reply_text("\n".join(lines))



async def defer_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: clear deferment for tokens (Telegram IDs or personnel_id) and optional [WINDOW_START_YEAR]."""
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Usage: /defer_reset <tokens> [WINDOW_START_YEAR]")
    tail = parts[1].strip()
    tokens = [t for t in re.split(r'[,\s]+', tail) if t]

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

async def admin_uncomplete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admins: undo completion for users by Telegram ID or personnel_id. Optional trailing WINDOW_START_YEAR (4 digits)."""
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    raw = update.message.text
    parts = raw.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Usage: /admin_uncomplete <tokens> [WINDOW_START_YEAR]")

    tail = parts[1].strip()
    tokens = [t for t in re.split(r'[,\s]+', tail) if t]
    if not tokens:
        return await update.message.reply_text("No tokens provided.")

    year = None
    if re.fullmatch(r"\d{4}", tokens[-1] or ""):
        year = int(tokens[-1])
        tokens = tokens[:-1]
    if not tokens:
        return await update.message.reply_text("No IDs provided. Put the window start year last, e.g. /admin_uncomplete 719B 2025")

    tids = await _resolve_tokens_to_tids(tokens)

    total_cleared = 0
    total_deleted_completions = 0
    not_found = []

    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for tid in tids:
            # Determine user's window start year if year not provided
            target_year = year
            if target_year is None:
                cur.execute("""
                    SELECT p.birthday FROM users u
                    JOIN personnel p ON u.personnel_id = p.personnel_id
                    WHERE u.telegram_id=?
                """, (tid,))
                r = cur.fetchone()
                if not r:
                    not_found.append(str(tid)); continue
                bday = parse_date_strict(r[0])
                start, _ = window_for_date(bday, current_local_date())
                target_year = start.year

            cur.execute(
                "UPDATE users SET completed_year=NULL, completed_at=NULL WHERE telegram_id=? AND completed_year=?",
                (tid, target_year),
            )
            if cur.rowcount == 0:
                not_found.append(str(tid))
            else:
                total_cleared += cur.rowcount
                cur.execute("DELETE FROM completions WHERE telegram_id=? AND year=?", (tid, target_year))
                total_deleted_completions += cur.rowcount
        conn.commit()

    lines = [f"↩️ Admin uncomplete summary"]
    lines.append(f"Users cleared: {total_cleared}")
    lines.append(f"Deleted completion rows: {total_deleted_completions}")
    if not_found:
        lines.append(f"No matching completion for: {', '.join(not_found)}")

    return await update.message.reply_text("\n".join(lines))



async def admin_complete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: mark completion for users identified by tokens (Telegram IDs or personnel_id). Optional [WINDOW_START_YEAR]."""
    if not is_admin(update.message.from_user.id):
        return await update.message.reply_text("Admins only.")
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        return await update.message.reply_text("Usage: /admin_complete <tokens> [WINDOW_START_YEAR]")
    tail = parts[1].strip()
    tokens = [t for t in re.split(r'[,\s]+', tail) if t]

    year = None
    if tokens and re.fullmatch(r"\d{4}", tokens[-1] or ""):
        year = int(tokens[-1]); tokens = tokens[:-1]
    if not tokens:
        return await update.message.reply_text("No IDs provided.")

    tids = await _resolve_tokens_to_tids(tokens)
    if not tids:
        return await update.message.reply_text("No verified users matched these tokens.")

    now_iso = datetime.now(TZINFO).isoformat()
    updated = 0
    inserted = 0
    with closing(db_connect()) as conn:
        cur = conn.cursor()
        for tid in tids:
            target_year = year
            if target_year is None:
                cur.execute("SELECT p.birthday FROM users u JOIN personnel p ON u.personnel_id=p.personnel_id WHERE u.telegram_id=?", (tid,))
                r = cur.fetchone()
                if not r:
                    continue
                bday = parse_date_strict(r[0])
                start, _ = window_for_date(bday, current_local_date())
                target_year = start.year

            cur.execute("UPDATE users SET completed_year=?, completed_at=? WHERE telegram_id=?", (target_year, now_iso, tid))
            updated += cur.rowcount
            try:
                cur.execute("INSERT INTO completions (telegram_id, year, completed_at) VALUES (?, ?, ?)", (tid, target_year, now_iso))
                inserted += 1
            except Exception:
                pass
        conn.commit()

    return await update.message.reply_text(f"✅ Admin completed. Users updated: {updated}, rows inserted: {inserted}.")



async def defer_reason(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: set/override an active deferment reason using: /defer_reason <tokens> [WINDOW_START_YEAR] -- <reason text>"""
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
    tokens = [t for t in re.split(r'[,\s]+', tail) if t]

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
        try:
            bday = parse_date_strict(bday_str)
            in_window, start, end = today_in_window(bday, today)
            window_key = start.year
            done = (completed_year == window_key)

            # Skip if active deferment
            skip = False
            with closing(db_connect()) as conn:
                d = get_deferment_by_pid(conn, pid, window_key)
                if d and d[1] == "approved":
                    skip = True

            if today > end:
                # Auto-reset deferment after window end
                try:
                    with closing(db_connect()) as conn2:
                        c2 = conn2.cursor()
                        c2.execute("DELETE FROM deferments WHERE personnel_id=? AND year=?", (pid, window_key))
                        conn2.commit()
                except Exception:
                    pass
            if in_window and not done and not skip:
                days_since_start = (today - start).days
                if days_since_start % REMINDER_INTERVAL_DAYS == 0:
                    remaining = (end - today).days
                    text = (
                        "⚠️ IPPT Reminder\n"
                        f"Window: <b>{format_date(start)}</b> → <b>{format_date(end)}</b> (100 days)\n"
                        f"Days left: <b>{remaining}</b>\n"
                        f"Interval: every <b>{REMINDER_INTERVAL_DAYS}</b> days.\n\n"
                        "Reply /complete once you've done it to stop reminders, or /defer with a reason if needed."
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
    app.add_handler(CommandHandler("uncomplete", uncomplete))
    app.add_handler(CommandHandler("defer", defer_request))

    # Admin
    app.add_handler(CommandHandler("admin_help", admin_help))
    app.add_handler(CommandHandler("add_personnel", add_personnel))
    app.add_handler(CommandHandler("update_birthday", update_birthday))
    app.add_handler(CommandHandler("import_csv", import_csv_start))
    app.add_handler(CommandHandler("report", report))
    app.add_handler(CommandHandler("report_group", report_group))
    app.add_handler(CommandHandler("defer_audit", defer_audit))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("unlink_user", unlink_user))
    app.add_handler(CommandHandler("remove_personnel", remove_personnel))
    app.add_handler(CommandHandler("admin_uncomplete", admin_uncomplete))
    app.add_handler(CommandHandler("admin_complete", admin_complete))
    app.add_handler(CommandHandler("defer_reset", defer_reset))
    app.add_handler(CommandHandler("defer_reason", defer_reason))

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
