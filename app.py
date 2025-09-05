import os
import io
import csv
import asyncio
import logging
import sqlite3
import secrets
import string
from contextlib import closing
from datetime import date, datetime, timedelta

import pytz
from telegram import Update, InputFile
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | ippt-bot | %(message)s",
)
log = logging.getLogger("ippt-bot")

# -------------------- Config --------------------
SG_TZ = pytz.timezone("Asia/Singapore")
DB_PATH = os.environ.get("DB_PATH", "ippt.sqlite3")
TOKEN = os.environ.get("BOT_TOKEN")  # required

REMINDER_HOUR_LOCAL = 9      # 09:00 Asia/Singapore
WINDOW_DAYS = 100            # IPPT window length after birthday
TOKEN_LEN = 6                # admin token length (alphanumeric)
ENV_ADMIN_IDS = set(
    int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip().isdigit()
)

# -------------------- DB Helpers --------------------
def init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            chat_id INTEGER PRIMARY KEY,
            username TEXT,
            name TEXT,              -- free-text user display name
            birthday TEXT,          -- YYYY-MM-DD
            last_completed TEXT,    -- YYYY-MM-DD (last IPPT completion date)
            reminders_enabled INTEGER DEFAULT 1,
            last_reminded_on TEXT,  -- YYYY-MM-DD (SG local date)
            token TEXT UNIQUE,      -- admin token to act on user
            is_admin INTEGER DEFAULT 0
        );
        """)
        # backfill token/is_admin/name for existing rows
        cur = con.execute("SELECT chat_id, token FROM users")
        rows = cur.fetchall()
        for chat_id, tok in rows:
            if not tok:
                con.execute("UPDATE users SET token=? WHERE chat_id=?", (generate_token(), chat_id))
        # elevate env admins
        if ENV_ADMIN_IDS:
            con.executemany("UPDATE users SET is_admin=1 WHERE chat_id=?", [(i,) for i in ENV_ADMIN_IDS])
        con.commit()

def generate_token(n: int = TOKEN_LEN) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))

def upsert_user(chat_id: int, username: str | None):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        INSERT INTO users (chat_id, username, token)
        VALUES (?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET username=excluded.username;
        """, (chat_id, username, generate_token()))
        # Ensure token exists even on update
        con.execute("""
        UPDATE users SET token=COALESCE(token, ?)
        WHERE chat_id=?;
        """, (generate_token(), chat_id))
        con.commit()

def set_birthday(chat_id: int, birthday_str: str):
    _ = date.fromisoformat(birthday_str)  # validate
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE users SET birthday=? WHERE chat_id=?", (birthday_str, chat_id))
        con.commit()

def set_name(chat_id: int, name: str):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE users SET name=? WHERE chat_id=?", (name, chat_id))
        con.commit()

def set_completed(chat_id: int, completed_str: str | None):
    if completed_str:
        _ = date.fromisoformat(completed_str)  # validate
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE users SET last_completed=? WHERE chat_id=?", (completed_str, chat_id))
        con.commit()

def set_reminders_enabled(chat_id: int, enabled: bool):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE users SET reminders_enabled=? WHERE chat_id=?", (1 if enabled else 0, chat_id))
        con.commit()

def set_admin(chat_id: int, is_admin: bool):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE users SET is_admin=? WHERE chat_id=?", (1 if is_admin else 0, chat_id))
        con.commit()

def get_user(chat_id: int) -> dict | None:
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,))
        row = cur.fetchone()
        return dict(row) if row else None

def get_user_by_token(token: str) -> dict | None:
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute("SELECT * FROM users WHERE token=?", (token,))
        row = cur.fetchone()
        return dict(row) if row else None

def list_users() -> list[dict]:
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute("SELECT * FROM users")
        return [dict(r) for r in cur.fetchall()]

def mark_reminded_today(chat_id: int, sg_today: date):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE users SET last_reminded_on=? WHERE chat_id=?", (sg_today.isoformat(), chat_id))
        con.commit()

# -------------------- IPPT Logic --------------------
def season_birthday(birthday_mmdd: tuple[int, int], year: int) -> date:
    m, d = birthday_mmdd
    try:
        return date(year, m, d)
    except ValueError:
        if m == 2 and d == 29:
            return date(year, 2, 28)
        raise

def compute_season(today_sg: date, birthday_str: str, year: int | None = None) -> tuple[date, date]:
    bday = date.fromisoformat(birthday_str)
    mmdd = (bday.month, bday.day)
    yy = year if year is not None else today_sg.year
    start = season_birthday(mmdd, yy)
    end = start + timedelta(days=WINDOW_DAYS)
    return start, end

def compute_status(today_sg: date, birthday_str: str | None, last_completed_str: str | None, year: int | None = None) -> dict:
    if not birthday_str:
        return {"ok": False, "state": "not_set", "bday": None, "start": None, "end": None, "days_left": None}

    start, end = compute_season(today_sg, birthday_str, year)
    last_completed = date.fromisoformat(last_completed_str) if last_completed_str else None

    # Completed for this season?
    if last_completed and last_completed >= start:
        return {"ok": False, "state": "completed", "start": start, "end": end, "days_left": max(0, (end - today_sg).days)}

    if today_sg < start:
        return {"ok": False, "state": "pre_window", "start": start, "end": end, "days_left": (end - today_sg).days}

    if start <= today_sg <= end:
        return {"ok": True, "state": "in_window", "start": start, "end": end, "days_left": (end - today_sg).days}

    return {"ok": True, "state": "overdue", "start": start, "end": end, "days_left": -1}

def fmt_status_summary(today_sg: date, user: dict, year: int | None = None) -> str:
    st = compute_status(today_sg, user.get("birthday"), user.get("last_completed"), year)
    if st["state"] == "not_set":
        return "⚠️ Birthday not set. Use /setbirthday YYYY-MM-DD"
    if st["state"] == "pre_window":
        return (f"🎂 Season: {st['start'].isoformat()} → {st['end'].isoformat()}\n"
                f"🟡 Window opens on your birthday and lasts {WINDOW_DAYS} days.\n"
                f"⏳ Days until window ends: {st['days_left']}")
    if st["state"] == "in_window":
        return (f"✅ In window ({st['start'].isoformat()} → {st['end'].isoformat()})\n"
                f"⏳ Days left: {st['days_left']}\n"
                f"Mark complete when done: /complete YYYY-MM-DD")
    if st["state"] == "completed":
        return (f"🎉 Completed this season. Window: {st['start'].isoformat()} → {st['end'].isoformat()}")
    if st["state"] == "overdue":
        return (f"🔴 Overdue! Window ended on {st['end'].isoformat()}.\n"
                f"Please complete ASAP and mark it: /complete YYYY-MM-DD")
    return "Unknown state."

# -------------------- Auth Helpers --------------------
def is_admin_chat(chat_id: int) -> bool:
    usr = get_user(chat_id)
    return bool(usr and usr.get("is_admin", 0) == 1)

def require_admin(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        if not is_admin_chat(chat_id):
            await update.effective_message.reply_text("🚫 Admins only.")
            return
        return await func(update, context)
    return wrapper

# -------------------- Bot Commands --------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    upsert_user(update.effective_chat.id, update.effective_user.username)
    msg = (
        "👋 Hello! I’ll remind you to complete your IPPT within 100 days after your birthday.\n\n"
        "Setup:\n"
        "• /setbirthday YYYY-MM-DD  – set/update your birthday\n"
        "• /setname Your Name       – optional display name (not validated)\n"
        "• /complete YYYY-MM-DD     – mark IPPT done\n"
        "• /uncomplete              – clear completion (allowed only if still in window)\n"
        "• /summary                 – see your status\n"
        "• /whoami                  – show your saved info & admin token\n"
        "• /pause /resume           – toggle reminders\n"
        "• /test_reminder           – send a test reminder now\n\n"
        "Admin:\n"
        "• /admin_complete <TOKEN> [YEAR] [--date YYYY-MM-DD]\n"
        "• /admin_uncomplete <TOKEN> [YEAR]\n"
        "• /admin_export [YEAR]\n"
        "• /admin_add <chat_id>, /admin_remove <chat_id>, /admin_list\n"
    )
    await update.effective_message.reply_text(msg)

async def setbirthday_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Usage: /setbirthday YYYY-MM-DD")
        return
    try:
        set_birthday(update.effective_chat.id, context.args[0])
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Invalid date. Use YYYY-MM-DD. Error: {e}")
        return

    usr = get_user(update.effective_chat.id)
    sg_today = datetime.now(SG_TZ).date()
    await update.effective_message.reply_text(f"✅ Birthday set to {context.args[0]}\n\n{fmt_status_summary(sg_today, usr)}")

async def setname_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Usage: /setname Your Name")
        return
    name = " ".join(context.args).strip()
    set_name(update.effective_chat.id, name[:120])
    await update.effective_message.reply_text(f"✅ Name saved: {name}")

async def complete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Usage: /complete YYYY-MM-DD\n(Use the actual completion date.)")
        return
    try:
        set_completed(update.effective_chat.id, context.args[0])
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Invalid date. Use YYYY-MM-DD. Error: {e}")
        return

    usr = get_user(update.effective_chat.id)
    sg_today = datetime.now(SG_TZ).date()
    await update.effective_message.reply_text(f"🎉 Marked completed on {context.args[0]}.\n\n{fmt_status_summary(sg_today, usr)}")

async def uncomplete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    usr = get_user(update.effective_chat.id)
    if not usr:
        await update.effective_message.reply_text("You’re not registered. Use /start first.")
        return
    if not usr.get("birthday"):
        await update.effective_message.reply_text("Set your birthday first: /setbirthday YYYY-MM-DD")
        return
    today = datetime.now(SG_TZ).date()
    st = compute_status(today, usr["birthday"], usr.get("last_completed"))
    if st["state"] == "in_window" or st["state"] == "pre_window":
        set_completed(update.effective_chat.id, None)
        await update.effective_message.reply_text("✅ Cleared completion for this season.")
    else:
        await update.effective_message.reply_text("⛔ You can only uncomplete while still within the current season window. Ask an admin if needed.")

async def summary_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    usr = get_user(update.effective_chat.id)
    if not usr:
        upsert_user(update.effective_chat.id, update.effective_user.username)
        usr = get_user(update.effective_chat.id)
    sg_today = datetime.now(SG_TZ).date()
    await update.effective_message.reply_text(fmt_status_summary(sg_today, usr))

async def whoami_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    usr = get_user(update.effective_chat.id)
    if not usr:
        await update.effective_message.reply_text("Not registered. Use /start.")
        return
    today = datetime.now(SG_TZ).date()
    st = compute_status(today, usr.get("birthday"), usr.get("last_completed"))
    msg = (
        f"🪪 Your Profile\n"
        f"• Chat ID: {usr['chat_id']}\n"
        f"• Username: @{usr['username']}\n"
        f"• Name: {usr.get('name') or '-'}\n"
        f"• Birthday: {usr.get('birthday') or '-'}\n"
        f"• Token: {usr.get('token') or '-'}\n"
        f"\n{fmt_status_summary(today, usr)}"
    )
    await update.effective_message.reply_text(msg)

async def pause_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_reminders_enabled(update.effective_chat.id, False)
    await update.effective_message.reply_text("⏸️ Reminders paused. Use /resume to turn them back on.")

async def resume_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    set_reminders_enabled(update.effective_chat.id, True)
    await update.effective_message.reply_text("▶️ Reminders resumed.")

async def test_reminder_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_one_reminder(update.effective_chat.id, force=True)
    await update.effective_message.reply_text("🧪 Test reminder sent (if deliverable).")

# -------------------- Admin Commands --------------------
def parse_admin_args(args: list[str]) -> tuple[str | None, int | None, str | None]:
    """
    Parses: <TOKEN> [YEAR] [--date YYYY-MM-DD]
    Returns (token, year, date_str)
    """
    if not args:
        return None, None, None
    token = args[0]
    year = None
    date_str = None
    i = 1
    if i < len(args) and args[i].isdigit():
        year = int(args[i]); i += 1
    if i < len(args) and args[i] == "--date":
        if i + 1 >= len(args):
            raise ValueError("Missing date after --date")
        date_str = args[i + 1]
        _ = date.fromisoformat(date_str)  # validate
    return token, year, date_str

@require_admin
async def admin_complete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        token, year, date_str = parse_admin_args(context.args)
        if not token:
            await update.effective_message.reply_text("Usage: /admin_complete <TOKEN> [YEAR] [--date YYYY-MM-DD]")
            return
        usr = get_user_by_token(token)
        if not usr:
            await update.effective_message.reply_text("❌ No user found for that token.")
            return
        if not usr.get("birthday"):
            await update.effective_message.reply_text("User has no birthday set.")
            return
        today = datetime.now(SG_TZ).date()
        # If date not provided, use Singapore today
        done_date = date.fromisoformat(date_str) if date_str else today
        # Force mark as completed; this is allowed anywhere in cycle
        set_completed(usr["chat_id"], done_date.isoformat())
        # Update last_reminded_on to suppress same-day re-pings
        mark_reminded_today(usr["chat_id"], today)
        text = f"✅ Admin override: marked completed on {done_date.isoformat()}."
        if year is not None:
            st = compute_status(today, usr["birthday"], usr.get("last_completed"), year)
            text += f"\nSeason ({year}): {st['start'].isoformat()} → {st['end'].isoformat()}"
        await update.effective_message.reply_text(text)
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Error: {e}")

@require_admin
async def admin_uncomplete_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        token, year, _ = parse_admin_args(context.args)
        if not token:
            await update.effective_message.reply_text("Usage: /admin_uncomplete <TOKEN> [YEAR]")
            return
        usr = get_user_by_token(token)
        if not usr:
            await update.effective_message.reply_text("❌ No user found for that token.")
            return
        # Clear completion regardless of season (admin override)
        set_completed(usr["chat_id"], None)
        today = datetime.now(SG_TZ).date()
        text = "✅ Admin override: cleared completion."
        if year is not None and usr.get("birthday"):
            st = compute_status(today, usr["birthday"], usr.get("last_completed"), year)
            text += f"\nSeason ({year}): {st['start'].isoformat()} → {st['end'].isoformat()}"
        await update.effective_message.reply_text(text)
    except Exception as e:
        await update.effective_message.reply_text(f"❌ Error: {e}")

@require_admin
async def admin_export_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /admin_export [YEAR]
    year = None
    if context.args and context.args[0].isdigit():
        year = int(context.args[0])
    users = list_users()
    today = datetime.now(SG_TZ).date()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["chat_id","username","name","token","birthday","season_start","season_end","last_completed","state","days_left","reminders_enabled"])
    for u in users:
        bday = u.get("birthday")
        if bday:
            start, end = compute_season(today, bday, year)
            st = compute_status(today, bday, u.get("last_completed"), year)
            state = st["state"]
            days_left = st["days_left"]
        else:
            start = end = ""
            state = "not_set"
            days_left = ""
        writer.writerow([
            u["chat_id"], u.get("username") or "", u.get("name") or "", u.get("token") or "",
            bday or "", start if not isinstance(start, date) else start.isoformat(),
            end if not isinstance(end, date) else end.isoformat(),
            u.get("last_completed") or "", state, days_left, u.get("reminders_enabled", 1),
        ])
    csv_bytes = io.BytesIO(buf.getvalue().encode("utf-8"))
    csv_bytes.seek(0)
    fname = f"ippt_export_{year or today.year}.csv"
    await update.effective_message.reply_document(InputFile(csv_bytes, filename=fname), caption="📄 IPPT export")

@require_admin
async def admin_add_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.effective_message.reply_text("Usage: /admin_add <chat_id>")
        return
    cid = int(context.args[0])
    upsert_user(cid, None)
    set_admin(cid, True)
    await update.effective_message.reply_text(f"✅ Added admin: {cid}")

@require_admin
async def admin_remove_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or not context.args[0].isdigit():
        await update.effective_message.reply_text("Usage: /admin_remove <chat_id>")
        return
    cid = int(context.args[0])
    set_admin(cid, False)
    await update.effective_message.reply_text(f"✅ Removed admin: {cid}")

@require_admin
async def admin_list_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    users = list_users()
    admin_ids = [str(u["chat_id"]) for u in users if u.get("is_admin") == 1]
    await update.effective_message.reply_text("👑 Admins:\n" + ("\n".join(admin_ids) if admin_ids else "(none)"))

# -------------------- Reminder Engine --------------------
async def send_one_reminder(chat_id: int, force: bool = False):
    usr = get_user(chat_id)
    if not usr or not usr.get("reminders_enabled", 1):
        return

    sg_now = datetime.now(SG_TZ)
    sg_today = sg_now.date()

    last_day = usr.get("last_reminded_on")
    if not force and last_day == sg_today.isoformat():
        return

    st = compute_status(sg_today, usr.get("birthday"), usr.get("last_completed"))
    if st["state"] in ("in_window", "overdue"):
        if st["state"] == "in_window":
            text = (
                f"📣 IPPT Reminder\n"
                f"Window: {st['start'].isoformat()} → {st['end'].isoformat()}\n"
                f"⏳ Days left: {st['days_left']}\n\n"
                f"When completed, confirm: /complete YYYY-MM-DD"
            )
        else:
            text = (
                f"🚨 IPPT Overdue!\n"
                f"Your window ended on {st['end'].isoformat()}.\n"
                f"Please complete ASAP and mark it: /complete YYYY-MM-DD"
            )
        # ephemeral app just for sending message
        app: Application = Application.builder().token(TOKEN).build()
        try:
            async with app:
                await app.bot.send_message(chat_id=chat_id, text=text)
            mark_reminded_today(chat_id, sg_today)
        except Exception as e:
            log.warning(f"Failed to send reminder to {chat_id}: {e}")

async def daily_sweep(context: ContextTypes.DEFAULT_TYPE):
    users = list_users()
    if not users:
        return
    log.info(f"Daily sweep: checking {len(users)} users")
    for u in users:
        try:
            await send_one_reminder(u["chat_id"])
        except Exception as e:
            log.exception(f"Error reminding {u['chat_id']}: {e}")

def schedule_daily_job(app: Application):
    when = datetime.now(SG_TZ).replace(hour=REMINDER_HOUR_LOCAL, minute=0, second=0, microsecond=0).timetz()
    app.job_queue.run_daily(daily_sweep, time=when, name="daily-ippt-sweep", timezone=SG_TZ)
    log.info("JobQueue scheduled: daily sweep at 09:00 Asia/Singapore")

# -------------------- Main --------------------
def build_app() -> Application:
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN env var is required.")
    init_db()
    app = ApplicationBuilder().token(TOKEN).build()

    # User commands
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler(["register", "setbirthday"], setbirthday_cmd))
    app.add_handler(CommandHandler("setname", setname_cmd))
    app.add_handler(CommandHandler("complete", complete_cmd))
    app.add_handler(CommandHandler("uncomplete", uncomplete_cmd))
    app.add_handler(CommandHandler("summary", summary_cmd))
    app.add_handler(CommandHandler("whoami", whoami_cmd))
    app.add_handler(CommandHandler("pause", pause_cmd))
    app.add_handler(CommandHandler("resume", resume_cmd))
    app.add_handler(CommandHandler("test_reminder", test_reminder_cmd))

    # Admin commands
    app.add_handler(CommandHandler("admin_complete", admin_complete_cmd))
    app.add_handler(CommandHandler("admin_uncomplete", admin_uncomplete_cmd))
    app.add_handler(CommandHandler("admin_export", admin_export_cmd))
    app.add_handler(CommandHandler("admin_add", admin_add_cmd))
    app.add_handler(CommandHandler("admin_remove", admin_remove_cmd))
    app.add_handler(CommandHandler("admin_list", admin_list_cmd))

    # Schedule daily sweep
    if app.job_queue is None:
        log.warning('No JobQueue available. Install PTB with: pip install "python-telegram-bot[job-queue]~=21.6"')
    else:
        schedule_daily_job(app)

    return app

def main():
    app = build_app()
    log.info("Starting bot…")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
