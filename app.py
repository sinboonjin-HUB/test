import os, io, csv, sqlite3, logging, secrets, string
from datetime import date, datetime, timedelta, time as dtime
import pytz
from telegram import Update, InputFile
from telegram.ext import (
    Application, ApplicationBuilder, CommandHandler, ContextTypes,
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
TOKEN = os.environ.get("BOT_TOKEN")
REMINDER_HOUR_LOCAL = 9
WINDOW_DAYS = 100
TOKEN_LEN = 6
ENV_ADMIN_IDS = set(
    int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip().isdigit()
)

# -------------------- Token + Helpers --------------------
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
        con.execute("""
        UPDATE users SET token=COALESCE(token, ?) WHERE chat_id=?;
        """, (generate_token(), chat_id))
        con.commit()

# -------------------- DB Init --------------------
def init_db():
    with sqlite3.connect(DB_PATH) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS users (
            chat_id INTEGER PRIMARY KEY,
            username TEXT,
            name TEXT,
            birthday TEXT,
            last_completed TEXT,
            reminders_enabled INTEGER DEFAULT 1,
            last_reminded_on TEXT,
            token TEXT UNIQUE,
            is_admin INTEGER DEFAULT 0
        );
        """)
        # Ensure every row has a token
        cur = con.execute("SELECT chat_id, token FROM users")
        rows = cur.fetchall()
        for cid, tok in rows:
            if not tok:
                con.execute("UPDATE users SET token=? WHERE chat_id=?", (generate_token(), cid))
        # Seed admins from env var
        if ENV_ADMIN_IDS:
            con.executemany("UPDATE users SET is_admin=1 WHERE chat_id=?", [(i,) for i in ENV_ADMIN_IDS])
        con.commit()

# -------------------- DB Helpers --------------------
# -------------------- DB Helpers --------------------
def get_user(chat_id: int) -> dict | None:
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        cur = con.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,))
        r = cur.fetchone()
        return dict(r) if r else None

def list_users() -> list[dict]:
    with sqlite3.connect(DB_PATH) as con:
        con.row_factory = sqlite3.Row
        return [dict(r) for r in con.execute("SELECT * FROM users")]

def set_birthday(chat_id: int, bday: str):
    # Validate format; raises ValueError if invalid
    date.fromisoformat(bday)
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE users SET birthday=? WHERE chat_id=?", (bday, chat_id))
        con.commit()

def set_name(chat_id: int, name: str):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE users SET name=? WHERE chat_id=?", (name, chat_id))
        con.commit()

def set_completed(chat_id: int, done: str | None):
    if done:
        # Validate format
        date.fromisoformat(done)
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE users SET last_completed=? WHERE chat_id=?", (done, chat_id))
        con.commit()

def mark_reminded_today(chat_id: int, today: date):
    with sqlite3.connect(DB_PATH) as con:
        con.execute("UPDATE users SET last_reminded_on=? WHERE chat_id=?", (today.isoformat(), chat_id))
        con.commit()


# -------------------- IPPT Logic --------------------
def season_start(birthday: date, year: int) -> date:
    try: return date(year, birthday.month, birthday.day)
    except ValueError: return date(year, 2, 28)

def compute_status(today: date, bday_str: str | None, last_completed: str | None):
    if not bday_str: return {"state": "not_set"}
    bday = date.fromisoformat(bday_str)
    start = season_start(bday, today.year)
    end = start + timedelta(days=WINDOW_DAYS)
    last = date.fromisoformat(last_completed) if last_completed else None
    if last and last >= start: return {"state": "completed", "start": start, "end": end}
    if today < start: return {"state": "pre", "start": start, "end": end}
    if start <= today <= end: return {"state": "in", "start": start, "end": end, "days": (end - today).days}
    return {"state": "overdue", "start": start, "end": end}

def fmt_status(user: dict) -> str:
    today = datetime.now(SG_TZ).date()
    st = compute_status(today, user.get("birthday"), user.get("last_completed"))
    if st["state"] == "not_set": return "⚠️ Birthday not set. Use /setbirthday YYYY-MM-DD"
    if st["state"] == "pre": return f"🎂 Season {st['start']} → {st['end']} (not opened yet)"
    if st["state"] == "in": return f"✅ In window {st['start']} → {st['end']}, {st['days']} days left"
    if st["state"] == "completed": return f"🎉 Completed for season {st['start']} → {st['end']}"
    if st["state"] == "overdue": return f"🔴 Overdue! Window ended {st['end']}"
    return "?"

# -------------------- Commands --------------------
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    upsert_user(update.effective_chat.id, update.effective_user.username)
    await update.message.reply_text("👋 Welcome! Use /setbirthday YYYY-MM-DD, /complete YYYY-MM-DD, /summary")

async def setbirthday_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args: return await update.message.reply_text("Usage: /setbirthday YYYY-MM-DD")
    try: set_birthday(update.effective_chat.id, ctx.args[0])
    except Exception as e: return await update.message.reply_text(f"❌ {e}")
    await update.message.reply_text("✅ Birthday set")

async def complete_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args: return await update.message.reply_text("Usage: /complete YYYY-MM-DD")
    try: set_completed(update.effective_chat.id, ctx.args[0])
    except Exception as e: return await update.message.reply_text(f"❌ {e}")
    await update.message.reply_text("🎉 Completion saved")

async def summary_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = get_user(update.effective_chat.id)
    if not u: return await update.message.reply_text("Not registered. /start first")
    await update.message.reply_text(fmt_status(u))

# -------------------- Reminder Engine --------------------
async def daily_sweep(ctx: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(SG_TZ).date()
    for u in list_users():
        if not u.get("birthday"): continue
        st = compute_status(today, u["birthday"], u.get("last_completed"))
        if st["state"] in ("in","overdue"):
            await ctx.bot.send_message(chat_id=u["chat_id"], text=f"⏰ Reminder:\n{fmt_status(u)}")
            mark_reminded_today(u["chat_id"], today)

def schedule_daily_job(app: Application):
    now = datetime.now(SG_TZ)
    first_run = now.replace(hour=REMINDER_HOUR_LOCAL, minute=0, second=0, microsecond=0)
    if first_run <= now: first_run += timedelta(days=1)
    delay = (first_run - now).total_seconds()
    app.job_queue.run_repeating(daily_sweep, interval=86400, first=delay)
    log.info("Daily reminders scheduled for 09:00 SGT")

# -------------------- Main --------------------
def build_app() -> Application:
    if not TOKEN: raise RuntimeError("BOT_TOKEN missing")
    init_db()
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setbirthday", setbirthday_cmd))
    app.add_handler(CommandHandler("complete", complete_cmd))
    app.add_handler(CommandHandler("summary", summary_cmd))
    schedule_daily_job(app)
    return app

def main(): app = build_app(); app.run_polling()

if __name__ == "__main__": main()

