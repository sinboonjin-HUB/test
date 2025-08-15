# bot.py
import os, logging
from datetime import datetime, date
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from sqlalchemy import create_engine, Column, Integer, Date
from sqlalchemy.orm import declarative_base, sessionmaker
from apscheduler.schedulers.asyncio import AsyncIOScheduler

BOT_TOKEN = os.getenv("BOT_TOKEN") or ""
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID") or 0)
DATABASE_URL = os.getenv("DATABASE_URL") or ""
if not (BOT_TOKEN and ADMIN_CHAT_ID and DATABASE_URL):
    raise ValueError("Set BOT_TOKEN, ADMIN_CHAT_ID, DATABASE_URL")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

Base = declarative_base()
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    chat_id = Column(Integer, nullable=False, unique=True)
    birthday = Column(Date, nullable=False)
    last_completed_year = Column(Integer, nullable=True)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

def bdy_this_year(bd: date, today: date) -> date:
    try: return date(today.year, bd.month, bd.day)
    except ValueError: return date(today.year, 2, 28)

# ---- Commands ----
async def start(update: Update, _: ContextTypes.DEFAULT_TYPE):
    logging.info("/start by %s", update.effective_chat.id)
    await update.message.reply_text("Commands: /setbirthday YYYY-MM-DD • /mytask • /done • /summary (admin)")

async def set_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logging.info("/setbirthday by %s", update.effective_chat.id)
    if len(context.args) != 1:
        return await update.message.reply_text("Usage: /setbirthday YYYY-MM-DD")
    try:
        bd = datetime.strptime(context.args[0], "%Y-%m-%d").date()
    except ValueError:
        return await update.message.reply_text("Invalid date. Use YYYY-MM-DD.")
    s = Session()
    u = s.query(User).filter_by(chat_id=update.effective_chat.id).first()
    if u: u.birthday = bd
    else: s.add(User(chat_id=update.effective_chat.id, birthday=bd))
    s.commit(); s.close()
    await update.message.reply_text(f"Birthday set: {bd}. IPPT window is 100 days after each birthday.")

async def done(update: Update, _: ContextTypes.DEFAULT_TYPE):
    s = Session()
    u = s.query(User).filter_by(chat_id=update.effective_chat.id).first()
    if not u:
        s.close(); return await update.message.reply_text("Set your birthday first: /setbirthday YYYY-MM-DD")
    u.last_completed_year = datetime.now().year
    s.commit(); s.close()
    await update.message.reply_text("IPPT marked completed for this year. Reminders stopped.")

async def mytask(update: Update, _: ContextTypes.DEFAULT_TYPE):
    s = Session()
    u = s.query(User).filter_by(chat_id=update.effective_chat.id).first()
    if not u:
        s.close(); return await update.message.reply_text("Set your birthday: /setbirthday YYYY-MM-DD")
    today = datetime.now().date(); bdy = bdy_this_year(u.birthday, today)
    days_since = (today - bdy).days
    if u.last_completed_year == today.year:
        msg = "✅ Completed this year."
    elif 0 <= days_since <= 100:
        msg = f"❌ Pending. {100 - days_since} days left."
    elif days_since < 0:
        msg = "❌ Pending. Window not started yet."
    else:
        msg = "❌ Pending. Window expired."
    s.close()
    await update.message.reply_text(f"Birthday: {u.birthday} • {msg}")

async def summary(update: Update, _: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.id != ADMIN_CHAT_ID: return
    s = Session(); today = datetime.now().date()
    pending = []
    for u in s.query(User).all():
        if u.last_completed_year == today.year: continue
        bdy = bdy_this_year(u.birthday, today)
        days_since = (today - bdy).days
        if 0 <= days_since <= 100:
            pending.append(f"{u.chat_id}: {100 - days_since} days left (BD {u.birthday})")
    s.close()
    await update.message.reply_text("No users in active 100-day window." if not pending
                                    else "Pending (within 100 days after birthday):\n" + "\n".join(pending))

# ---- Scheduled jobs ----
async def send_reminders(app):
    s = Session(); today = datetime.now().date()
    for u in s.query(User).all():
        if u.last_completed_year == today.year: continue
        bdy = bdy_this_year(u.birthday, today)
        days_since = (today - bdy).days
        if 0 <= days_since <= 100:
            days_left = 100 - days_since
            if days_left % 10 == 0 or days_left == 100:
                try:
                    await app.bot.send_message(u.chat_id, f"IPPT reminder: {days_left} days left. Send /done when finished.")
                except Exception as e:
                    logging.warning("Notify fail %s: %s", u.chat_id, e)
    s.close()

async def send_admin_report(app):
    s = Session(); today = datetime.now().date()
    report = []
    for u in s.query(User).all():
        if u.last_completed_year == today.year: continue
        bdy = bdy_this_year(u.birthday, today)
        days_since = (today - bdy).days
        if 0 <= days_since <= 30:
            report.append(f"{u.chat_id} (BD {u.birthday}, {30 - days_since}d of first 30)")
    s.close()
    if report:
        try: await app.bot.send_message(ADMIN_CHAT_ID, "Within first 30 days after birthday:\n" + "\n".join(report))
        except Exception as e: logging.warning("Admin report fail: %s", e)

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setbirthday", set_birthday))
    app.add_handler(CommandHandler("done", done))
    app.add_handler(CommandHandler("mytask", mytask))
    app.add_handler(CommandHandler("summary", summary))

    sched = AsyncIOScheduler()
    sched.add_job(send_reminders, "cron", hour=0, minute=5, args=[app])
    sched.add_job(send_admin_report, "cron", hour=0, minute=10, args=[app])
    sched.start()
    logging.info("Scheduler started.")

    # IMPORTANT: clears any webhook so polling receives updates
    app.run_polling(drop_pending_updates=True, close_loop=False)

if __name__ == "__main__":
    main()
