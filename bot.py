import os
import logging
from datetime import datetime, timedelta
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from sqlalchemy import create_engine, Column, Integer, String, Date, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

BOT_TOKEN = os.getenv("BOT_TOKEN", "8468290286:AAGOf234scakMkfJsC1BU-3Zw4jf5Dqt4o8")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "635939460"))
DATABASE_URL = os.getenv("DATABASE_URL")

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    chat_id = Column(Integer, nullable=False)
    birthday = Column(Date, nullable=False)
    task_done = Column(Boolean, default=False)

engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hi! Send /setbirthday YYYY-MM-DD to register your birthday."
    )

async def set_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session = Session()
    try:
        if len(context.args) != 1:
            await update.message.reply_text("Usage: /setbirthday YYYY-MM-DD")
            return

        birthday = datetime.strptime(context.args[0], "%Y-%m-%d").date()

        user = session.query(User).filter_by(chat_id=update.effective_chat.id).first()
        if user:
            user.birthday = birthday
            user.task_done = False
        else:
            user = User(chat_id=update.effective_chat.id, birthday=birthday)
            session.add(user)

        session.commit()
        await update.message.reply_text(f"Birthday set to {birthday}. Reminders will start 100 days before.")

    except ValueError:
        await update.message.reply_text("Invalid date format. Use YYYY-MM-DD.")
    finally:
        session.close()

async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session = Session()
    user = session.query(User).filter_by(chat_id=update.effective_chat.id).first()
    if user:
        user.task_done = True
        session.commit()
        await update.message.reply_text("Great! Task marked as done. No more reminders.")
    else:
        await update.message.reply_text("You haven't set your birthday yet.")
    session.close()

async def check_reminders(context: ContextTypes.DEFAULT_TYPE):
    session = Session()
    today = datetime.now().date()

    for user in session.query(User).filter_by(task_done=False).all():
        reminder_start = user.birthday - timedelta(days=100)
        if reminder_start <= today <= user.birthday:
            days_left = (user.birthday - today).days
            if days_left % 10 == 0:
                try:
                    await context.bot.send_message(
                        chat_id=user.chat_id,
                        text=f"Reminder: {days_left} days left to complete your task! Send /done when finished."
                    )
                except Exception as e:
                    logging.error(f"Failed to send reminder to {user.chat_id}: {e}")
    session.close()

async def send_admin_report(context: ContextTypes.DEFAULT_TYPE):
    session = Session()
    today = datetime.now().date()
    report_users = []
    for user in session.query(User).filter_by(task_done=False).all():
        if timedelta(days=0) <= (user.birthday - today) <= timedelta(days=30):
            report_users.append(f"User {user.chat_id} - Birthday: {user.birthday}")

    if report_users:
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text="📋 Pending tasks (within 30 days):\n" + "\n".join(report_users)
        )
    session.close()

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("setbirthday", set_birthday))
    app.add_handler(CommandHandler("done", done))

    app.job_queue.run_daily(check_reminders, time=datetime.min.time())
    app.job_queue.run_daily(send_admin_report, time=datetime.min.time())

    app.run_polling()

if __name__ == "__main__":
    main()
