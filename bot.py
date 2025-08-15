import os
import psycopg2
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from datetime import datetime, timedelta
import asyncio

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
DATABASE_URL = os.getenv("DATABASE_URL")

# Connect to PostgreSQL
def db_connect():
    return psycopg2.connect(DATABASE_URL)

# Setup DB tables
def init_db():
    conn = db_connect()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name TEXT,
            birthday DATE,
            chat_id BIGINT,
            task_done BOOLEAN DEFAULT FALSE
        )
    """)
    conn.commit()
    conn.close()

# Register user
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Hi! What's your name?")
    context.user_data["step"] = "name"

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    step = context.user_data.get("step")
    text = update.message.text
    chat_id = update.message.chat_id

    if step == "name":
        context.user_data["name"] = text
        await update.message.reply_text("Please enter your birthday (YYYY-MM-DD)")
        context.user_data["step"] = "birthday"

    elif step == "birthday":
        try:
            bd = datetime.strptime(text, "%Y-%m-%d").date()
            conn = db_connect()
            c = conn.cursor()
            c.execute("""
                INSERT INTO users (name, birthday, chat_id) VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (context.user_data["name"], bd, chat_id))
            conn.commit()
            conn.close()
            await update.message.reply_text("Registered successfully! 🎉")
            context.user_data.clear()
        except ValueError:
            await update.message.reply_text("Invalid date format. Please use YYYY-MM-DD.")

# Mark task as done
async def done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    conn = db_connect()
    c = conn.cursor()
    c.execute("UPDATE users SET task_done = TRUE WHERE chat_id = %s", (chat_id,))
    conn.commit()
    conn.close()
    await update.message.reply_text("✅ Task marked as completed! You won't get more reminders.")

# Reminder job (TEST MODE: triggers every 30 seconds)
async def reminder_job(app):
    while True:
        today = datetime.now().date()
        conn = db_connect()
        c = conn.cursor()
        c.execute("SELECT id, name, birthday, chat_id, task_done FROM users")
        users = c.fetchall()

        for user_id, name, birthday, chat_id, task_done in users:
            # For test mode: treat birthday as 5 days from now
            bd_this_year = birthday.replace(year=today.year)
            if bd_this_year < today:
                bd_this_year = bd_this_year.replace(year=today.year + 1)

            days_left = (bd_this_year - today).days

            if not task_done:
                # TEST MODE: send reminder if days_left <= 5 and divisible by 1
                if 0 < days_left <= 5 and days_left % 1 == 0:
                    await app.bot.send_message(
                        chat_id,
                        f"Hi {name}, only {days_left} days left until your birthday 🎂. "
                        "Please complete your task before then!"
                    )
        conn.close()
        await asyncio.sleep(30)  # 30 sec check for testing

# Weekly admin report job (TEST MODE: every 60 sec)
async def admin_report_job(app):
    while True:
        today = datetime.now().date()
        conn = db_connect()
        c = conn.cursor()
        c.execute("SELECT name, birthday FROM users WHERE task_done = FALSE")
        pending_users = []
        for name, birthday in c.fetchall():
            bd_this_year = birthday.replace(year=today.year)
            if bd_this_year < today:
                bd_this_year = bd_this_year.replace(year=today.year + 1)
            days_left = (bd_this_year - today).days
            if days_left <= 3:  # Test threshold
                pending_users.append(f"{name} ({days_left} days left)")

        if pending_users:
            await app.bot.send_message(
                ADMIN_CHAT_ID,
                "⚠ TEST MODE: Users close to birthday & task not done:\n" + "\n".join(pending_users)
            )
        conn.close()
        await asyncio.sleep(60)  # check every 1 min for testing

# Main function
def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("done", done))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    app.job_queue.run_once(lambda _: asyncio.create_task(reminder_job(app)), when=0)
    app.job_queue.run_once(lambda _: asyncio.create_task(admin_report_job(app)), when=0)

    app.run_polling()

if __name__ == "__main__":
    main()
