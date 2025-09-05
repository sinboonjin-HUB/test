# IPPT Reminder Bot — v2

## Environment (Railway)
- BOT_TOKEN
- ADMIN_IDS (comma-separated Telegram IDs)
- DB_PATH=/data/ippt.db
- TZ=Asia/Singapore
- REMINDER_INTERVAL_DAYS=10 (optional)

## Volume
Mount your Railway volume to `/data` so the DB persists.

## Commands
/start, /help, /setbirthday YYYY-MM-DD, /summary, /complete [--date YYYY-MM-DD],
/uncomplete (window-only), /admin_complete <user_id> [YYYY-MM-DD], /export, /import,
/admin_add <id>, /admin_remove <id>.
