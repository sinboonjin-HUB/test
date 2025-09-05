# IPPT Reminder Bot — Bare Date in /admin_complete + Window-only /uncomplete

## Environment (Railway)
- BOT_TOKEN
- ADMIN_IDS (comma-separated Telegram IDs)
- DB_PATH=/data/ippt.db
- TZ=Asia/Singapore
- REMINDER_INTERVAL_DAYS=10 (optional)

## Volume
Mount your Railway volume to `/data` so the DB persists.

## Notes
- `/admin_complete` accepts `--date YYYY-MM-DD` *or* a bare `YYYY-MM-DD` anywhere.
- `/uncomplete` only works when the user is **inside the active 100-day window**.
