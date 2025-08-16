# IPPT Reminder Telegram Bot — Interval Reminders + CSV/XLSX UPSERT

This build adds:
- Reminder interval (default **every 10 days**), enforced by the daily job.
- `/status` shows **Reminder interval** and **Next reminder date**.
- CSV/XLSX import uses **UPSERT** (insert or update) on `personnel_id`.
- CSV import auto-strips BOM; `.xlsx` supported via openpyxl.

## Env Vars
- `BOT_TOKEN` — from @BotFather
- `ADMIN_IDS` — comma-separated Telegram user IDs
- `TZ` — default `Asia/Singapore`
- `DB_PATH` — e.g., `/data/ippt.db`
- `REMINDER_INTERVAL_DAYS` — default `10`

## Deploy on Railway
1) Deploy with Dockerfile.
2) Set variables above.
3) Add a Volume mounted to `/data`.
4) Deploy. The bot runs in polling mode.

## Usage
- Admin: `/import_csv` → upload `.csv` or `.xlsx` (only `personnel_id` & `birthday` are used; extras ignored).
- Users: `/status` shows window, days left, reminder interval, and next reminder date.
