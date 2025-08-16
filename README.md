# IPPT Reminder Telegram Bot — Interval + Import UPSERT + Delete Commands

Features:
- Reminder interval (default **every 10 days**) with next reminder shown in `/status`.
- Daily job sends only on interval days within the 100-day window.
- CSV/XLSX import with **UPSERT** (insert or update) on `personnel_id`.
- CSV import auto-strips BOM; `.xlsx` supported via openpyxl (Excel dates OK).
- **New admin delete commands:** `/unlink_user <TELEGRAM_ID>` and `/remove_personnel <PERSONNEL_ID>`.

## Env Vars
- `BOT_TOKEN` — from @BotFather
- `ADMIN_IDS` — comma-separated Telegram user IDs
- `TZ` — default `Asia/Singapore`
- `DB_PATH` — e.g., `/data/ippt.db`
- `REMINDER_INTERVAL_DAYS` — default `10`

## Deploy on Railway
1) Deploy with Dockerfile.
2) Set variables above.
3) Add a Volume mounted to `/data` (so DB persists).
4) Deploy. The bot runs in polling mode.

## Admin usage
- `/import_csv` then upload `.csv` or `.xlsx` (only `personnel_id` & `birthday` used; extra columns ignored).
- `/unlink_user 123456789` — removes a Telegram user’s link & completions (keeps personnel row).
- `/remove_personnel ABC123` — removes personnel + any linked users & completions.
