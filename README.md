# IPPT Reminder Telegram Bot — Railway (Fixed)

Changes in this build:
- Uses `python-telegram-bot[job-queue]==21.6` so scheduler works.
- Fixes CSV filter to `filters.Document.FileExtension("csv")`.
- Schedules daily job with `datetime.time(tzinfo=TZINFO)`.
- Uses `await app.run_polling()` lifecycle (clean shutdown).

## Deploy on Railway
1) Create a new project and deploy this repo (Dockerfile based).
2) Variables:
   - `BOT_TOKEN`
   - `ADMIN_IDS`
   - `TZ` = `Asia/Singapore` (optional)
   - `DB_PATH` = `/data/ippt.db`
3) Add a Volume and mount it to `/data`.
4) Deploy. Bot runs in polling mode.

## Local
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # fill values
python app.py
```
