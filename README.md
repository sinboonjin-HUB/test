# IPPT Reminder Telegram Bot (100-day window)

Features:
- Public verification against admin-loaded personnel DB (ID + birthday).
- Reminders from **birthday → +100 days** (inclusive), every `REMINDER_INTERVAL_DAYS` (default 10).
- Users can mark complete: `/complete [YYYY-MM-DD]` (date must be within the *current* window).
- **Admins only** can set deferment reasons, reset deferments, complete on behalf (with date override), uncomplete, edit birthdays, import CSV/XLSX, and export reports.
- Deferments are tied to **personnel_id** (not Telegram ID). Completion & deferment **auto-reset at window end**.
- `/status` shows:
  - ✅ Completed
  - ⛔️ Defer — <reason>
  - ⏳ N day(s) left (while in window)
  - 💤 Window not open yet — starts <date> (before window)
  - **Window closed — next window starts <date>** (after window)

## Quick deploy (Railway)

1. Create a new service from this repo/ZIP.
2. Add **Environment Variables**:
   - `BOT_TOKEN` – your Telegram bot token
   - `ADMIN_IDS` – comma-separated Telegram user IDs (e.g. `123,456`)
   - `DB_PATH` – `/data/ippt.db`
   - `TZ` – `Asia/Singapore`
   - `REMINDER_INTERVAL_DAYS` – `10` (optional)
3. Add a **Volume** and mount to `/data`.
4. Deploy.

## CSV/XLSX import
- Use `/import_csv`, then upload a file `personnel_id,birthday[,group]` (ignores extra columns, BOM-safe; `.csv` or `.xlsx`).
- Dates must be `YYYY-MM-DD`.

## Reports
- `/report` → Excel with **All** sheet + one sheet per group (red rows = not completed & no deferment).
- `/report_group <GROUP>` → single sheet export.

## Admin commands
- `/admin_help` – show all admin commands.
- `/add_personnel <ID> <YYYY-MM-DD> [GROUP]`
- `/update_birthday <PERSONNEL_ID> <YYYY-MM-DD>`
- `/defer_reason <tokens> [YEAR] -- <reason>`
- `/defer_reset <tokens> [YEAR]`
- `/admin_complete <tokens> [YEAR] [--date YYYY-MM-DD]` *(overrides completion date for that window)*
- `/admin_uncomplete <tokens> [YEAR]`
- `/unlink_user <tokens>`
- `/remove_personnel <ID or comma-list>`
- `/import_csv` then upload CSV/XLSX
- `/report`, `/report_group <GROUP>`
- `/defer_audit` – CSV of migrated (if any) + current deferments

*Tokens can be Telegram IDs or personnel IDs (mixed; comma/space separated).*
