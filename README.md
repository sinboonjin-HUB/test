# IPPT Reminder Telegram Bot — Full-year Window + Reason-only Deferments

- **Window**: from each member's current birthday to their **next birthday** (exclusive).
- **Deferments**: reason-only and active immediately (no approve/reject). Reminders pause while a reason exists for the window.
- **Report**: Excel (.xlsx) with `days_left` / `days_overdue` and red-highlight for incomplete (no active deferment).
- **Groups**: optional `group` column in CSV/XLSX and `/add_personnel`.
- **Imports**: CSV/XLSX, BOM-safe, extra columns ignored.

## Commands

**User**
- `/verify <PERSONNEL_ID> <YYYY-MM-DD>`
- `/status`
- `/complete`
- `/uncomplete`
- `/defer <reason>`

**Admin**
- `/admin_help`
- `/add_personnel <ID> <YYYY-MM-DD> [GROUP]`
- `/import_csv` (then upload .csv/.xlsx with `personnel_id,birthday[,group]`)
- `/report` (Excel with red highlight)
- `/whoami`
- `/unlink_user <tokens>` (Telegram IDs **or** `personnel_id`s, mixed, multi)
- `/remove_personnel <ID or list>`
- `/admin_uncomplete <tokens> [WINDOW_START_YEAR]`
- `/defer_reason <tokens> [WINDOW_START_YEAR] -- <reason text>`  (sets active deferment)

## Deploy (Railway)
1) Deploy with Dockerfile.
2) Set env: `BOT_TOKEN`, `ADMIN_IDS`, `TZ=Asia/Singapore`, `DB_PATH=/data/ippt.db`, `REMINDER_INTERVAL_DAYS=10`.
3) Add a Volume mounted at `/data`.
4) Deploy.
