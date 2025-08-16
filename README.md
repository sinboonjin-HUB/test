# IPPT Reminder Telegram Bot — 100‑day Window + Reason‑only Deferments

- **Window:** from birthday (inclusive) to **birthday + 100 days** (inclusive).
- **Status categories:** Completed / 0–100 days left to complete / Overdue by N days / Defer.
- **Deferments:** reason‑only (user `/defer`, admin `/defer_reason`) — active immediately, pauses reminders.
- **Report:** Excel (.xlsx) with `days_left` / `days_overdue` and red rows for “not completed & no active deferment”.
- **Groups:** optional `group` column in CSV/XLSX and `/add_personnel`.
- **Imports:** CSV/XLSX, BOM‑safe, extra columns ignored.

## Commands

**User**
- `/verify <PERSONNEL_ID> <YYYY-MM-DD>`
- `/status` (shows IPPT Status with days left/overdue)
- `/complete`
- `/uncomplete`
- `/defer <reason>`

**Admin**
- `/admin_help`
- `/add_personnel <ID> <YYYY-MM-DD> [GROUP]`
- `/import_csv` then upload .csv/.xlsx with `personnel_id,birthday[,group]`
- `/report` (Excel with red highlight)
- `/whoami`
- `/unlink_user <tokens>` (Telegram IDs **or** `personnel_id`s, mixed, multi)
- `/remove_personnel <ID or list>`
- `/admin_uncomplete <tokens> [WINDOW_START_YEAR]`
- `/defer_reason <tokens> [WINDOW_START_YEAR] -- <reason text>`  (sets active deferment)

## Deploy (Railway)
1) Set env: `BOT_TOKEN`, `ADMIN_IDS`, `TZ=Asia/Singapore`, `DB_PATH=/data/ippt.db`, `REMINDER_INTERVAL_DAYS=10`.
2) Add a Volume and mount at `/data`.
3) Deploy.
