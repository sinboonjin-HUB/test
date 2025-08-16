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


## Impact on existing data
If you previously saved completions using the **calendar year**, the bot now expects
`completed_year` to equal the **window’s start year** (the year of the birthday in that window).
Example: Birthday Dec 15 → window starts **Dec 15, 2024** and ends **Mar 25, 2025**. A completion on
Jan 10, 2025 belongs to **2024**.

Use the migration script to align your data:

**Dry-run (no writes)**

```bash
# DB path from env
DB_PATH=/data/ippt.db python scripts/migrate_years.py
# or specify explicitly
python scripts/migrate_years.py --db /data/ippt.db
```

**Apply changes**

```bash
DB_PATH=/data/ippt.db python scripts/migrate_years.py --apply
# or
python scripts/migrate_years.py --db /data/ippt.db --apply
```

On Railway, run a one-off command for your service (with the Volume mounted at /data):

```bash
python scripts/migrate_years.py --apply
```


### New features
- `/report` now produces **one sheet per group** plus an **All** sheet.
- Users **cannot** submit deferment reasons; only admins can via `/defer_reason`.
- Admins can update birthdays with `/update_birthday <PERSONNEL_ID> <YYYY-MM-DD>`.

- New: `/report_group <GROUP>` exports a single Excel report for the specified group (case-insensitive). Use `No Group` for empty groups.
