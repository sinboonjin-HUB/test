# IPPT Reminder Telegram Bot — Deferment + XLSX Report + Groups + Uncomplete

### New in this build
- **Deferment workflow**
  - Users: `/defer <reason>` creates/updates a *pending* deferment request for the current year.
  - Admins:
    - `/defer_approve <tokens> [YEAR]`
    - `/defer_reject <tokens> [YEAR]`
    - `/defer_reason <tokens> [YEAR] -- <reason text>`
  - Reminders are **paused** for users with an **approved** deferment.
  - `/status` shows deferment status & reason.
  - `/report` now includes `deferment_status` and `deferment_reason`.

- **Report as Excel (.xlsx) with formatting**
  - Columns include: group, verified, window, completion, **days_left**, **days_overdue**, deferment status/reason.
  - Rows are **highlighted in red** when the user **has not completed** and does **not** have an **approved** deferment.
  - File: `ippt_report_<year>.xlsx`

- Everything else retained
  - Interval reminders (default **every 10 days**) with “Next reminder” shown in `/status`.
  - CSV/XLSX import with UPSERT and optional **group** column.
  - Multi-ID `/unlink_user` (by Telegram ID **or** personnel_id) & `/remove_personnel`.

## CSV/XLSX columns
Required: `personnel_id`, `birthday` (YYYY-MM-DD)  
Optional: `group`/`group_name`/`grp`

## Admin quick reference
- `/report` — Excel with red-highlighted rows for incomplete (no approved deferment).
- `/defer_approve 719B, 123B` or `/defer_approve 123456 2025`
- `/defer_reject 719B`
- `/defer_reason 719B -- long hospitalisation leave`
