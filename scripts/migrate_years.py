#!/usr/bin/env python3
"""
Migration: map calendar-year completions to the correct *window start year*.

Why?
- Older deployments may have saved `completed_year` as the *calendar year*
  (e.g., 2025 if completion happened in Jan 2025), but the bot now expects
  `completed_year` to equal the *start year of the relevant 100-day window*.
  Example: Birthday Dec 15 → 100-day window starts 2024-12-15 and ends 2025-03-25.
  A completion on 2025-01-10 belongs to window-start year **2024**.

What this script does
- Recomputes the correct window-start year from each user's birthday and
  `completed_at` timestamp.
- Updates `completions.year` and `users.completed_year` where needed.

Safety
- Runs in **dry-run mode** by default (no DB writes).
- Use `--apply` to write changes.

Usage
- With env `DB_PATH` set (recommended):  `python scripts/migrate_years.py --apply`
- Or specify DB explicitly:              `python scripts/migrate_years.py --db /data/ippt.db --apply`
"""
import argparse
import os
import sqlite3
from datetime import date, datetime, timedelta

def parse_date_strict(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def adjusted_birthday_for_year(bday: date, year: int) -> date:
    try:
        return date(year, bday.month, bday.day)
    except ValueError:
        if bday.month == 2 and bday.day == 29:
            return date(year, 2, 28)
        raise

WINDOW_DAYS = 100

def window_for_completion(bday: date, completed_on: date) -> int:
    """Return the window *start year* for the 100-day window that contains completed_on."""
    # The window tied to the birthday in completed_on.year
    start = adjusted_birthday_for_year(bday, completed_on.year)
    end = start + timedelta(days=WINDOW_DAYS)
    if start <= completed_on <= end:
        return start.year
    # If completion fell before that year's birthday, it belongs to previous year's window
    prev_start = adjusted_birthday_for_year(bday, completed_on.year - 1)
    prev_end = prev_start + timedelta(days=WINDOW_DAYS)
    if prev_start <= completed_on <= prev_end:
        return prev_start.year
    # Otherwise, try the next year's window (rare if timestamps are odd)
    next_start = adjusted_birthday_for_year(bday, completed_on.year + 1)
    next_end = next_start + timedelta(days=WINDOW_DAYS)
    if next_start <= completed_on <= next_end:
        return next_start.year
    # Fallback: default to the birthday year of completed_on
    return start.year

def main():
    ap = argparse.ArgumentParser(description="Migrate completed_year to window start year.")
    ap.add_argument("--db", default=os.getenv("DB_PATH", "ippt.db"), help="Path to SQLite DB (default: env DB_PATH or ippt.db)")
    ap.add_argument("--apply", action="store_true", help="Apply changes (otherwise dry-run)")
    ap.add_argument("--verbose", action="store_true", help="Verbose output")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    # 1) Fix completions.year
    cur.execute(        """        SELECT c.id, c.telegram_id, c.year, c.completed_at, p.birthday
          FROM completions c
          JOIN users u   ON u.telegram_id = c.telegram_id
          JOIN personnel p ON p.personnel_id = u.personnel_id
        """    )
    comp_rows = cur.fetchall()
    comp_updates = 0
    for cid, tid, year, completed_at, bday_str in comp_rows:
        try:
            completed_on = datetime.fromisoformat(completed_at).date()
        except Exception:
            # If badly formatted, skip
            continue
        bday = parse_date_strict(bday_str)
        new_year = window_for_completion(bday, completed_on)
        if new_year != year:
            comp_updates += 1
            if args.verbose or not args.apply:
                print(f"[completions] id={cid} telegram_id={tid} {year} -> {new_year} (completed_at={completed_at}, bday={bday_str})")
            if args.apply:
                cur.execute("UPDATE completions SET year=? WHERE id=?", (new_year, cid))

    # 2) Fix users.completed_year (based on users.completed_at when present)
    cur.execute(        """        SELECT u.telegram_id, u.completed_year, u.completed_at, p.birthday
          FROM users u
          JOIN personnel p ON p.personnel_id = u.personnel_id
         WHERE u.completed_year IS NOT NULL
        """    )
    user_rows = cur.fetchall()
    user_updates = 0
    for tid, uyear, ucompleted_at, bday_str in user_rows:
        if not ucompleted_at:
            continue
        try:
            completed_on = datetime.fromisoformat(ucompleted_at).date()
        except Exception:
            continue
        bday = parse_date_strict(bday_str)
        new_year = window_for_completion(bday, completed_on)
        if new_year != uyear:
            user_updates += 1
            if args.verbose or not args.apply:
                print(f"[users] telegram_id={tid} {uyear} -> {new_year} (completed_at={ucompleted_at}, bday={bday_str})")
            if args.apply:
                cur.execute("UPDATE users SET completed_year=? WHERE telegram_id=?", (new_year, tid))

    if args.apply:
        conn.commit()

    print("\nSummary:")
    print(f"  completions rows needing update: {comp_updates}")
    print(f"  users rows needing update:       {user_updates}")
    print(f"  mode: {'APPLY' if args.apply else 'DRY-RUN'}")

if __name__ == "__main__":
    main()
