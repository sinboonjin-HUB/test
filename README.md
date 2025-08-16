# IPPT Reminder Telegram Bot — CSV/XLSX Import

This build adds:
- Accept extra columns (only needs `personnel_id` and `birthday`).
- Auto-strips BOM on CSV (`utf-8-sig`).
- Supports `.xlsx` import (first sheet).

## Deploy on Railway
1) Deploy this repo (Dockerfile).
2) Variables: `BOT_TOKEN`, `ADMIN_IDS`, `DB_PATH=/data/ippt.db`, `TZ=Asia/Singapore`.
3) Add a Volume mounted to `/data`.
4) Deploy. Use `/import_csv`, then upload `.csv` or `.xlsx`.

## Notes
- For `.xlsx`, dates typed as Excel dates are supported automatically.
- For CSV, keep dates as `YYYY-MM-DD`.
