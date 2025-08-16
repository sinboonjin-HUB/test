# IPPT Reminder Telegram Bot — Railway Deployment

This repo contains a Telegram bot that:
- Lets admins manage a database of personnel (unique ID + birthday)
- Public users verify themselves
- Sends reminders to complete IPPT within **100 days after their birthday**
- Users can mark `/complete` to stop reminders for the year
- Admins can generate completion reports

## Deploy on Railway (Polling)

### 1) Create a project on Railway
- Go to Railway (https://railway.app), create a new project, choose “Deploy from GitHub” or “New -> Empty Project” and upload this repo.
- Railway will detect the `Dockerfile` and build automatically.

### 2) Add Environment Variables
Set these in **Variables**:
- `BOT_TOKEN` — from @BotFather
- `ADMIN_IDS` — comma-separated Telegram IDs for admins (e.g., `12345678,87654321`)
- `TZ` (optional) — default `Asia/Singapore`
- `DB_PATH` — `/data/ippt.db` (default). This folder is persisted if you add a volume.

### 3) Add a Persistent Volume (recommended)
In the **Storage/Volumes** section, add a volume and mount it to `/data` so your SQLite DB persists across deploys.

### 4) Deploy
- Click **Deploy**. Once running, your bot will start in polling mode.
- Invite your bot to a chat or DM it. Run `/start`.

> If you want **webhooks** instead of polling, you’ll need a public HTTPS endpoint. On Railway, add a `PORT` environment variable and modify `app.py` to use webhooks. The provided `Dockerfile` and `app.py` default to polling for simplicity.

## Local Development

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .\.venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env        # fill in values
python app.py
```

## CSV Template

Create a CSV (`personnel.csv`) like:

```csv
personnel_id,birthday
A12345,1995-07-14
B10086,1988-02-29
C90001,2000-12-05
```

Admins can import with `/import_csv` and then upload the file.

## License
MIT
