# KAM_Scores (Backend)

env -> pandas

FastAPI backend to ingest monthly project datasets (TSV) and compute KAM scores Aug–Dec 2025,
with monthly and cumulative summaries.

## Features
- Upload monthly dataset (tab-separated `.txt`) via `/upload?month=YYYY-MM`
- Compute scores per KAM per month via `/scores?from=2025-08&to=2025-12`
- Simple HTML dashboard at `/`
- SQLite locally; switch to PostgreSQL via `DATABASE_URL`
- Docker-ready and Dokku-friendly

## Local Run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL=sqlite:///./kam_scores.db
uvicorn app.main:app --reload
```

Open http://127.0.0.1:8000

## Upload
Use the form at `/` or curl:
```bash
curl -F "file=@projects_2025-08.txt" "http://127.0.0.1:8000/upload?month=2025-08"
```

## Scores
```bash
curl "http://127.0.0.1:8000/scores?from=2025-08&to=2025-12"
```

## Docker (local)
```bash
docker build -t kam-scores .
docker run --rm -it -p 8000:8000 -e DATABASE_URL=sqlite:///./kam_scores.db kam-scores
```

## Dokku (subdomain split)
- Create two apps:
  - Frontend: `score.athenalabo.com`
  - Backend API: `api.score.athenalabo.com`

Backend setup example:
```bash
dokku apps:create api-score
dokku config:set api-score APP_ENV=prod
dokku postgres:create kamscores-db
dokku postgres:link kamscores-db api-score
dokku domains:add api-score api.score.athenalabo.com
```

Deploy (subdirectory):
```bash
git init && git add . && git commit -m "init"
git remote add dokku dokku@YOUR_VPS_IP:api-score
git push dokku main:master   # or main:main
```

**Persistent Postgres on Dokku:** The dokku-postgres plugin stores data on the VPS disk.
Backups: `dokku postgres:backup` or VPS snapshots.
