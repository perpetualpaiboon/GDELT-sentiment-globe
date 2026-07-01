# Setup

**Requirements:** Docker Desktop, Python 3.10+

**1. Clone the repository**
```bash
git clone https://github.com/perpetualpaiboon/gdelt-sentiment-globe
cd gdelt-sentiment-globe
```

**2. Create virtual environment**
```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
```

**3. Install Python dependencies for setup scripts**
```bash
pip install -r requirements-dev.txt
```

**4. Download static reference data**
```bash
python scripts/download_static_data.py
```

**5. Generate `.env` file**
```bash
AIRFLOW_UID=$(id -u) python scripts/generate_env.py
```

**6. Start all services**
```bash
docker compose up -d
```

**7. Wait for Airflow to initialize, then open the UI**

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | airflow / airflow |
| Dashboard | http://localhost:8501 | - |
| pgAdmin | http://localhost:5050 | admin@admin.com / your `PGADMIN_PASSWORD` from `.env` |

**8. Register the Postgres connection in Airflow**
```bash
docker exec gdelt-sentiment-globe-airflow-apiserver-1 airflow connections add postgres_default \
  --conn-type postgres --conn-host postgres --conn-login airflow \
  --conn-password <your-POSTGRES_PASSWORD from .env> --conn-schema airflow --conn-port 5432
```

**8a. (Optional) Connect pgAdmin to the database**

1. Open http://localhost:5050
2. Login: `admin@admin.com` / your `PGADMIN_PASSWORD` from `.env`
3. Right-click **Servers -> Register -> Server**
4. **General tab** - Name: anything (e.g. `gdelt`)
5. **Connection tab** - Host: `postgres`, Port: `5432`, Database: `airflow`, Username: `airflow`, Password: your `POSTGRES_PASSWORD`
6. Click **Save**

**9. Enable the automated pipeline**

The DAG is paused by default. To enable the daily schedule:

1. Open http://localhost:8080
2. Find the `gdelt_gkg_daily` DAG
3. Toggle the pause button on the left to unpause it

Once unpaused, the DAG runs automatically every day at 20:00 UTC, processing the previous day's data. The dashboard will update once the run completes.

**9a. (Optional) Manually trigger the pipeline for a specific date**

In the Airflow UI, trigger the `gdelt_gkg_daily` DAG with:
```json
{ "target_date": "2025-06-18" }
```

If `target_date` is not provided, the DAG defaults to yesterday's date in UTC.
