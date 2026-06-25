"""
Generates a .env file with all secrets required by docker-compose.yaml.

Run once before starting the stack:
    AIRFLOW_UID=$(id -u) python scripts/generate_env.py

Existing .env will not be overwritten unless --force is passed:
    AIRFLOW_UID=$(id -u) python scripts/generate_env.py --force
"""

import argparse
import os
import secrets
import sys
from pathlib import Path

from cryptography.fernet import Fernet

ENV_PATH = Path(".env")

POSTGRES_USER = "airflow"
POSTGRES_DB   = "airflow"


def generate_env_contents(postgres_password: str, fernet_key: str, jwt_secret: str, pgadmin_password: str, airflow_uid: int) -> str:
    return f"""\
# --- Airflow UID (run: AIRFLOW_UID=$(id -u) python scripts/generate_env.py) ---
AIRFLOW_UID={airflow_uid}

# --- Postgres ---
POSTGRES_USER={POSTGRES_USER}
POSTGRES_PASSWORD={postgres_password}
POSTGRES_DB={POSTGRES_DB}

# --- Airflow core ---
FERNET_KEY={fernet_key}
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://{POSTGRES_USER}:{postgres_password}@postgres/{POSTGRES_DB}
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://{POSTGRES_USER}:{postgres_password}@postgres/{POSTGRES_DB}
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0

# --- Airflow API auth ---
AIRFLOW__API_AUTH__JWT_SECRET={jwt_secret}
AIRFLOW__API_AUTH__JWT_ISSUER=airflow

# --- Airflow web UI ---
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# --- pgAdmin ---
PGADMIN_EMAIL=admin@admin.com
PGADMIN_PASSWORD={pgadmin_password}
"""


def main() -> None:
    argument_parser = argparse.ArgumentParser(description=__doc__)
    argument_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing .env file",
    )
    arguments = argument_parser.parse_args()

    if ENV_PATH.exists() and not arguments.force:
        print(f"[SKIP] {ENV_PATH} already exists. Use --force to overwrite.")
        sys.exit(0)

    airflow_uid = int(os.environ.get("AIRFLOW_UID", 50000))

    postgres_password = secrets.token_urlsafe(32)
    fernet_key        = Fernet.generate_key().decode()
    jwt_secret        = secrets.token_urlsafe(32)
    pgadmin_password  = secrets.token_urlsafe(16)

    ENV_PATH.write_text(generate_env_contents(
        postgres_password=postgres_password,
        fernet_key=fernet_key,
        jwt_secret=jwt_secret,
        pgadmin_password=pgadmin_password,
        airflow_uid=airflow_uid,
    ))
    print(f"[OK] Generated {ENV_PATH} (AIRFLOW_UID={airflow_uid})")


if __name__ == "__main__":
    main()
