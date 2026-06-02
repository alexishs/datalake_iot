"""Smoke test de connectivité MinIO.

    python -m datalake

À lancer DANS le dev container (ou via le debugger VSCode) : comme il tourne
dans le réseau Docker, `minio:9000` résout exactement comme dans Airflow.
"""
from __future__ import annotations

from datalake.storage import get_s3_client


def main() -> None:
    client = get_s3_client()
    buckets = [b["Name"] for b in client.list_buckets()["Buckets"]]
    print("Connexion MinIO OK. Buckets :", buckets)


if __name__ == "__main__":
    main()
