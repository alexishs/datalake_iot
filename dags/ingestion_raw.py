"""DAG 1 — ingestion brute : dépose les CSV de data/ dans raw/ (C19).

Coquille fine : aucune logique métier ici. Une tâche par CSV (dynamic task
mapping) appelle `ingest_file` — exactement la fonction du CLI
`python -m datalake.ingestion`. Idempotent (skip si MD5 identique).
"""
from __future__ import annotations

from datetime import datetime

from _om_lineage import LINES, container
from airflow.decorators import dag, task

from datalake.explore import csv_paths
from datalake.ingestion import ingest_file
from datalake.runner import checked

DATA_DIR = "/opt/airflow/data"  # ./data monté ici (cf. compose.yaml)


@dag(
    dag_id="ingestion_raw",
    description="Dépose les CSV de data/ dans raw/ (byte-identique + MD5, idempotent).",
    schedule=None,  # déclenché manuellement (les CSV arrivent par `download`)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["c19", "ingestion", "raw"],
)
def ingestion_raw() -> None:
    # Lignage (C20) : ce DAG produit les conteneurs `raw` (source externe = CSV,
    # sans entité amont dans le catalogue). Métadonnées lues par OpenMetadata.
    @task(outlets=[container("raw", line, line) for line in LINES])
    def lister_csv() -> list[str]:
        return [str(p) for p in csv_paths(DATA_DIR)]

    @task
    def ingerer(path: str) -> str:
        return checked(ingest_file(path))

    ingerer.expand(path=lister_csv())


ingestion_raw()
