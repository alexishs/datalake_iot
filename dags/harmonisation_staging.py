"""DAG 2 — harmonisation raw → staging (C19, fil-de-l'eau).

Coquille fine : appelle `harmonize_step` (un pas = une journée, choisie par le
filigrane) toutes les minutes. Toute la logique vit dans datalake.harmonization.
"""
from __future__ import annotations

from datetime import datetime

from _om_lineage import LINES, container
from airflow.decorators import dag, task

from datalake.harmonization import harmonize_step
from datalake.runner import checked


@dag(
    dag_id="harmonisation_staging",
    description="Harmonise une journée raw → staging (filigrane, une journée par minute).",
    schedule="* * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["c19", "harmonisation", "staging"],
)
def harmonisation_staging() -> None:
    # Lignage (C20) : raw.lineX → staging.lineX (appariement par `key`).
    @task(
        inlets=[container("raw", line, line) for line in LINES],
        outlets=[container("staging", line, line) for line in LINES],
    )
    def harmoniser() -> str:
        return checked(harmonize_step())

    harmoniser()


harmonisation_staging()
