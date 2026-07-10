"""DAG 3 — consolidation staging → curated (C19, assumé).

Coquille fine : appelle `consolidate_step` (un pas = une journée, filigrane
curated vs staging) toutes les minutes. Logique dans datalake.consolidation.
"""
from __future__ import annotations

from datetime import datetime

from _om_lineage import LINES, container
from airflow.decorators import dag, task

from datalake.consolidation import consolidate_step
from datalake.runner import checked


@dag(
    dag_id="consolidation_curated",
    description="Consolide une journée staging → curated (filigrane, une journée par minute).",
    schedule="* * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["c19", "consolidation", "curated"],
)
def consolidation_curated() -> None:
    # Lignage (C20) : staging.lineX → curated.line=lineX (partition Hive `line=`).
    @task(
        inlets=[container("staging", line, line) for line in LINES],
        outlets=[container("curated", f"line={line}", line) for line in LINES],
    )
    def consolider() -> str:
        return checked(consolidate_step())

    consolider()


consolidation_curated()
