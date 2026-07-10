"""DAG C20 — archivage : déplace les (ligne, mois) de plus de N mois de raw vers
archive/, purge staging/curated. Coquille fine (logique dans datalake.archive).
"""
from __future__ import annotations

from datetime import date, datetime

from _om_lineage import container
from airflow.decorators import dag, task

from datalake.archive import archive_month, mois_a_archiver
from datalake.runner import checked
from datalake.storage import get_s3_client


@dag(
    dag_id="archivage",
    description="Archive raw → archive/ (+ purge staging/curated) au-delà de 18 mois (démo).",
    # Cadence MENSUELLE : l'éligibilité se décide au mois (raw partitionné au mois,
    # seuil comparé en mois) — la population éligible ne change qu'au passage d'un
    # mois. Un @daily ferait ~30 exécutions à vide/mois. (À l'inverse des DAGs
    # fil-de-l'eau harmonisation/consolidation, pilotés par l'arrivée continue de
    # données, donc à la minute.) La suppression relève de l'ILM MinIO (continu).
    schedule="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["c20", "archive", "cycle-de-vie"],
)
def archivage() -> None:
    # Lignage (C20) : raw.lineE → archive.lineE (seule ligne archivée par la démo ;
    # les deux conteneurs doivent exister dans le catalogue pour tracer l'arête).
    @task(
        inlets=[container("raw", "lineE", "lineE")],
        outlets=[container("archive", "lineE", "lineE")],
    )
    def lister_mois() -> list[list]:
        return [list(t) for t in mois_a_archiver(get_s3_client(), date.today())]

    @task
    def archiver(mois: list) -> str:
        line, year, month = mois
        return checked(archive_month(get_s3_client(), line, year, month))

    archiver.expand(mois=lister_mois())


archivage()
