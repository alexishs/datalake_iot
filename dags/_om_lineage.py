"""Helpers de lignage OpenMetadata (C20) — annotations `inlets`/`outlets` des DAGs.

Métadonnées **pures** : aucun effet à l'exécution. Le connecteur Airflow
d'OpenMetadata lit ces annotations depuis le DAG **sérialisé** et en déduit le
lignage entre conteneurs. Format attendu (classe `OMEntity` d'OpenMetadata) :
un dict `{"entity": "container", "fqn": …, "key": …}`. OpenMetadata regroupe
tous les inlets/outlets d'un DAG **par `key`** ; on utilise donc le nom de la
ligne comme clé pour apparier chaque source à sa cible (raw.lineA → staging.lineA).

Ce module n'est pas un DAG (préfixe `_`) : Airflow l'importe sans y trouver de DAG.
"""
from __future__ import annotations

SERVICE = "datalake_minio"
LINES = ["lineA", "lineB", "lineC", "lineD", "lineE"]


def container(bucket: str, path: str, key: str) -> dict:
    """Référence OMEntity d'un conteneur, au format `inlets`/`outlets` d'OpenMetadata.

    `path` est le segment sous `production_lines/` (p. ex. `lineA` ou `line=lineA`).
    `key` groupe la source et la cible d'une même arête de lignage.
    """
    return {
        "entity": "container",
        "fqn": f"{SERVICE}.{bucket}.production_lines/{path}",
        "key": key,
    }
