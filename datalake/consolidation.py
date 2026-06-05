"""Consolidation staging → curated (C19, 3ᵉ DAG assumé — cf. architecture §3.3/§11).

Réunit (FUSIONNE) les 5 lignes dans la table unifiée curated : `line` y est une
**partition Hive** (`line=lineX`), à la différence de staging (segment simple
`lineX`). Layout : `production_lines/line=lineX/year=/month=/day=/` (racine
`production_lines` homogène avec staging ; bucket distinct). Même patron que
l'harmonisation : un pas = une journée, filigrane curated vs staging.
"""
from __future__ import annotations

import io
from datetime import date

import polars as pl
from botocore.client import BaseClient

from datalake.harmonization import (
    STAGING_BUCKET,
    days_from_paths,
    staging_day_prefix,
    staging_days,
)
from datalake.runner import Result, drainer, filigrane, pas
from datalake.storage import delete_prefix, get_s3_client, list_keys

CURATED_BUCKET = "curated"


def curated_day_prefix(line: str, year: int, month: int, day: int) -> str:
    """Préfixe de partition curated (jour) : `production_lines/line=lineX/year=/month=/day=/`.

    Table **unifiée** : `line` est une **partition Hive** (`line=`), marquant la
    fusion des 5 lignes — à la différence de staging (segment simple `lineX`).
    """
    return f"production_lines/line={line}/year={year}/month={month:02d}/day={day:02d}/"


def _curated_days(client: BaseClient) -> dict[str, set[date]]:
    """Jours présents par ligne dans curated (depuis les chemins, même layout que staging)."""
    return days_from_paths(client, CURATED_BUCKET, "production_lines/", line_index=1)


def jour_a_traiter(client: BaseClient) -> date | None:
    """Plus ancien (ligne, jour) présent dans staging mais absent de curated (filigrane)."""
    return filigrane(staging_days(client), _curated_days(client))


def _read_staging_day(client: BaseClient, line: str, jour: date) -> pl.DataFrame:
    """Lit la partition staging d'un (ligne, jour) en DataFrame."""
    prefix = staging_day_prefix(line, jour.year, jour.month, jour.day)
    key = next(k for k in list_keys(client, STAGING_BUCKET, prefix))
    body = client.get_object(Bucket=STAGING_BUCKET, Key=key)["Body"].read()
    return pl.read_parquet(io.BytesIO(body))


def consolidate_day(client: BaseClient, jour: date) -> list[Result]:
    """Consolide `jour` pour chaque ligne l'ayant en staging mais pas en curated."""
    stg, cur = staging_days(client), _curated_days(client)
    resultats: list[Result] = []
    for line, jours in stg.items():
        if jour not in jours or jour in cur.get(line, set()):
            continue
        df = _read_staging_day(client, line, jour)
        prefix = curated_day_prefix(line, jour.year, jour.month, jour.day)
        delete_prefix(client, CURATED_BUCKET, prefix)        # idempotence
        buf = io.BytesIO()
        df.write_parquet(buf)
        client.put_object(Bucket=CURATED_BUCKET, Key=prefix + "part.parquet", Body=buf.getvalue())
        resultats.append(Result(f"{line} {jour}", f"{df.height} ligne(s) → curated", True))
    return resultats


def consolidate_step(client: BaseClient | None = None) -> Result:
    """Traite la plus ancienne journée en attente (un pas). 'à jour' s'il n'y a rien."""
    client = client or get_s3_client()
    return pas(client, jour_a_traiter, consolidate_day, CURATED_BUCKET)


def main() -> int:
    """Draine staging → curated. Lançable en CLI."""
    return drainer(get_s3_client(), consolidate_step, "Consolidation staging → curated")


if __name__ == "__main__":
    raise SystemExit(main())
