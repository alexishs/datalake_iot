"""Ingestion brute : dépôt des CSV source dans `raw/` + intégrité MD5 (C19).

Conforme au contrat docs/architecture.md §12 : clé `production_lines/{line}/year=YYYY/month=MM/`,
`line` du nom de fichier, `year`/`month` des données (garde-fou « un seul mois »), dépôt
byte-identique, MD5 via ETag, idempotence + cascade d'invalidation vers `staging`.
"""
from __future__ import annotations

from pathlib import Path

import polars as pl
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from datalake.explore import TS_FORMAT, csv_paths, line_id
from datalake.runner import Result, run
from datalake.storage import delete_keys, delete_prefix, get_s3_client, list_keys, md5_file

RAW_BUCKET = "raw"
STAGING_BUCKET = "staging"


def partition_prefix(line: str, year: int, month: int) -> str:
    """Retourne le préfixe de partition S3 pour une ligne, une année et un mois donnés."""
    return f"production_lines/{line}/year={year}/month={month:02d}/"


def partition_key(path: Path | str) -> tuple[str, str]:
    """Retourne (prefix, key) pour un fichier. Lève ValueError si >1 mois (garde-fou)."""
    path = Path(path)
    df = pl.read_csv(path)
    tcol = next(c for c in df.columns if c.lower() == "timestamp")
    ts = df.get_column(tcol).str.to_datetime(TS_FORMAT)
    years = ts.dt.year().unique().to_list()
    months = ts.dt.month().unique().to_list()
    if len(years) != 1 or len(months) != 1:
        raise ValueError(
            f"{path.name} : couvre plusieurs (année, mois) — years={years}, months={months}. "
            "Ingestion refusée (garde-fou : un fichier = un seul mois)."
        )
    prefix = partition_prefix(line_id(path.name), years[0], months[0])
    return prefix, prefix + path.name


def _remote_etag(client: BaseClient, bucket: str, key: str) -> str | None:
    """ETag (= MD5 pour un upload simple) de l'objet, ou None s'il est absent."""
    try:
        return client.head_object(Bucket=bucket, Key=key)["ETag"].strip('"')
    except ClientError:
        return None


def ingest_file(path: Path | str, client: BaseClient | None = None) -> Result:
    """Dépose un CSV dans `raw/` (byte-identique, MD5) ; idempotent + cascade vers `staging`."""
    path = Path(path)
    client = client or get_s3_client()
    prefix, key = partition_key(path)               # garde-fou inclus
    local_md5 = md5_file(path)

    if _remote_etag(client, RAW_BUCKET, key) == local_md5:
        return Result(path.name, "inchangé (MD5)", True)

    # (ré)import — ORDRE SÛR : on n'invalide l'aval qu'une fois `raw` confirmé.
    with open(path, "rb") as f:
        etag = client.put_object(Bucket=RAW_BUCKET, Key=key, Body=f)["ETag"].strip('"')
    if etag != local_md5:
        return Result(path.name, "ÉCHEC MD5", False)

    # nettoyage `raw` : retirer d'éventuels autres objets de la partition (fichier renommé)
    delete_keys(client, RAW_BUCKET, [k for k in list_keys(client, RAW_BUCKET, prefix) if k != key])
    # cascade : invalider la même (ligne, mois) en `staging` APRÈS confirmation de `raw`
    delete_prefix(client, STAGING_BUCKET, prefix)
    return Result(path.name, "ré-importé", True)


def main() -> int:
    """Dépose tous les CSV de `data/` dans `raw/` (rapport + code de sortie)."""
    client = get_s3_client()
    return run(lambda p: ingest_file(p, client), csv_paths(), "Ingestion → raw/")


if __name__ == "__main__":
    raise SystemExit(main())
