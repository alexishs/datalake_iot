"""Cycle de vie (C20) : archive les (ligne, mois) anciennes de raw vers archive/,
purge les couches dérivées staging/curated. La suppression finale relève de l'ILM
MinIO (expiration). Réintégration : remettre l'objet dans raw relance le filigrane.
"""
from __future__ import annotations

from datetime import date

from botocore.client import BaseClient

from datalake.ingestion import (
    CURATED_BUCKET,
    RAW_BUCKET,
    STAGING_BUCKET,
    curated_partition_prefix,
    partition_prefix,
)
from datalake.runner import Result, run
from datalake.storage import delete_keys, delete_prefix, get_s3_client, list_keys

ARCHIVE_BUCKET = "archive"


def _mois_index(year: int, month: int) -> int:
    """Index absolu du mois (pour comparer/soustraire des mois simplement)."""
    return year * 12 + (month - 1)


def _raw_months(client: BaseClient) -> set[tuple[str, int, int]]:
    """(ligne, année, mois) présents dans raw, déduits des chemins."""
    out: set[tuple[str, int, int]] = set()
    for k in list_keys(client, RAW_BUCKET, "production_lines/"):
        parts = k.split("/")
        try:
            line = parts[1]
            year = int(next(p for p in parts if p.startswith("year=")).split("=")[1])
            month = int(next(p for p in parts if p.startswith("month=")).split("=")[1])
        except (IndexError, StopIteration, ValueError):
            continue
        out.add((line, year, month))
    return out


def mois_a_archiver(
    client: BaseClient, reference: date, anciennete_mois: int = 18,
) -> list[tuple[str, int, int]]:
    """(ligne, mois) de raw dont le mois de DONNÉES est <= référence - anciennete_mois."""
    seuil = _mois_index(reference.year, reference.month) - anciennete_mois
    return sorted(
        (line, y, m) for (line, y, m) in _raw_months(client) if _mois_index(y, m) <= seuil
    )


def archive_month(client: BaseClient, line: str, year: int, month: int) -> Result:
    """Déplace l'objet raw de (ligne, mois) vers archive/ (miroir du chemin) puis
    purge staging et curated pour la même (ligne, mois). Idempotent.
    """
    prefix = partition_prefix(line, year, month)
    raw_keys = list_keys(client, RAW_BUCKET, prefix)
    for key in raw_keys:
        client.copy_object(Bucket=ARCHIVE_BUCKET, Key=key,
                           CopySource={"Bucket": RAW_BUCKET, "Key": key})
    delete_keys(client, RAW_BUCKET, raw_keys)
    delete_prefix(client, STAGING_BUCKET, prefix)
    delete_prefix(client, CURATED_BUCKET, curated_partition_prefix(line, year, month))
    return Result(
        f"{line} {year}-{month:02d}",
        f"{len(raw_keys)} objet(s) archivé(s) + dérivés purgés",
        True,
    )


def main(anciennete_mois: int = 18) -> int:
    """Archive toutes les (ligne, mois) éligibles. Lançable en CLI."""
    client = get_s3_client()
    mois = mois_a_archiver(client, date.today(), anciennete_mois)
    return run(
        lambda t: archive_month(client, *t),
        mois,
        "Archivage raw → archive/ (+ purge staging/curated)",
    )


if __name__ == "__main__":
    raise SystemExit(main())
