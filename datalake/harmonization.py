"""Harmonisation raw → staging (C19) : une journée à la fois (fil-de-l'eau).

Coquille appelée par le DAG `harmonisation_staging` ET par le CLI
`python -m datalake.harmonization`. Conforme au contrat docs/architecture.md §12 :
casse en minuscules, timestamp ISO (UTC supposé), elapsed_time nullable, schéma
cible §6, partition au jour, dédup (line, timestamp), idempotence par partition.
"""
from __future__ import annotations

import io
from datetime import date

import polars as pl
from botocore.client import BaseClient

from datalake.explore import TS_FORMAT
from datalake.runner import Result, drainer, filigrane, pas
from datalake.storage import delete_prefix, get_s3_client, list_keys

RAW_BUCKET = "raw"
STAGING_BUCKET = "staging"

# Schéma cible figé (architecture §6), ordre des colonnes inclus.
TARGET_COLUMNS = [
    "timestamp", "temperature", "pressure", "elapsed_time", "label",
    "line", "year", "month", "day",
]


def staging_day_prefix(line: str, year: int, month: int, day: int) -> str:
    """Préfixe de partition staging (jour)."""
    return f"production_lines/{line}/year={year}/month={month:02d}/day={day:02d}/"


def _harmonize_frame(df: pl.DataFrame, line: str) -> pl.DataFrame:
    """Applique les règles d'harmonisation §12 et renvoie le schéma cible §6."""
    df = df.rename({c: c.lower() for c in df.columns})
    df = df.with_columns(pl.col("timestamp").str.to_datetime(TS_FORMAT))
    if "elapsed_time" not in df.columns:
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("elapsed_time"))
    df = df.with_columns(
        pl.col("temperature").cast(pl.Float64),
        pl.col("pressure").cast(pl.Float64),
        pl.col("elapsed_time").cast(pl.Float64),
        pl.col("label").cast(pl.Int64),
        pl.lit(line).alias("line"),
        pl.col("timestamp").dt.year().alias("year"),
        pl.col("timestamp").dt.month().alias("month"),
        pl.col("timestamp").dt.day().alias("day"),
    )
    df = df.unique(subset=["line", "timestamp"], keep="first").sort("timestamp")
    return df.select(TARGET_COLUMNS)


def _lines_in_raw(client: BaseClient) -> list[str]:
    """Lignes présentes dans raw (déduites des chemins production_lines/<line>/...)."""
    lignes = {k.split("/")[1] for k in list_keys(client, RAW_BUCKET, "production_lines/")}
    return sorted(lignes)


def _raw_csv_key(client: BaseClient, line: str) -> str | None:
    """Clé du CSV brut d'une ligne (un seul fichier par partition mois)."""
    keys = list_keys(client, RAW_BUCKET, f"production_lines/{line}/")
    csvs = [k for k in keys if k.endswith(".csv")]
    return csvs[0] if csvs else None


def _read_raw_csv(client: BaseClient, key: str) -> pl.DataFrame:
    """Lit un objet CSV brut depuis S3 en DataFrame Polars."""
    body = client.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
    return pl.read_csv(io.BytesIO(body))


def raw_days(client: BaseClient) -> dict[str, set[date]]:
    """Pour chaque ligne, l'ensemble des jours présents dans raw (lit les CSV)."""
    out: dict[str, set[date]] = {}
    for line in _lines_in_raw(client):
        key = _raw_csv_key(client, line)
        if key is None:
            continue
        df = _read_raw_csv(client, key)
        tcol = next(c for c in df.columns if c.lower() == "timestamp")
        jours = df.get_column(tcol).str.to_datetime(TS_FORMAT).dt.date().unique().to_list()
        out[line] = set(jours)
    return out


def days_from_paths(
    client: BaseClient, bucket: str, root: str, line_index: int,
) -> dict[str, set[date]]:
    """Jours présents par ligne, déduits des chemins `…/year=/month=/day=/`."""
    out: dict[str, set[date]] = {}
    for k in list_keys(client, bucket, root):
        parts = k.split("/")
        try:
            line = parts[line_index].split("=")[-1]  # `lineX` ou `line=lineX`
            y = int(next(p for p in parts if p.startswith("year=")).split("=")[1])
            m = int(next(p for p in parts if p.startswith("month=")).split("=")[1])
            d = int(next(p for p in parts if p.startswith("day=")).split("=")[1])
        except (IndexError, StopIteration, ValueError):
            continue
        out.setdefault(line, set()).add(date(y, m, d))
    return out


def staging_days(client: BaseClient) -> dict[str, set[date]]:
    """Pour chaque ligne, l'ensemble des jours présents dans staging (depuis les chemins)."""
    return days_from_paths(client, STAGING_BUCKET, "production_lines/", line_index=1)


def jour_a_traiter(client: BaseClient) -> date | None:
    """Plus ancien (ligne, jour) présent dans raw mais absent de staging (filigrane)."""
    return filigrane(raw_days(client), staging_days(client))


def harmonize_day(client: BaseClient, jour: date) -> list[Result]:
    """Harmonise `jour` pour chaque ligne l'ayant en raw mais pas en staging."""
    raw, stg = raw_days(client), staging_days(client)
    resultats: list[Result] = []
    for line in _lines_in_raw(client):
        if jour not in raw.get(line, set()) or jour in stg.get(line, set()):
            continue
        df = _read_raw_csv(client, _raw_csv_key(client, line))
        jour_df = _harmonize_frame(df, line).filter(pl.col("timestamp").dt.date() == jour)
        prefix = staging_day_prefix(line, jour.year, jour.month, jour.day)
        delete_prefix(client, STAGING_BUCKET, prefix)          # idempotence
        buf = io.BytesIO()
        jour_df.write_parquet(buf)
        client.put_object(Bucket=STAGING_BUCKET, Key=prefix + "part.parquet", Body=buf.getvalue())
        resultats.append(Result(f"{line} {jour}", f"{jour_df.height} ligne(s) → staging", True))
    return resultats


def harmonize_step(client: BaseClient | None = None) -> Result:
    """Traite la plus ancienne journée en attente (un pas). 'à jour' s'il n'y a rien."""
    client = client or get_s3_client()
    return pas(client, jour_a_traiter, harmonize_day, STAGING_BUCKET)


def main() -> int:
    """Draine raw → staging (toutes les journées en attente). Lançable en CLI."""
    return drainer(get_s3_client(), harmonize_step, "Harmonisation raw → staging")


if __name__ == "__main__":
    raise SystemExit(main())
