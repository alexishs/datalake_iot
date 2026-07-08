from __future__ import annotations

import io
from datetime import date
from typing import TYPE_CHECKING

from datalake import archive

if TYPE_CHECKING:
    from conftest import FakeS3


def _seed_raw(fake_s3: FakeS3, line: str, y: int, m: int) -> None:
    fake_s3.put_object(
        Bucket="raw",
        Key=f"production_lines/{line}/year={y}/month={m:02d}/{line}.csv",
        Body=io.BytesIO(b"timestamp,temperature,label\n"),
    )


def test_mois_a_archiver_seuil_18_mois(fake_s3: FakeS3) -> None:
    _seed_raw(fake_s3, "lineE", 2025, 1)   # janvier 2025
    _seed_raw(fake_s3, "lineD", 2025, 2)   # février 2025
    # référence mi-2026 : seuil = 2026-07 moins 18 mois = 2025-01
    resultat = archive.mois_a_archiver(fake_s3, date(2026, 7, 8), anciennete_mois=18)
    assert resultat == [("lineE", 2025, 1)]   # seul janvier 2025


def _seed_derivees(fake_s3: FakeS3, line: str, y: int, m: int, d: int) -> None:
    base = f"production_lines/{line}/year={y}/month={m:02d}/day={d:02d}/part.parquet"
    fake_s3.put_object(Bucket="staging", Key=base, Body=io.BytesIO(b"stg"))
    fake_s3.put_object(
        Bucket="curated",
        Key=f"production_lines/line={line}/year={y}/month={m:02d}/day={d:02d}/part.parquet",
        Body=io.BytesIO(b"cur"),
    )


def test_archive_month_deplace_raw_et_purge_derivees(fake_s3: FakeS3) -> None:
    _seed_raw(fake_s3, "lineE", 2025, 1)
    _seed_derivees(fake_s3, "lineE", 2025, 1, 1)
    res = archive.archive_month(fake_s3, "lineE", 2025, 1)
    assert res.ok
    raw_key = "production_lines/lineE/year=2025/month=01/lineE.csv"
    assert ("archive", raw_key) in fake_s3.store        # déplacé (miroir) vers archive
    assert ("raw", raw_key) not in fake_s3.store          # supprimé de raw
    from datalake.storage import list_keys
    assert list_keys(fake_s3, "staging", "production_lines/lineE/year=2025/month=01/") == []
    assert list_keys(fake_s3, "curated", "production_lines/line=lineE/year=2025/month=01/") == []


def test_archive_month_idempotent(fake_s3: FakeS3) -> None:
    _seed_raw(fake_s3, "lineE", 2025, 1)
    archive.archive_month(fake_s3, "lineE", 2025, 1)
    res = archive.archive_month(fake_s3, "lineE", 2025, 1)  # rejoué : raw déjà vide
    assert res.ok  # ne lève pas


def test_reintegration_remet_dans_raw(fake_s3: FakeS3) -> None:
    _seed_raw(fake_s3, "lineE", 2025, 1)
    archive.archive_month(fake_s3, "lineE", 2025, 1)
    key = "production_lines/lineE/year=2025/month=01/lineE.csv"
    fake_s3.copy_object(Bucket="raw", Key=key, CopySource={"Bucket": "archive", "Key": key})
    from datalake.storage import list_keys
    assert list_keys(fake_s3, "raw", "production_lines/lineE/year=2025/month=01/") == [key]
