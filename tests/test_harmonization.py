from __future__ import annotations

import io
from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from datalake import harmonization
from datalake.harmonization import _harmonize_frame, staging_day_prefix

if TYPE_CHECKING:
    from conftest import FakeS3


def test_staging_day_prefix_format() -> None:
    assert staging_day_prefix("lineA", 2025, 5, 1) == \
        "production_lines/lineA/year=2025/month=05/day=01/"


def test_harmonize_frame_normalise_casse_et_schema() -> None:
    brut = pl.DataFrame({
        "Timestamp": ["2025-05-01 00:00:00", "2025-05-01 00:01:00"],
        "Temperature": [180.0, 180.5],
        "Pressure": [1.2, 1.3],
        "label": [0, 1],
    })  # pas d'elapsed_time (cas LineC/D/E)
    out = _harmonize_frame(brut, "lineC")
    assert out.columns == [
        "timestamp", "temperature", "pressure", "elapsed_time", "label",
        "line", "year", "month", "day",
    ]
    assert out["elapsed_time"].null_count() == 2          # absent -> NULL
    assert out["line"].unique().to_list() == ["lineC"]
    assert out["timestamp"].dtype == pl.Datetime
    assert out["day"].to_list() == [1, 1]


def test_harmonize_frame_dedoublonne_line_timestamp() -> None:
    brut = pl.DataFrame({
        "timestamp": ["2025-05-01 00:00:00", "2025-05-01 00:00:00"],  # doublon
        "temperature": [180.0, 181.0],
        "pressure": [1.2, 1.2],
        "label": [0, 0],
    })
    out = _harmonize_frame(brut, "lineA")
    assert out.height == 1


_CSV_LINEA = (
    b"timestamp,temperature,pressure,elapsed_time,label\n"
    b"2025-05-01 00:00:00,180.0,1.2,5.0,0\n"
    b"2025-05-02 00:00:00,181.0,1.3,6.0,1\n"
)


def _seed_raw_linea(fake_s3: FakeS3) -> None:
    fake_s3.put_object(
        Bucket="raw",
        Key="production_lines/lineA/year=2025/month=05/LineA.csv",
        Body=io.BytesIO(_CSV_LINEA),
    )


def test_jour_a_traiter_prend_le_plus_ancien(fake_s3: FakeS3) -> None:
    _seed_raw_linea(fake_s3)
    assert harmonization.jour_a_traiter(fake_s3) == date(2025, 5, 1)


def test_jour_a_traiter_none_si_a_jour(fake_s3: FakeS3) -> None:
    _seed_raw_linea(fake_s3)
    for d in (1, 2):
        fake_s3.put_object(
            Bucket="staging",
            Key=f"production_lines/lineA/year=2025/month=05/day=0{d}/part.parquet",
            Body=io.BytesIO(b"x"),
        )
    assert harmonization.jour_a_traiter(fake_s3) is None


def _staging_parquet(fake_s3: FakeS3, line: str, y: int, m: int, d: int) -> pl.DataFrame | None:
    prefix = harmonization.staging_day_prefix(line, y, m, d)
    keys = [k for k in fake_s3.store if k[0] == "staging" and k[1].startswith(prefix)]
    if not keys:
        return None
    return pl.read_parquet(io.BytesIO(fake_s3.store[keys[0]]))


def test_harmonize_day_ecrit_la_partition_du_jour(fake_s3: FakeS3) -> None:
    _seed_raw_linea(fake_s3)
    res = harmonization.harmonize_day(fake_s3, date(2025, 5, 1))
    assert res and all(r.ok for r in res)
    df = _staging_parquet(fake_s3, "lineA", 2025, 5, 1)
    assert df is not None
    assert df.height == 1                       # seule la journée du 1er
    assert df["day"].unique().to_list() == [1]
    assert df.columns == harmonization.TARGET_COLUMNS


def test_harmonize_step_puis_a_jour(fake_s3: FakeS3) -> None:
    _seed_raw_linea(fake_s3)
    r1 = harmonization.harmonize_step(fake_s3)   # traite le 1er mai
    assert r1.ok and r1.statut != "à jour"
    r2 = harmonization.harmonize_step(fake_s3)   # traite le 2 mai
    assert r2.ok and r2.statut != "à jour"
    r3 = harmonization.harmonize_step(fake_s3)   # plus rien
    assert r3.ok and r3.statut == "à jour"


def test_harmonize_day_idempotent(fake_s3: FakeS3) -> None:
    _seed_raw_linea(fake_s3)
    harmonization.harmonize_day(fake_s3, date(2025, 5, 1))
    harmonization.harmonize_day(fake_s3, date(2025, 5, 1))  # rejoué
    keys = [k for k in fake_s3.store if k[0] == "staging" and "day=01" in k[1]]
    assert len(keys) == 1                        # une seule partition, pas de doublon
