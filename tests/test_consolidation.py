from __future__ import annotations

import io
from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from datalake import consolidation
from datalake.storage import delete_prefix

if TYPE_CHECKING:
    from conftest import FakeS3


def _seed_staging_day(fake_s3: FakeS3, line: str, y: int, m: int, d: int) -> None:
    df = pl.DataFrame({
        "timestamp": [f"{y:04d}-{m:02d}-{d:02d}T00:00:00"],
        "temperature": [180.0], "pressure": [1.2], "elapsed_time": [None],
        "label": [0], "line": [line], "year": [y], "month": [m], "day": [d],
    })
    buf = io.BytesIO()
    df.write_parquet(buf)
    fake_s3.put_object(
        Bucket="staging",
        Key=f"production_lines/{line}/year={y}/month={m:02d}/day={d:02d}/part.parquet",
        Body=io.BytesIO(buf.getvalue()),
    )


def test_curated_day_prefix_format() -> None:
    assert consolidation.curated_day_prefix("lineA", 2025, 5, 1) == \
        "production_lines/line=lineA/year=2025/month=05/day=01/"


def test_jour_a_traiter_filigrane_curated(fake_s3: FakeS3) -> None:
    _seed_staging_day(fake_s3, "lineE", 2025, 1, 1)
    assert consolidation.jour_a_traiter(fake_s3) == date(2025, 1, 1)


def test_consolidate_day_ecrit_curated(fake_s3: FakeS3) -> None:
    _seed_staging_day(fake_s3, "lineE", 2025, 1, 1)
    res = consolidation.consolidate_day(fake_s3, date(2025, 1, 1))
    assert res and all(r.ok for r in res)
    prefix = consolidation.curated_day_prefix("lineE", 2025, 1, 1)
    keys = [k for k in fake_s3.store if k[0] == "curated" and k[1].startswith(prefix)]
    assert len(keys) == 1
    df = pl.read_parquet(io.BytesIO(fake_s3.store[keys[0]]))
    assert df["line"].unique().to_list() == ["lineE"]


def test_consolidate_step_puis_a_jour(fake_s3: FakeS3) -> None:
    _seed_staging_day(fake_s3, "lineE", 2025, 1, 1)
    r1 = consolidation.consolidate_step(fake_s3)
    assert r1.ok and r1.statut != "à jour"
    r2 = consolidation.consolidate_step(fake_s3)
    assert r2.ok and r2.statut == "à jour"


def test_consolidate_auto_reparation_curated(fake_s3: FakeS3) -> None:
    # curated rempli puis sa partition supprimée (simule la cascade d'invalidation)
    _seed_staging_day(fake_s3, "lineE", 2025, 1, 1)
    consolidation.consolidate_step(fake_s3)
    prefix = consolidation.curated_day_prefix("lineE", 2025, 1, 1)
    delete_prefix(fake_s3, "curated", prefix)
    # le filigrane redétecte le jour manquant -> recalcul automatique
    assert consolidation.jour_a_traiter(fake_s3) == date(2025, 1, 1)
    res = consolidation.consolidate_step(fake_s3)
    assert res.ok and res.statut != "à jour"
    keys = [k for k in fake_s3.store if k[0] == "curated" and k[1].startswith(prefix)]
    assert len(keys) == 1  # partition recréée


def test_consolidate_day_multi_lignes_meme_jour(fake_s3: FakeS3) -> None:
    # cas générique : deux lignes ayant des données le même jour calendaire
    _seed_staging_day(fake_s3, "lineA", 2025, 1, 1)
    _seed_staging_day(fake_s3, "lineB", 2025, 1, 1)
    res = consolidation.consolidate_day(fake_s3, date(2025, 1, 1))
    assert len(res) == 2 and all(r.ok for r in res)
    for line in ("lineA", "lineB"):
        prefix = consolidation.curated_day_prefix(line, 2025, 1, 1)
        assert any(k[0] == "curated" and k[1].startswith(prefix) for k in fake_s3.store)
