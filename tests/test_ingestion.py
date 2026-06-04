import io

import pytest

from datalake import ingestion
from datalake.ingestion import partition_key, partition_prefix
from datalake.storage import list_keys


def _write_csv(path, rows):
    path.write_text("timestamp,temperature,label\n" + "\n".join(rows) + "\n", encoding="utf-8")


def test_partition_prefix_format():
    assert partition_prefix("lineA", 2025, 5) == "production_lines/lineA/year=2025/month=05/"


def test_partition_key_single_month(tmp_path):
    f = tmp_path / "LineA_Stable_10K.csv"
    _write_csv(f, ["2025-05-01 00:00:00,180.0,0", "2025-05-01 00:01:00,180.1,0"])
    prefix, key = partition_key(f)
    assert prefix == "production_lines/lineA/year=2025/month=05/"
    assert key == "production_lines/lineA/year=2025/month=05/LineA_Stable_10K.csv"


def test_partition_key_multi_month_raises(tmp_path):
    f = tmp_path / "LineA_Stable_10K.csv"
    _write_csv(f, ["2025-05-31 23:59:00,180.0,0", "2025-06-01 00:00:00,180.1,0"])
    with pytest.raises(ValueError):
        partition_key(f)


def _line_csv(path):
    path.write_text(
        "timestamp,temperature,label\n2025-05-01 00:00:00,180.0,0\n2025-05-01 00:01:00,180.1,1\n",
        encoding="utf-8",
    )
    return path


def test_ingest_first_import(tmp_path, fake_s3):
    f = _line_csv(tmp_path / "LineA_Stable_10K.csv")
    res = ingestion.ingest_file(f, client=fake_s3)
    assert res.ok and res.statut == "ré-importé"
    key = "production_lines/lineA/year=2025/month=05/LineA_Stable_10K.csv"
    assert ("raw", key) in fake_s3.store
    assert fake_s3.store[("raw", key)] == f.read_bytes()  # byte-identique


def test_ingest_skip_when_md5_identical(tmp_path, fake_s3):
    f = _line_csv(tmp_path / "LineA_Stable_10K.csv")
    ingestion.ingest_file(f, client=fake_s3)              # 1er import
    # un objet staging dérivé existe :
    fake_s3.put_object(Bucket="staging",
                       Key="production_lines/lineA/year=2025/month=05/day=01/part.parquet",
                       Body=io.BytesIO(b"derive"))
    res = ingestion.ingest_file(f, client=fake_s3)        # 2e passage, contenu inchangé
    assert res.ok and res.statut == "inchangé (MD5)"
    # staging NON touché (skip) :
    assert list_keys(fake_s3, "staging", "production_lines/lineA/year=2025/month=05/")


def test_reimport_invalidates_staging(tmp_path, fake_s3):
    f = _line_csv(tmp_path / "LineA_Stable_10K.csv")
    key = "production_lines/lineA/year=2025/month=05/LineA_Stable_10K.csv"
    # raw contient une ANCIENNE version (MD5 différent) + un dérivé en staging :
    fake_s3.put_object(Bucket="raw", Key=key, Body=io.BytesIO(b"ancien contenu"))
    fake_s3.put_object(Bucket="staging",
                       Key="production_lines/lineA/year=2025/month=05/day=01/part.parquet",
                       Body=io.BytesIO(b"derive"))
    res = ingestion.ingest_file(f, client=fake_s3)
    assert res.ok and res.statut == "ré-importé"
    assert fake_s3.store[("raw", key)] == f.read_bytes()                       # raw à jour
    assert list_keys(fake_s3, "staging", "production_lines/lineA/year=2025/month=05/") == []  # cascade


def test_reimport_cleans_renamed_object_in_raw(tmp_path, fake_s3):
    f = _line_csv(tmp_path / "LineA_Stable_10K.csv")
    # un objet d'un ancien nom traîne dans la partition raw :
    stale = "production_lines/lineA/year=2025/month=05/LineA_OLDNAME.csv"
    fake_s3.put_object(Bucket="raw", Key=stale, Body=io.BytesIO(b"vieux"))
    ingestion.ingest_file(f, client=fake_s3)
    keys = list_keys(fake_s3, "raw", "production_lines/lineA/year=2025/month=05/")
    assert keys == ["production_lines/lineA/year=2025/month=05/LineA_Stable_10K.csv"]


def test_md5_failure_does_not_touch_staging(tmp_path, fake_s3, monkeypatch):
    f = _line_csv(tmp_path / "LineA_Stable_10K.csv")
    fake_s3.put_object(Bucket="staging",
                       Key="production_lines/lineA/year=2025/month=05/day=01/part.parquet",
                       Body=io.BytesIO(b"derive"))
    # simuler une corruption : put_object renvoie un ETag erroné
    monkeypatch.setattr(fake_s3, "put_object", lambda **kw: {"ETag": '"deadbeef"'})
    res = ingestion.ingest_file(f, client=fake_s3)
    assert not res.ok and res.statut == "ÉCHEC MD5"
    # staging intact (ordre sûr : on n'invalide pas l'aval si raw n'est pas confirmé) :
    assert list_keys(fake_s3, "staging", "production_lines/lineA/year=2025/month=05/")
