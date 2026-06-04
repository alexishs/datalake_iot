# Factorisation du package `datalake` (ingestion vers `raw/`) — Plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Doter le package `datalake/` d'un `runner` partagé, d'un module d'`ingestion` (data → `raw/`) idempotent avec cascade vers `staging/`, d'une primitive S3 `delete_prefix`, et réaligner `download.py` — de sorte que le lancement manuel et les DAGs appellent la même fonction métier.

**Architecture :** Chaque étape expose une fonction « par item » (`ingest_file`, `download_one`) renvoyant un `Result` ; un `runner.run` générique boucle, capture les erreurs, affiche un rapport et renvoie un code de sortie. `ingest_file` dépose le CSV byte-identique dans `raw/` (partition au mois, garde-fou « un seul mois »), décide via MD5, et — sur (ré)import — écrit+vérifie `raw` **puis** invalide la `(ligne, mois)` en `staging` (cascade). Spec : [docs/superpowers/specs/2026-06-04-factorisation-package-datalake-design.md](../specs/2026-06-04-factorisation-package-datalake-design.md).

**Tech Stack :** Python 3.12, Polars, boto3/botocore (MinIO S3), pytest. Tests **en pur Python** via un faux client S3 en mémoire (aucun MinIO requis pour les tests unitaires).

**Conventions de commit :** messages en français ; terminer par `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

---

## Structure des fichiers

- Créer `datalake/runner.py` — `Result` + `run(action, items, titre)`.
- Créer `datalake/ingestion.py` — `partition_prefix`, `partition_key`, `_remote_etag`, `ingest_file`, `main`.
- Modifier `datalake/storage.py` — ajouter `list_keys`, `delete_keys`, `delete_prefix`.
- Modifier `datalake/download.py` — `download_one` renvoie un `Result`, `main` utilise `run`.
- Créer `tests/conftest.py` — faux client S3 `FakeS3` + fixture `fake_s3`.
- Créer `tests/test_runner.py`, `tests/test_storage.py`, `tests/test_ingestion.py`, `tests/test_download.py`.
- Créer `pytest.ini` — config pytest.
- Modifier `requirements.txt` — ajouter `pytest`.

---

## Task 1 : Infrastructure de test (pytest + faux client S3)

**Files:**
- Modify: `requirements.txt`
- Create: `pytest.ini`
- Create: `tests/conftest.py`
- Test: `tests/test_fakes.py`

- [ ] **Step 1 : Ajouter pytest aux dépendances**

Dans `requirements.txt`, sous la section « Qualité de code (dev) », ajouter après la ligne `ruff~=0.8` :

```
pytest~=8.3            # tests unitaires (faux client S3, pur Python)
```

- [ ] **Step 2 : Installer pytest**

Run: `.venv/bin/pip install -q pytest`
Expected: installation sans erreur.

- [ ] **Step 3 : Créer `pytest.ini`**

```ini
[pytest]
pythonpath = .
testpaths = tests
```

- [ ] **Step 4 : Créer le faux client S3 et la fixture (`tests/conftest.py`)**

```python
"""Outils de test partagés : faux client S3 en mémoire (aucun MinIO requis)."""
import hashlib

import pytest
from botocore.exceptions import ClientError


class FakeS3:
    """Client S3 minimal en mémoire : put / head / list / delete.

    Imite le strict nécessaire de l'API boto3 utilisée par le code :
    - put_object renvoie un ETag = MD5 du contenu (comme un upload simple MinIO) ;
    - head_object lève botocore ClientError si la clé est absente ;
    - list_objects_v2 filtre par préfixe (sans pagination) ;
    - delete_objects supprime les clés données.
    """

    def __init__(self):
        self.store: dict[tuple[str, str], bytes] = {}

    def put_object(self, Bucket, Key, Body):
        data = Body.read() if hasattr(Body, "read") else Body
        self.store[(Bucket, Key)] = data
        return {"ETag": '"' + hashlib.md5(data).hexdigest() + '"'}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.store:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"ETag": '"' + hashlib.md5(self.store[(Bucket, Key)]).hexdigest() + '"'}

    def list_objects_v2(self, Bucket, Prefix="", **kwargs):
        keys = sorted(k for (b, k) in self.store if b == Bucket and k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def delete_objects(self, Bucket, Delete):
        for obj in Delete["Objects"]:
            self.store.pop((Bucket, obj["Key"]), None)
        return {"Deleted": [{"Key": o["Key"]} for o in Delete["Objects"]]}


@pytest.fixture
def fake_s3():
    return FakeS3()
```

- [ ] **Step 5 : Écrire le test du faux client (`tests/test_fakes.py`)**

```python
import io

import pytest
from botocore.exceptions import ClientError


def test_fake_put_head_list_delete(fake_s3):
    fake_s3.put_object(Bucket="raw", Key="a/x.csv", Body=io.BytesIO(b"hello"))
    # MD5("hello") = 5d41402abc4b2a76b9719d911017c592
    assert fake_s3.head_object(Bucket="raw", Key="a/x.csv")["ETag"].strip('"') == \
        "5d41402abc4b2a76b9719d911017c592"
    keys = [o["Key"] for o in fake_s3.list_objects_v2(Bucket="raw", Prefix="a/")["Contents"]]
    assert keys == ["a/x.csv"]
    fake_s3.delete_objects(Bucket="raw", Delete={"Objects": [{"Key": "a/x.csv"}]})
    with pytest.raises(ClientError):
        fake_s3.head_object(Bucket="raw", Key="a/x.csv")
```

- [ ] **Step 6 : Lancer le test (doit passer)**

Run: `.venv/bin/pytest tests/test_fakes.py -v`
Expected: PASS (1 test).

- [ ] **Step 7 : Commit**

```bash
git add requirements.txt pytest.ini tests/conftest.py tests/test_fakes.py
git commit -m "test : infra pytest + faux client S3 en mémoire

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2 : `runner.py` — `Result` + `run`

**Files:**
- Create: `datalake/runner.py`
- Test: `tests/test_runner.py`

- [ ] **Step 1 : Écrire les tests (`tests/test_runner.py`)**

```python
from datalake.runner import Result, run


def test_run_all_ok_returns_0(capsys):
    items = ["a", "b"]
    code = run(lambda x: Result(x, "ok", True), items, "Titre")
    assert code == 0
    out = capsys.readouterr().out
    assert "Titre" in out and "2 OK, 0 échec(s)." in out


def test_run_with_failure_returns_1():
    code = run(lambda x: Result(x, "ko", x == "a"), ["a", "b"], "Titre")
    assert code == 1  # 'b' échoue


def test_run_captures_exceptions_and_continues():
    seen = []

    def action(x):
        seen.append(x)
        if x == "boom":
            raise ValueError("explosion")
        return Result(x, "ok", True)

    code = run(action, ["x", "boom", "y"], "Titre")
    assert code == 1
    assert seen == ["x", "boom", "y"]  # tous les items traités malgré l'exception
```

- [ ] **Step 2 : Lancer les tests (doivent échouer)**

Run: `.venv/bin/pytest tests/test_runner.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'datalake.runner'`).

- [ ] **Step 3 : Implémenter `datalake/runner.py`**

```python
"""Exécuteur générique : applique une action à des items, rapporte, renvoie un code de sortie.

Mutualise la mécanique (boucle, capture d'erreurs, rapport) entre les CLI des étapes
du pipeline. Chaque action renvoie un `Result` ; le `runner` n'a aucune logique métier.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any


@dataclass
class Result:
    label: str   # ce qui est traité (p. ex. nom de fichier)
    statut: str  # texte court : "déposé", "inchangé (MD5)", "ré-importé", "ÉCHEC MD5"…
    ok: bool     # succès


def run(action: Callable[[Any], Result], items: Iterable[Any], titre: str) -> int:
    """Applique `action` à chaque item. Renvoie 0 si tout OK, 1 si au moins un échec."""
    print(titre)
    ok_count = ko_count = 0
    for item in items:
        try:
            res = action(item)
        except Exception as exc:  # garde-fou, réseau, S3… : un échec n'arrête pas les autres
            res = Result(str(item), f"ERREUR : {exc}", False)
        print(f"  {'✓' if res.ok else '✗'} {res.label} — {res.statut}")
        if res.ok:
            ok_count += 1
        else:
            ko_count += 1
    print(f"→ {ok_count} OK, {ko_count} échec(s).")
    return 1 if ko_count else 0
```

- [ ] **Step 4 : Lancer les tests (doivent passer)**

Run: `.venv/bin/pytest tests/test_runner.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5 : Commit**

```bash
git add datalake/runner.py tests/test_runner.py
git commit -m "feat(datalake) : runner partagé (Result + run)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3 : `storage.py` — `list_keys`, `delete_keys`, `delete_prefix`

**Files:**
- Modify: `datalake/storage.py`
- Test: `tests/test_storage.py`

- [ ] **Step 1 : Écrire les tests (`tests/test_storage.py`)**

```python
import io

from datalake.storage import delete_keys, delete_prefix, list_keys


def _put(client, bucket, key, data=b"x"):
    client.put_object(Bucket=bucket, Key=key, Body=io.BytesIO(data))


def test_list_keys_filters_by_prefix(fake_s3):
    _put(fake_s3, "raw", "p/a")
    _put(fake_s3, "raw", "p/b")
    _put(fake_s3, "raw", "autre/c")
    assert list_keys(fake_s3, "raw", "p/") == ["p/a", "p/b"]


def test_delete_keys_returns_count_and_removes(fake_s3):
    _put(fake_s3, "raw", "p/a")
    _put(fake_s3, "raw", "p/b")
    assert delete_keys(fake_s3, "raw", ["p/a", "p/b"]) == 2
    assert list_keys(fake_s3, "raw", "p/") == []


def test_delete_prefix_removes_all_under_prefix(fake_s3):
    _put(fake_s3, "raw", "p/a")
    _put(fake_s3, "raw", "p/sub/b")
    _put(fake_s3, "raw", "garde/c")
    assert delete_prefix(fake_s3, "raw", "p/") == 2
    assert list_keys(fake_s3, "raw", "") == ["garde/c"]
```

- [ ] **Step 2 : Lancer les tests (doivent échouer)**

Run: `.venv/bin/pytest tests/test_storage.py -v`
Expected: FAIL (`ImportError: cannot import name 'list_keys'`).

- [ ] **Step 3 : Ajouter les fonctions à `datalake/storage.py`**

Ajouter à la fin de `datalake/storage.py` :

```python
def list_keys(client, bucket: str, prefix: str) -> list[str]:
    """Toutes les clés d'objets sous `prefix` (pagination gérée)."""
    keys, token = [], None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        keys.extend(obj["Key"] for obj in resp.get("Contents", []))
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            return keys


def delete_keys(client, bucket: str, keys: list[str]) -> int:
    """Supprime les clés données (par lots de 1000). Retourne le nombre supprimé."""
    total = 0
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        if batch:
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})
            total += len(batch)
    return total


def delete_prefix(client, bucket: str, prefix: str) -> int:
    """Supprime tous les objets sous `prefix`. Retourne le nombre supprimé."""
    return delete_keys(client, bucket, list_keys(client, bucket, prefix))
```

- [ ] **Step 4 : Lancer les tests (doivent passer)**

Run: `.venv/bin/pytest tests/test_storage.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5 : Commit**

```bash
git add datalake/storage.py tests/test_storage.py
git commit -m "feat(datalake) : storage list_keys/delete_keys/delete_prefix

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4 : `ingestion.partition_key` + garde-fou « un seul mois »

**Files:**
- Create: `datalake/ingestion.py`
- Test: `tests/test_ingestion.py`

- [ ] **Step 1 : Écrire les tests (`tests/test_ingestion.py`)**

```python
import pytest

from datalake.ingestion import partition_key, partition_prefix


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
```

- [ ] **Step 2 : Lancer les tests (doivent échouer)**

Run: `.venv/bin/pytest tests/test_ingestion.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'datalake.ingestion'`).

- [ ] **Step 3 : Créer `datalake/ingestion.py` (partie partition)**

```python
"""Ingestion brute : dépôt des CSV source dans `raw/` + intégrité MD5 (C19).

Conforme au contrat docs/architecture.md §12 : clé `production_lines/{line}/year=YYYY/month=MM/`,
`line` du nom de fichier, `year`/`month` des données (garde-fou « un seul mois »), dépôt
byte-identique, MD5 via ETag, idempotence + cascade d'invalidation vers `staging`.
"""
from __future__ import annotations

from pathlib import Path

import polars as pl
from botocore.exceptions import ClientError

from datalake.explore import TS_FORMAT, csv_paths, line_id
from datalake.runner import Result, run
from datalake.storage import delete_keys, delete_prefix, get_s3_client, list_keys, md5_file

RAW_BUCKET = "raw"
STAGING_BUCKET = "staging"


def partition_prefix(line: str, year: int, month: int) -> str:
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
```

- [ ] **Step 4 : Lancer les tests (doivent passer)**

Run: `.venv/bin/pytest tests/test_ingestion.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5 : Commit**

```bash
git add datalake/ingestion.py tests/test_ingestion.py
git commit -m "feat(datalake) : ingestion.partition_key + garde-fou un seul mois

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5 : `ingestion.ingest_file` — idempotence MD5 + cascade (ordre sûr)

**Files:**
- Modify: `datalake/ingestion.py`
- Test: `tests/test_ingestion.py`

- [ ] **Step 1 : Écrire les tests (ajouter à `tests/test_ingestion.py`)**

```python
import io

from datalake import ingestion
from datalake.storage import list_keys, md5_file


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
```

- [ ] **Step 2 : Lancer les tests (doivent échouer)**

Run: `.venv/bin/pytest tests/test_ingestion.py -v`
Expected: FAIL (`AttributeError: module 'datalake.ingestion' has no attribute 'ingest_file'`).

- [ ] **Step 3 : Ajouter `_remote_etag` et `ingest_file` à `datalake/ingestion.py`**

Ajouter après `partition_key` :

```python
def _remote_etag(client, bucket: str, key: str) -> str | None:
    """ETag (= MD5 pour un upload simple) de l'objet, ou None s'il est absent."""
    try:
        return client.head_object(Bucket=bucket, Key=key)["ETag"].strip('"')
    except ClientError:
        return None


def ingest_file(path: Path | str, client=None) -> Result:
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
```

- [ ] **Step 4 : Lancer les tests (doivent passer)**

Run: `.venv/bin/pytest tests/test_ingestion.py -v`
Expected: PASS (tous, dont les 5 nouveaux).

- [ ] **Step 5 : Commit**

```bash
git add datalake/ingestion.py tests/test_ingestion.py
git commit -m "feat(datalake) : ingest_file (idempotence MD5 + cascade staging, ordre sûr)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6 : `ingestion.main` + entrée CLI + vérification manuelle

**Files:**
- Modify: `datalake/ingestion.py`

- [ ] **Step 1 : Ajouter `main` et le garde `__main__` à `datalake/ingestion.py`**

Ajouter à la fin du fichier :

```python
def main() -> int:
    """Dépose tous les CSV de `data/` dans `raw/` (rapport + code de sortie)."""
    client = get_s3_client()
    return run(lambda p: ingest_file(p, client), csv_paths(), "Ingestion → raw/")


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2 : Vérifier que toute la suite de tests passe**

Run: `.venv/bin/pytest -v`
Expected: PASS (tous les fichiers de tests).

- [ ] **Step 3 : Vérification manuelle dans le conteneur dev (MinIO réel)**

Prérequis : la stack tourne (`docker compose up -d`) et les CSV sont dans `data/`.

Run: `docker compose exec dev python -m datalake.ingestion`
Expected (sortie) : `Ingestion → raw/` puis 5 lignes `✓ LineX_….csv — ré-importé`, et `→ 5 OK, 0 échec(s).`

- [ ] **Step 4 : Vérifier l'idempotence (2e exécution)**

Run: `docker compose exec dev python -m datalake.ingestion`
Expected : 5 lignes `✓ … — inchangé (MD5)`, `→ 5 OK, 0 échec(s).` (aucun ré-import).

- [ ] **Step 5 : Vérifier le partitionnement et le MD5 dans MinIO**

Run: `docker compose exec dev mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" && docker compose exec dev mc ls --recursive local/raw/production_lines/`
Expected : un objet par ligne, sous `lineX/year=2025/month=MM/LineX_….csv` (mois distincts : 01→05).
*(Si `mc` n'est pas présent dans l'image dev, vérifier plutôt via la console MinIO http://localhost:9001.)*

- [ ] **Step 6 : Commit**

```bash
git add datalake/ingestion.py
git commit -m "feat(datalake) : ingestion.main + entrée CLI (python -m datalake.ingestion)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 7 : Réaligner `download.py` sur le `runner`

**Files:**
- Modify: `datalake/download.py`
- Test: `tests/test_download.py`

- [ ] **Step 1 : Écrire le test du chemin « déjà présent » (`tests/test_download.py`)**

Ce test ne touche pas le réseau (branche skip).

```python
import hashlib

from datalake import download
from datalake.runner import Result


def test_download_one_skip_when_present(tmp_path):
    content = b"timestamp,label\n2025-01-01 00:00:00,0\n"
    (tmp_path / "f.csv").write_bytes(content)
    meta = {
        "key": "f.csv",
        "md5": hashlib.md5(content).hexdigest(),
        "url": "http://invalid.invalid/should-not-be-called",
        "size": len(content),
    }
    res = download.download_one(meta, tmp_path)
    assert isinstance(res, Result)
    assert res.ok and "déjà présent" in res.statut
```

- [ ] **Step 2 : Lancer le test (doit échouer)**

Run: `.venv/bin/pytest tests/test_download.py -v`
Expected: FAIL (`download_one` renvoie une chaîne, pas un `Result` ; `isinstance(... Result)` faux).

- [ ] **Step 3 : Modifier `datalake/download.py`**

3a. Ajouter l'import du runner en tête (après les autres imports `from datalake…`) :

```python
from datalake.runner import Result, run
```

3b. Remplacer **toute la fonction `download_one`** par :

```python
def download_one(meta: dict, dest: Path) -> Result:
    """Télécharge un fichier dans `dest` (vérif MD5). Idempotent (skip si déjà présent)."""
    target = dest / meta["key"]
    if target.exists() and md5_file(target) == meta["md5"]:
        return Result(meta["key"], f"déjà présent ({human(meta['size'])})", True)

    tmp = target.with_suffix(target.suffix + ".part")
    req = urllib.request.Request(meta["url"], headers={"User-Agent": "datalake-iot/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp, open(tmp, "wb") as out:
        while chunk := resp.read(CHUNK):
            out.write(chunk)

    got = md5_file(tmp)
    if got != meta["md5"]:
        tmp.unlink(missing_ok=True)
        return Result(meta["key"], f"MD5 INVALIDE (attendu {meta['md5']}, obtenu {got})", False)
    tmp.replace(target)
    return Result(meta["key"], f"téléchargé ({human(meta['size'])})", True)
```

3c. Remplacer **toute la fonction `main`** par (s'appuie sur le `runner`, supprime la boucle/rapport maison) :

```python
def main() -> int:
    parser = argparse.ArgumentParser(description="Télécharge les sources depuis Zenodo.")
    parser.add_argument("--record-id", default=DEFAULT_RECORD_ID)
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--csv-only", action="store_true",
                        help="ne télécharger que les fichiers .csv (ignore les PDF)")
    args = parser.parse_args()

    args.dest.mkdir(parents=True, exist_ok=True)
    files = list_files(args.record_id)
    if args.csv_only:
        files = [f for f in files if f["key"].lower().endswith(".csv")]
    files = sorted(files, key=lambda f: f["key"])
    return run(lambda meta: download_one(meta, args.dest), files,
               f"Dépôt Zenodo {args.record_id} → {args.dest}/")
```

- [ ] **Step 4 : Lancer le test (doit passer)**

Run: `.venv/bin/pytest tests/test_download.py -v`
Expected: PASS.

- [ ] **Step 5 : Vérification manuelle (idempotence, hors réseau si déjà téléchargé)**

Run: `.venv/bin/python -m datalake.download`
Expected : `Dépôt Zenodo 15277168 → data/` puis 7 lignes `✓ … — déjà présent (…)`, `→ 7 OK, 0 échec(s).`

- [ ] **Step 6 : Commit**

```bash
git add datalake/download.py tests/test_download.py
git commit -m "refactor(datalake) : download.py utilise le runner (download_one -> Result)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 8 : Documentation & clôture

**Files:**
- Modify: `README.md`
- Modify: `rapport/rapport.md`

- [ ] **Step 1 : Mettre à jour l'index du package dans `README.md`**

Dans la section « Livrables & progression », mettre la ligne « Script d'upload vers `raw/` » à **✅** et pointer vers le module :

Remplacer :

```
| **C19 · J2** | Script d'upload vers `raw/` + vérification MD5 | `datalake/` *(à créer)* | ⏳ |
```

par :

```
| **C19 · J2** | Upload vers `raw/` + vérification MD5 | [datalake/ingestion.py](datalake/ingestion.py) | ✅ |
```

Et compléter le paragraphe « package métier » pour citer `runner.py` et `ingestion.py` (après `explore.py`) : ajouter `, [runner.py](datalake/runner.py) (boucle/rapport partagés), [ingestion.py](datalake/ingestion.py) (data → raw)`.

- [ ] **Step 2 : Noter l'avancée dans le rapport (section C19)**

Dans `rapport/rapport.md`, section « 3. C19 — Intégration », remplacer la ligne placeholder par un début de contenu (une seule ligne, sans hard-wrap) :

```
**Ingestion `data/` → `raw/`** : module réutilisable `datalake/ingestion.py` (appelé par le CLI `python -m datalake.ingestion` et, à terme, par le DAG d'ingestion) — dépôt byte-identique, partition au mois, **vérification MD5** (ETag), **idempotence** (skip si MD5 identique) et **cascade** d'invalidation de `staging`. Mécanique mutualisée via `datalake/runner.py`. *(Buckets, upload boto3, MD5 = exigences Jour 2 ✅ ; DAGs = Jours 3-4.)*
```

- [ ] **Step 3 : Vérifier l'absence de hard-wrap et de tutoiement dans les .md modifiés**

Run: `grep -nE "\b(tu|ton|ta|tes)\b" README.md rapport/rapport.md || echo "OK vouvoiement"`
Expected: `OK vouvoiement`.

- [ ] **Step 4 : Lancer toute la suite de tests**

Run: `.venv/bin/pytest -v`
Expected: PASS (tous).

- [ ] **Step 5 : Commit**

```bash
git add README.md rapport/rapport.md
git commit -m "docs : ingestion raw livrée (README livrables + rapport C19)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Auto-revue (à la fin de l'implémentation)

- **Couverture du spec :** `runner` (Task 2), `delete_prefix` + helpers (Task 3), `ingest_file` idempotence/cascade ordre sûr (Tasks 4-5), `main` CLI (Task 6), refactor `download` (Task 7), docs (Task 8). ✅
- **Manuel == DAG :** `ingest_file(path, client)` est l'unité unique ; le CLI la lie via `lambda`, le DAG (Jours 3-4) l'appellera de même. ✅
- **Pas de placeholder** dans le code des tâches ; chemins et commandes exacts.
- **Cohérence des noms :** `Result`, `run`, `partition_prefix`, `partition_key`, `ingest_file`, `list_keys`, `delete_keys`, `delete_prefix` — identiques entre tâches.
