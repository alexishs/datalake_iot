# Pipeline DAGs Airflow (raw → staging → curated) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Orchestrer le pipeline du data lake avec 3 DAGs Airflow (coquilles fines) réutilisant le package `datalake`, en TDD pur Python.

**Architecture:** Toute la logique vit dans `datalake/` (testée avec le faux client S3 `FakeS3` + Polars) ; les DAGs ne font qu'appeler des fonctions « pas » via `runner.checked`. Filigrane auto-réparant (plus ancien `(ligne, jour)` présent en amont, absent en aval). Cascade d'invalidation `raw → staging → curated` portée par `ingest_file`. Spec de référence : [docs/superpowers/specs/2026-06-04-pipeline-dags-airflow-design.md](../specs/2026-06-04-pipeline-dags-airflow-design.md).

**Tech Stack:** Python 3.12, Polars, boto3/botocore, pytest (venv `.venv`), Apache Airflow 2.9 (TaskFlow API), Docker Compose, MinIO.

> **Mise à jour post-implémentation (racine `curated`).** Seule la **racine** du layout `curated` a changé : `sensor_readings/line=lineX/...` → **`production_lines/line=lineX/...`** (cohérence de racine avec staging). Le **`line=` Hive est conservé** : `curated` **fusionne** les 5 lignes en table unifiée, `line` y reste une partition Hive (≠ staging, segment simple `lineX`). `consolidation._curated_days` réutilise `harmonization.days_from_paths` (qui gère `line=lineX` comme `lineX`). **Les blocs de code ci-dessous montrent `sensor_readings/`** ; la version livrée utilise `production_lines/line=...` (cf. architecture.md §4 et le code).

> **⚠️ Règle de ce projet — AUCUN commit.** Ne lancez **aucun** `git commit` ni `git add`. L'utilisateur committe lui-même à la fin, après sa revue. Les étapes ci-dessous s'arrêtent donc à « tests verts » ; il n'y a pas d'étape de commit.
>
> **Conventions :** lancer Python via `.venv/bin/python` / `.venv/bin/pytest`. **Polars, jamais pandas.** Vouvoiement dans docstrings/commentaires. Markdown sans hard-wrap. Le lint `ruff` du projet impose le **typage** (`ANN`) : annotez tous les paramètres et retours (le client boto3 se type `botocore.client.BaseClient`). **Regroupez tous les `import` en tête de fichier** (ruff `E402`/`I001` ; quand une tâche « ajoute » un import à un fichier de test, fusionnez-le avec le bloc d'imports existant en haut, ne l'insérez pas au milieu). Vérifier `.venv/bin/ruff check .` à la fin de chaque tâche de code.

---

## Structure des fichiers

| Fichier | Responsabilité | Action |
|---|---|---|
| `datalake/runner.py` | + `checked(Result) -> str` (lève si échec) | Modifier |
| `datalake/ingestion.py` | + cascade `curated` ; helpers `_line_year_month`, `curated_partition_prefix`, `CURATED_BUCKET` | Modifier |
| `datalake/harmonization.py` | `raw → staging` : filigrane + harmonisation d'une journée + `main()` | Créer |
| `datalake/consolidation.py` | `staging → curated` : filigrane + consolidation d'une journée + `main()` | Créer |
| `tests/conftest.py` | + `FakeS3.get_object` | Modifier |
| `tests/test_runner.py` | + tests de `checked` | Modifier |
| `tests/test_ingestion.py` | + test cascade `curated` | Modifier |
| `tests/test_harmonization.py` | tests du module harmonization | Créer |
| `tests/test_consolidation.py` | tests du module consolidation | Créer |
| `dags/ingestion_raw.py` | DAG 1 (coquille) | Créer |
| `dags/harmonisation_staging.py` | DAG 2 (coquille) | Créer |
| `dags/consolidation_curated.py` | DAG 3 (coquille) | Créer |
| `compose.yaml` | montage `./data:/opt/airflow/data:ro` | Modifier |
| `docs/architecture.md`, `README.md`, `rapport/rapport.md` | doc (cascade curated, DAGs) | Modifier |

---

# PHASE 1 — Socle d'orchestration + DAG 1 (ingestion → raw)

## Task 1.1 : `runner.checked`

**Files:** Modifier `datalake/runner.py` · Test `tests/test_runner.py`

- [ ] **Step 1 : Écrire le test (ajouter à `tests/test_runner.py`)**

```python
import pytest

from datalake.runner import Result, checked


def test_checked_ok_renvoie_le_libelle() -> None:
    assert checked(Result("f.csv", "déposé", True)) == "f.csv — déposé"


def test_checked_echec_leve() -> None:
    with pytest.raises(RuntimeError, match="ÉCHEC MD5"):
        checked(Result("f.csv", "ÉCHEC MD5", False))
```

> Si `import pytest` est déjà présent en tête du fichier, ne pas le dupliquer ; ajouter `checked` à la ligne `from datalake.runner import ...`.

- [ ] **Step 2 : Lancer (doit échouer)**

Run : `.venv/bin/pytest tests/test_runner.py -v`
Expected : FAIL (`ImportError: cannot import name 'checked'`).

- [ ] **Step 3 : Implémenter (ajouter à la fin de `datalake/runner.py`)**

```python
def checked(result: Result) -> str:
    """Renvoie un libellé « label — statut » si succès, lève `RuntimeError` sinon.

    Adaptateur pour Airflow : une tâche échoue (rouge) si l'exception remonte.
    La logique métier (qui produit le `Result`) reste, elle, dans le package.
    """
    libelle = f"{result.label} — {result.statut}"
    if not result.ok:
        raise RuntimeError(libelle)
    return libelle
```

- [ ] **Step 4 : Lancer (doit passer)**

Run : `.venv/bin/pytest tests/test_runner.py -v` → PASS. Puis `.venv/bin/ruff check datalake/runner.py tests/test_runner.py`.

---

## Task 1.2 : Cascade `curated` dans `ingest_file`

**Files:** Modifier `datalake/ingestion.py` · Test `tests/test_ingestion.py`

- [ ] **Step 1 : Écrire le test (ajouter à `tests/test_ingestion.py`)**

```python
def test_reimport_invalide_aussi_curated(tmp_path: Path, fake_s3: FakeS3) -> None:
    f = _line_csv(tmp_path / "LineA_Stable_10K.csv")
    # un dérivé existe dans curated pour la même (ligne, mois) :
    fake_s3.put_object(
        Bucket="curated",
        Key="sensor_readings/line=lineA/year=2025/month=05/day=01/part.parquet",
        Body=io.BytesIO(b"derive"),
    )
    # raw contient une ANCIENNE version (MD5 différent) -> ré-import :
    key = "production_lines/lineA/year=2025/month=05/LineA_Stable_10K.csv"
    fake_s3.put_object(Bucket="raw", Key=key, Body=io.BytesIO(b"ancien"))
    res = ingestion.ingest_file(f, client=fake_s3)
    assert res.ok and res.statut == "ré-importé"
    # curated de la (ligne, mois) vidé par la cascade :
    assert list_keys(fake_s3, "curated", "sensor_readings/line=lineA/year=2025/month=05/") == []
```

> `_line_csv`, `io`, `Path`, `list_keys`, `ingestion`, `fake_s3` sont déjà importés/présents dans ce fichier (cf. tests existants).

- [ ] **Step 2 : Lancer (doit échouer)**

Run : `.venv/bin/pytest tests/test_ingestion.py::test_reimport_invalide_aussi_curated -v`
Expected : FAIL (curated non vidé — il reste 1 clé).

- [ ] **Step 3 : Modifier `datalake/ingestion.py`**

3a. Ajouter la constante après `STAGING_BUCKET` :

```python
CURATED_BUCKET = "curated"
```

3b. Ajouter, après `partition_prefix`, le préfixe `curated` et un helper de champs :

```python
def curated_partition_prefix(line: str, year: int, month: int) -> str:
    """Préfixe de partition curated (mois) : sensor_readings/line=.../year=.../month=.../."""
    return f"sensor_readings/line={line}/year={year}/month={month:02d}/"


def _line_year_month(path: Path) -> tuple[str, int, int]:
    """(ligne, année, mois) d'un fichier. Lève ValueError si >1 mois (garde-fou §12)."""
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
    return line_id(path.name), years[0], months[0]
```

3c. Réécrire `partition_key` pour s'appuyer sur `_line_year_month` (signature inchangée) :

```python
def partition_key(path: Path | str) -> tuple[str, str]:
    """Retourne (prefix, key) pour un fichier. Lève ValueError si >1 mois (garde-fou)."""
    path = Path(path)
    line, year, month = _line_year_month(path)
    prefix = partition_prefix(line, year, month)
    return prefix, prefix + path.name
```

3d. Réécrire `ingest_file` pour ajouter la cascade `curated` (branche ré-import) :

```python
def ingest_file(path: Path | str, client: BaseClient | None = None) -> Result:
    """Dépose un CSV dans `raw/` (byte-identique, MD5) ; idempotent + cascade staging & curated."""
    path = Path(path)
    client = client or get_s3_client()
    line, year, month = _line_year_month(path)      # garde-fou inclus
    prefix = partition_prefix(line, year, month)
    key = prefix + path.name
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
    # cascade : invalider la même (ligne, mois) en `staging` PUIS `curated` (après confirmation raw)
    delete_prefix(client, STAGING_BUCKET, prefix)
    delete_prefix(client, CURATED_BUCKET, curated_partition_prefix(line, year, month))
    return Result(path.name, "ré-importé", True)
```

- [ ] **Step 4 : Lancer (tout doit passer)**

Run : `.venv/bin/pytest tests/test_ingestion.py -v` → PASS (anciens + nouveau). Puis suite complète `.venv/bin/pytest -q` et `.venv/bin/ruff check datalake/ingestion.py tests/test_ingestion.py`.

---

## Task 1.3 : DAG `ingestion_raw` + montage `data/`

**Files:** Créer `dags/ingestion_raw.py` · Modifier `compose.yaml`

- [ ] **Step 1 : Créer `dags/ingestion_raw.py`**

```python
"""DAG 1 — ingestion brute : dépose les CSV de data/ dans raw/ (C19).

Coquille fine : aucune logique métier ici. Une tâche par CSV (dynamic task
mapping) appelle `ingest_file` — exactement la fonction du CLI
`python -m datalake.ingestion`. Idempotent (skip si MD5 identique).
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from datalake.explore import csv_paths
from datalake.ingestion import ingest_file
from datalake.runner import checked

DATA_DIR = "/opt/airflow/data"  # ./data monté ici (cf. compose.yaml)


@dag(
    dag_id="ingestion_raw",
    description="Dépose les CSV de data/ dans raw/ (byte-identique + MD5, idempotent).",
    schedule=None,  # déclenché manuellement (les CSV arrivent par `download`)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["c19", "ingestion", "raw"],
)
def ingestion_raw() -> None:
    @task
    def lister_csv() -> list[str]:
        return [str(p) for p in csv_paths(DATA_DIR)]

    @task
    def ingerer(path: str) -> str:
        return checked(ingest_file(path))

    ingerer.expand(path=lister_csv())


ingestion_raw()
```

- [ ] **Step 2 : Monter `data/` dans Airflow (`compose.yaml`)**

Dans l'ancre `x-airflow-common` → `volumes`, ajouter la ligne `./data` (lecture seule) :

```yaml
  volumes:
    - ./dags:/opt/airflow/dags
    - ./datalake:/opt/airflow/datalake
    - ./data:/opt/airflow/data:ro
    - airflow-logs:/opt/airflow/logs
```

- [ ] **Step 3 : Recréer Airflow pour appliquer le montage**

Run : `docker compose up -d airflow-scheduler airflow-webserver`
Expected : conteneurs recréés (`Recreated` / `Started`).

- [ ] **Step 4 : Contrôle d'intégrité DagBag**

Run : `docker compose exec airflow-scheduler airflow dags list-import-errors`
Expected : **aucune erreur** (sortie vide ou « No data found »).

Run : `docker compose exec airflow-scheduler airflow dags list | grep ingestion_raw`
Expected : la ligne `ingestion_raw` apparaît.

- [ ] **Step 5 : Déclencher le DAG et vérifier**

Run : `docker compose exec airflow-scheduler airflow dags test ingestion_raw 2025-01-01`
Expected : exécution OK ; dans les logs, 5 tâches `ingerer` mappées renvoyant `LineX_… — inchangé (MD5)` (raw déjà peuplé) ou `— ré-importé`. Aucune tâche en échec.

---

# PHASE 2 — Module `harmonization` + DAG 2 (raw → staging)

## Task 2.1 : `FakeS3.get_object`

**Files:** Modifier `tests/conftest.py` · Test `tests/test_fakes.py`

- [ ] **Step 1 : Écrire le test (ajouter à `tests/test_fakes.py`)**

```python
def test_fake_get_object_renvoie_le_corps(fake_s3: FakeS3) -> None:
    fake_s3.put_object(Bucket="raw", Key="a/x.csv", Body=io.BytesIO(b"coucou"))
    assert fake_s3.get_object(Bucket="raw", Key="a/x.csv")["Body"].read() == b"coucou"


def test_fake_get_object_absent_leve(fake_s3: FakeS3) -> None:
    with pytest.raises(ClientError):
        fake_s3.get_object(Bucket="raw", Key="absent")
```

- [ ] **Step 2 : Lancer (doit échouer)**

Run : `.venv/bin/pytest tests/test_fakes.py -v` → FAIL (`AttributeError: ... 'get_object'`).

- [ ] **Step 3 : Ajouter `get_object` à la classe `FakeS3` (`tests/conftest.py`)**

```python
    def get_object(self, Bucket: str, Key: str) -> dict:
        if (Bucket, Key) not in self.store:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "GetObject")
        return {"Body": io.BytesIO(self.store[(Bucket, Key)])}
```

> Ajouter `import io` en tête de `conftest.py` s'il n'y est pas.

- [ ] **Step 4 : Lancer (doit passer)**

Run : `.venv/bin/pytest tests/test_fakes.py -v` → PASS. Puis `.venv/bin/ruff check tests/conftest.py tests/test_fakes.py`.

---

## Task 2.2 : `harmonization` — préfixes + harmonisation d'un DataFrame

**Files:** Créer `datalake/harmonization.py` · Test `tests/test_harmonization.py`

- [ ] **Step 1 : Écrire le test (`tests/test_harmonization.py`)**

```python
from __future__ import annotations

import polars as pl

from datalake.harmonization import _harmonize_frame, staging_day_prefix


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
```

- [ ] **Step 2 : Lancer (doit échouer)**

Run : `.venv/bin/pytest tests/test_harmonization.py -v` → FAIL (`ModuleNotFoundError`).

- [ ] **Step 3 : Créer `datalake/harmonization.py` (en-tête + ces fonctions)**

```python
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
from datalake.runner import Result
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
```

- [ ] **Step 4 : Lancer (doit passer)**

Run : `.venv/bin/pytest tests/test_harmonization.py -v` → PASS (3 tests). Puis `.venv/bin/ruff check datalake/harmonization.py tests/test_harmonization.py`.

---

## Task 2.3 : `harmonization` — filigrane (jours raw/staging, jour à traiter)

**Files:** Modifier `datalake/harmonization.py` · Test `tests/test_harmonization.py`

- [ ] **Step 1 : Écrire les tests (ajouter à `tests/test_harmonization.py`)**

```python
import io
from datetime import date
from typing import TYPE_CHECKING

from datalake import harmonization

if TYPE_CHECKING:
    from conftest import FakeS3

_CSV_LINEA = (
    "timestamp,temperature,pressure,elapsed_time,label\n"
    "2025-05-01 00:00:00,180.0,1.2,5.0,0\n"
    "2025-05-02 00:00:00,181.0,1.3,6.0,1\n"
).encode()


def _seed_raw_linea(fake_s3: "FakeS3") -> None:
    fake_s3.put_object(
        Bucket="raw",
        Key="production_lines/lineA/year=2025/month=05/LineA.csv",
        Body=io.BytesIO(_CSV_LINEA),
    )


def test_jour_a_traiter_prend_le_plus_ancien(fake_s3: "FakeS3") -> None:
    _seed_raw_linea(fake_s3)
    assert harmonization.jour_a_traiter(fake_s3) == date(2025, 5, 1)


def test_jour_a_traiter_none_si_a_jour(fake_s3: "FakeS3") -> None:
    _seed_raw_linea(fake_s3)
    # staging contient déjà les deux jours
    for d in (1, 2):
        fake_s3.put_object(
            Bucket="staging",
            Key=f"production_lines/lineA/year=2025/month=05/day=0{d}/part.parquet",
            Body=io.BytesIO(b"x"),
        )
    assert harmonization.jour_a_traiter(fake_s3) is None
```

- [ ] **Step 2 : Lancer (doit échouer)**

Run : `.venv/bin/pytest tests/test_harmonization.py -k jour_a_traiter -v` → FAIL (`AttributeError: ... 'jour_a_traiter'`).

- [ ] **Step 3 : Ajouter à `datalake/harmonization.py`**

```python
def _lines_in_raw(client: BaseClient) -> list[str]:
    """Lignes présentes dans raw (déduites des chemins production_lines/<line>/...)."""
    lignes = {k.split("/")[1] for k in list_keys(client, RAW_BUCKET, "production_lines/")}
    return sorted(lignes)


def _raw_csv_key(client: BaseClient, line: str) -> str | None:
    """Clé du CSV brut d'une ligne (un seul fichier par partition mois)."""
    csvs = [k for k in list_keys(client, RAW_BUCKET, f"production_lines/{line}/") if k.endswith(".csv")]
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


def _days_from_paths(client: BaseClient, bucket: str, root: str, line_index: int) -> dict[str, set[date]]:
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
    return _days_from_paths(client, STAGING_BUCKET, "production_lines/", line_index=1)


def jour_a_traiter(client: BaseClient) -> date | None:
    """Plus ancien (ligne, jour) présent dans raw mais absent de staging (filigrane)."""
    raw, stg = raw_days(client), staging_days(client)
    manquants = {d for line, jours in raw.items() for d in jours if d not in stg.get(line, set())}
    return min(manquants) if manquants else None
```

- [ ] **Step 4 : Lancer (doit passer)**

Run : `.venv/bin/pytest tests/test_harmonization.py -v` → PASS. Puis `.venv/bin/ruff check datalake/harmonization.py`.

---

## Task 2.4 : `harmonization` — `harmonize_day`, `harmonize_step`, `main`

**Files:** Modifier `datalake/harmonization.py` · Test `tests/test_harmonization.py`

- [ ] **Step 1 : Écrire les tests (ajouter à `tests/test_harmonization.py`)**

```python
def _staging_parquet(fake_s3: "FakeS3", line: str, y: int, m: int, d: int) -> pl.DataFrame | None:
    prefix = harmonization.staging_day_prefix(line, y, m, d)
    keys = [k for k in fake_s3.store if k[0] == "staging" and k[1].startswith(prefix)]
    if not keys:
        return None
    return pl.read_parquet(io.BytesIO(fake_s3.store[keys[0]]))


def test_harmonize_day_ecrit_la_partition_du_jour(fake_s3: "FakeS3") -> None:
    _seed_raw_linea(fake_s3)
    res = harmonization.harmonize_day(fake_s3, date(2025, 5, 1))
    assert res and all(r.ok for r in res)
    df = _staging_parquet(fake_s3, "lineA", 2025, 5, 1)
    assert df is not None
    assert df.height == 1                       # seule la journée du 1er
    assert df["day"].unique().to_list() == [1]
    assert df.columns == harmonization.TARGET_COLUMNS


def test_harmonize_step_puis_a_jour(fake_s3: "FakeS3") -> None:
    _seed_raw_linea(fake_s3)
    r1 = harmonization.harmonize_step(fake_s3)   # traite le 1er mai
    assert r1.ok and r1.statut != "à jour"
    r2 = harmonization.harmonize_step(fake_s3)   # traite le 2 mai
    assert r2.ok and r2.statut != "à jour"
    r3 = harmonization.harmonize_step(fake_s3)   # plus rien
    assert r3.ok and r3.statut == "à jour"


def test_harmonize_day_idempotent(fake_s3: "FakeS3") -> None:
    _seed_raw_linea(fake_s3)
    harmonization.harmonize_day(fake_s3, date(2025, 5, 1))
    harmonization.harmonize_day(fake_s3, date(2025, 5, 1))  # rejoué
    keys = [k for k in fake_s3.store
            if k[0] == "staging" and "day=01" in k[1]]
    assert len(keys) == 1                        # une seule partition, pas de doublon
```

- [ ] **Step 2 : Lancer (doit échouer)**

Run : `.venv/bin/pytest tests/test_harmonization.py -k "harmonize_day or harmonize_step" -v` → FAIL.

- [ ] **Step 3 : Ajouter à `datalake/harmonization.py`**

```python
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
    jour = jour_a_traiter(client)
    if jour is None:
        return Result("staging", "à jour", True)
    res = harmonize_day(client, jour)
    return Result(str(jour), f"{len(res)} ligne(s) harmonisée(s)", all(r.ok for r in res))


def main() -> int:
    """Draine raw → staging (toutes les journées en attente). Lançable en CLI."""
    client = get_s3_client()
    print("Harmonisation raw → staging")
    while True:
        r = harmonize_step(client)
        print(f"  {'✓' if r.ok else '✗'} {r.label} — {r.statut}")
        if not r.ok:
            return 1
        if r.statut == "à jour":
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4 : Lancer (doit passer)**

Run : `.venv/bin/pytest tests/test_harmonization.py -v` → PASS (tous). Puis suite complète `.venv/bin/pytest -q` et `.venv/bin/ruff check datalake/harmonization.py tests/test_harmonization.py`.

- [ ] **Step 5 : Vérification manuelle (drainage réel dans `dev`)**

Run : `docker compose exec dev python -m datalake.harmonization`
Expected : `Harmonisation raw → staging` puis des lignes `✓ 2025-01-01 — 1 ligne(s) harmonisée(s)` … jusqu'à `✓ staging — à jour`, code de sortie 0. (Les CSV doivent être dans `raw` — sinon lancer d'abord `python -m datalake.ingestion`.)

Run (contrôle partitions staging) :
```
docker compose exec dev python -c "from datalake.storage import get_s3_client, list_keys; print('\n'.join(list_keys(get_s3_client(),'staging','production_lines/')))"
```
Expected : des objets `…/lineX/year=2025/month=MM/day=DD/part.parquet`.

---

## Task 2.5 : DAG `harmonisation_staging`

**Files:** Créer `dags/harmonisation_staging.py`

- [ ] **Step 1 : Créer `dags/harmonisation_staging.py`**

```python
"""DAG 2 — harmonisation raw → staging (C19, fil-de-l'eau).

Coquille fine : appelle `harmonize_step` (un pas = une journée, choisie par le
filigrane) toutes les minutes. Toute la logique vit dans datalake.harmonization.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from datalake.harmonization import harmonize_step
from datalake.runner import checked


@dag(
    dag_id="harmonisation_staging",
    description="Harmonise une journée raw → staging (filigrane, une journée par minute).",
    schedule="* * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["c19", "harmonisation", "staging"],
)
def harmonisation_staging() -> None:
    @task
    def harmoniser() -> str:
        return checked(harmonize_step())

    harmoniser()


harmonisation_staging()
```

- [ ] **Step 2 : Contrôle d'intégrité DagBag**

Run : `docker compose exec airflow-scheduler airflow dags list-import-errors` → aucune erreur.
Run : `docker compose exec airflow-scheduler airflow dags list | grep harmonisation_staging` → présent.

- [ ] **Step 3 : Test du DAG**

Run : `docker compose exec airflow-scheduler airflow dags test harmonisation_staging 2025-01-01`
Expected : tâche `harmoniser` verte ; log `… — N ligne(s) harmonisée(s)` ou `staging — à jour`.

---

# PHASE 3 — Module `consolidation` + DAG 3 (staging → curated)

## Task 3.1 : Module `consolidation`

**Files:** Créer `datalake/consolidation.py` · Test `tests/test_consolidation.py`

- [ ] **Step 1 : Écrire les tests (`tests/test_consolidation.py`)**

```python
from __future__ import annotations

import io
from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from datalake import consolidation

if TYPE_CHECKING:
    from conftest import FakeS3


def _seed_staging_day(fake_s3: "FakeS3", line: str, y: int, m: int, d: int) -> None:
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
        "sensor_readings/line=lineA/year=2025/month=05/day=01/"


def test_jour_a_traiter_filigrane_curated(fake_s3: "FakeS3") -> None:
    _seed_staging_day(fake_s3, "lineE", 2025, 1, 1)
    assert consolidation.jour_a_traiter(fake_s3) == date(2025, 1, 1)


def test_consolidate_day_ecrit_curated(fake_s3: "FakeS3") -> None:
    _seed_staging_day(fake_s3, "lineE", 2025, 1, 1)
    res = consolidation.consolidate_day(fake_s3, date(2025, 1, 1))
    assert res and all(r.ok for r in res)
    prefix = consolidation.curated_day_prefix("lineE", 2025, 1, 1)
    keys = [k for k in fake_s3.store if k[0] == "curated" and k[1].startswith(prefix)]
    assert len(keys) == 1
    df = pl.read_parquet(io.BytesIO(fake_s3.store[keys[0]]))
    assert df["line"].unique().to_list() == ["lineE"]


def test_consolidate_step_puis_a_jour(fake_s3: "FakeS3") -> None:
    _seed_staging_day(fake_s3, "lineE", 2025, 1, 1)
    r1 = consolidation.consolidate_step(fake_s3)
    assert r1.ok and r1.statut != "à jour"
    r2 = consolidation.consolidate_step(fake_s3)
    assert r2.ok and r2.statut == "à jour"
```

- [ ] **Step 2 : Lancer (doit échouer)**

Run : `.venv/bin/pytest tests/test_consolidation.py -v` → FAIL (`ModuleNotFoundError`).

- [ ] **Step 3 : Créer `datalake/consolidation.py`**

```python
"""Consolidation staging → curated (C19, 3ᵉ DAG assumé — cf. architecture §3.3/§11).

Réunit les journées harmonisées dans la table unifiée curated (colonne `line`
déjà présente), partition au jour `sensor_readings/line=lineX/...`. Même patron
que l'harmonisation : un pas = une journée, filigrane curated vs staging.
Réutilise les utilitaires de chemins de `harmonization`.
"""
from __future__ import annotations

import io
from datetime import date

import polars as pl
from botocore.client import BaseClient

from datalake.harmonization import STAGING_BUCKET, staging_day_prefix, staging_days
from datalake.runner import Result
from datalake.storage import delete_prefix, get_s3_client, list_keys

CURATED_BUCKET = "curated"


def curated_day_prefix(line: str, year: int, month: int, day: int) -> str:
    """Préfixe de partition curated (jour) : sensor_readings/line=.../year=/month=/day=/."""
    return f"sensor_readings/line={line}/year={year}/month={month:02d}/day={day:02d}/"


def _curated_days(client: BaseClient) -> dict[str, set[date]]:
    """Jours présents par ligne dans curated (depuis les chemins line=.../day=...)."""
    out: dict[str, set[date]] = {}
    for k in list_keys(client, CURATED_BUCKET, "sensor_readings/"):
        parts = k.split("/")
        try:
            line = next(p for p in parts if p.startswith("line=")).split("=")[1]
            y = int(next(p for p in parts if p.startswith("year=")).split("=")[1])
            m = int(next(p for p in parts if p.startswith("month=")).split("=")[1])
            d = int(next(p for p in parts if p.startswith("day=")).split("=")[1])
        except (StopIteration, ValueError):
            continue
        out.setdefault(line, set()).add(date(y, m, d))
    return out


def jour_a_traiter(client: BaseClient) -> date | None:
    """Plus ancien (ligne, jour) présent dans staging mais absent de curated."""
    stg, cur = staging_days(client), _curated_days(client)
    manquants = {d for line, jours in stg.items() for d in jours if d not in cur.get(line, set())}
    return min(manquants) if manquants else None


def _read_staging_day(client: BaseClient, line: str, jour: date) -> pl.DataFrame:
    """Lit la partition staging d'un (ligne, jour) en DataFrame."""
    prefix = staging_day_prefix(line, jour.year, jour.month, jour.day)
    key = next(k for k in list_keys(client, STAGING_BUCKET, prefix))
    return pl.read_parquet(io.BytesIO(client.get_object(Bucket=STAGING_BUCKET, Key=key)["Body"].read()))


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
    jour = jour_a_traiter(client)
    if jour is None:
        return Result("curated", "à jour", True)
    res = consolidate_day(client, jour)
    return Result(str(jour), f"{len(res)} ligne(s) consolidée(s)", all(r.ok for r in res))


def main() -> int:
    """Draine staging → curated. Lançable en CLI."""
    client = get_s3_client()
    print("Consolidation staging → curated")
    while True:
        r = consolidate_step(client)
        print(f"  {'✓' if r.ok else '✗'} {r.label} — {r.statut}")
        if not r.ok:
            return 1
        if r.statut == "à jour":
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4 : Lancer (doit passer)**

Run : `.venv/bin/pytest tests/test_consolidation.py -v` → PASS. Puis suite complète `.venv/bin/pytest -q` et `.venv/bin/ruff check datalake/consolidation.py tests/test_consolidation.py`.

- [ ] **Step 5 : Vérification manuelle (dans `dev`)**

Run : `docker compose exec dev python -m datalake.consolidation`
Expected : `Consolidation staging → curated` puis `✓ … — N ligne(s) consolidée(s)` jusqu'à `✓ curated — à jour`. (Nécessite `staging` peuplé : lancer `python -m datalake.harmonization` avant.)

---

## Task 3.2 : DAG `consolidation_curated`

**Files:** Créer `dags/consolidation_curated.py`

- [ ] **Step 1 : Créer `dags/consolidation_curated.py`**

```python
"""DAG 3 — consolidation staging → curated (C19, assumé).

Coquille fine : appelle `consolidate_step` (un pas = une journée, filigrane
curated vs staging) toutes les minutes. Logique dans datalake.consolidation.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from datalake.consolidation import consolidate_step
from datalake.runner import checked


@dag(
    dag_id="consolidation_curated",
    description="Consolide une journée staging → curated (filigrane, une journée par minute).",
    schedule="* * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["c19", "consolidation", "curated"],
)
def consolidation_curated() -> None:
    @task
    def consolider() -> str:
        return checked(consolidate_step())

    consolider()


consolidation_curated()
```

- [ ] **Step 2 : Contrôle d'intégrité DagBag**

Run : `docker compose exec airflow-scheduler airflow dags list-import-errors` → aucune erreur.
Run : `docker compose exec airflow-scheduler airflow dags list | grep consolidation_curated` → présent.

- [ ] **Step 3 : Test du DAG**

Run : `docker compose exec airflow-scheduler airflow dags test consolidation_curated 2025-01-01`
Expected : tâche `consolider` verte.

---

## Task 3.3 : Documentation (architecture, README, rapport)

**Files:** Modifier `docs/architecture.md`, `README.md`, `rapport/rapport.md`

- [ ] **Step 1 : `docs/architecture.md` — étendre la cascade à `curated`**

Dans §3.1 (puce « (Ré)import idempotent & cascade »), §11 (« Granularités, cadence, filigrane & cascade ») et §12 (règle 12), préciser que le (ré)import d'une `(ligne, mois)` vide **`staging` ET `curated`** pour cette `(ligne, mois)` (et non `staging` seul). Reformuler en une seule ligne par bloc (pas de hard-wrap).

- [ ] **Step 2 : `README.md` — tracking des DAGs**

Passer la ligne « **C19 · J3-4** | 3 DAGs … » à ✅ et pointer vers `dags/`. Compléter le paragraphe « package métier » avec `harmonization.py` et `consolidation.py`. Ajouter au flux (section « Commandes du package ») : `python -m datalake.harmonization` (raw → staging) et `python -m datalake.consolidation` (staging → curated).

- [ ] **Step 3 : `rapport/rapport.md` — journal**

Ajouter une entrée datée (date réelle de réalisation) sous §2 décrivant : 3 DAGs (coquilles fines), modules `harmonization`/`consolidation`, fil-de-l'eau (filigrane auto-réparant), cascade `raw → staging → curated`, et la stratégie de test (TDD pur Python + intégrité DagBag). Une ligne par bloc.

- [ ] **Step 4 : Vérifications finales**

Run : `.venv/bin/pytest -q` → tout PASS.
Run : `.venv/bin/ruff check .` → « All checks passed! ».
Run : `grep -rnE "\b(tu|ton|ta|tes)\b" README.md rapport/rapport.md docs/architecture.md || echo "OK vouvoiement"`.
Run (intégrité des 3 DAGs) : `docker compose exec airflow-scheduler airflow dags list-import-errors` → vide.

---

## Auto-revue (fin de plan)

- **Couverture du spec :** `checked` (T1.1) ; cascade curated dans `ingest_file` (T1.2) ; DAG 1 + montage data (T1.3) ; `FakeS3.get_object` (T2.1) ; harmonisation — frame/préfixes (T2.2), filigrane (T2.3), `harmonize_day/step/main` (T2.4), DAG 2 (T2.5) ; consolidation module (T3.1) + DAG 3 (T3.2) ; docs (T3.3). ✅
- **Manuel == DAG :** chaque module a un `main()` (drainage) et un `*_step()` appelé à l'identique par le DAG. ✅
- **Pas de placeholder ; chemins et commandes exacts ; AUCUN commit.**
- **Cohérence des noms :** `checked`, `CURATED_BUCKET`, `curated_partition_prefix`, `_line_year_month`, `staging_day_prefix`, `curated_day_prefix`, `jour_a_traiter`, `raw_days`, `staging_days`, `harmonize_day/step`, `consolidate_day/step`, `TARGET_COLUMNS` — identiques d'une tâche à l'autre.
- **Filigrane auto-réparant :** la cascade supprime les partitions aval → redeviennent « absentes » → recalcul. ✅
