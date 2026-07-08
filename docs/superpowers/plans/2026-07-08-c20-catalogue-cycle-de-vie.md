# Catalogue OpenMetadata & cycle de vie (C20) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cataloguer le data lake dans OpenMetadata (fiches par ligne + lignage `raw→staging→curated→archive`) et implémenter le cycle de vie (archivage `raw→archive` par DAG + suppression par ILM).

**Architecture:** Deux sous-ensembles. (A) **Cycle de vie** — module `datalake/archive.py` (déterministe, TDD pur Python) + DAG `archivage` + règle ILM ; agit sur les buckets par conception. (B) **Catalogue OpenMetadata** — intégration d'un outil tiers (connecteur S3, fiches, pipelines, lignage via `inlets`/`outlets`), **observationnel** (ne modifie ni données ni comportement des DAGs). Spec : [docs/superpowers/specs/2026-07-08-c20-catalogue-cycle-de-vie-design.md](../specs/2026-07-08-c20-catalogue-cycle-de-vie-design.md).

**Tech Stack:** Python 3.12, Polars, boto3/botocore, pytest (venv `.venv`), Apache Airflow 2.9 (TaskFlow API), MinIO + `mc`, OpenMetadata 1.5.6.

## Global Constraints

- **AUCUN commit** : ne lancez ni `git add` ni `git commit`. L'utilisateur committe après revue. Les tâches s'arrêtent à « tests/vérif verts ».
- **Polars, jamais pandas.** Vouvoiement dans docstrings/commentaires. Markdown sans hard-wrap.
- **Ruff** impose le typage (`ANN`) : annotez paramètres et retours (client boto3 = `botocore.client.BaseClient`). Imports en tête de fichier. Vérifier `.venv/bin/ruff check .` après chaque tâche de code.
- **Non-invasivité (côté catalogue OM)** : ne modifiez **pas** la logique des DAGs ni les buckets. Seule touche permise aux DAGs : des annotations `inlets`/`outlets` (métadonnées de lignage, aucun effet d'exécution), validées par le contrôle DagBag.
- **Ordre d'exécution** : la **Phase 1 (cycle de vie)** est faite **avant** la **Phase 2 (catalogue OM)** — le lignage OM a besoin que le DAG `archivage` existe, et on sécurise d'abord la partie déterministe. *(C'est l'inverse de la numérotation du spec, volontairement.)*

---

# PHASE 1 — Cycle de vie (déterministe, TDD)

## Task 1.1 : `FakeS3.copy_object`

**Files:** Modifier `tests/conftest.py` · Test `tests/test_fakes.py`

**Interfaces:**
- Produces: `FakeS3.copy_object(Bucket, Key, CopySource) -> dict` — copie in-memory `store[CopySource] → store[(Bucket, Key)]`.

- [ ] **Step 1 — Test (ajouter à `tests/test_fakes.py`, imports déjà en tête)**
```python
def test_fake_copy_object(fake_s3: FakeS3) -> None:
    fake_s3.put_object(Bucket="raw", Key="a/x.csv", Body=io.BytesIO(b"data"))
    fake_s3.copy_object(Bucket="archive", Key="a/x.csv",
                        CopySource={"Bucket": "raw", "Key": "a/x.csv"})
    assert fake_s3.get_object(Bucket="archive", Key="a/x.csv")["Body"].read() == b"data"
```

- [ ] **Step 2 — Lancer (échoue)** : `.venv/bin/pytest tests/test_fakes.py -k copy_object -v` → `AttributeError`.

- [ ] **Step 3 — Ajouter la méthode à `FakeS3` (après `get_object`)**
```python
    def copy_object(self, Bucket: str, Key: str, CopySource: dict) -> dict:
        src = (CopySource["Bucket"], CopySource["Key"])
        self.store[(Bucket, Key)] = self.store[src]
        return {"CopyObjectResult": {}}
```

- [ ] **Step 4 — Vérifier** : `.venv/bin/pytest tests/test_fakes.py -v` → PASS ; `.venv/bin/ruff check tests/conftest.py tests/test_fakes.py`.

---

## Task 1.2 : `archive.py` — sélection des mois à archiver

**Files:** Créer `datalake/archive.py` · Test `tests/test_archive.py`

**Interfaces:**
- Consumes: `datalake.ingestion.RAW_BUCKET`, `partition_prefix`; `datalake.storage.list_keys`.
- Produces: `mois_a_archiver(client: BaseClient, reference: date, anciennete_mois: int = 18) -> list[tuple[str, int, int]]` (triée, `(line, year, month)`).

- [ ] **Step 1 — Test (`tests/test_archive.py`)**
```python
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
```

- [ ] **Step 2 — Lancer (échoue)** : `.venv/bin/pytest tests/test_archive.py -v` → `ModuleNotFoundError`.

- [ ] **Step 3 — Créer `datalake/archive.py`**
```python
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
```

- [ ] **Step 4 — Vérifier** : `.venv/bin/pytest tests/test_archive.py -v` → PASS. `.venv/bin/ruff check datalake/archive.py tests/test_archive.py` — les imports `CURATED_BUCKET`/`STAGING_BUCKET`/`curated_partition_prefix`/`partition_prefix`/`Result`/`run`/`delete_keys`/`delete_prefix`/`get_s3_client`/`ARCHIVE_BUCKET` non encore utilisés sont **volontaires** (Task 1.3) → tolérez les `F401` sur ceux-là uniquement ; corrigez tout autre avertissement.

---

## Task 1.3 : `archive.py` — déplacement + purge + `main`

**Files:** Modifier `datalake/archive.py` · Test `tests/test_archive.py`

**Interfaces:**
- Produces: `archive_month(client, line, year, month) -> Result` (déplace raw→archive en miroir, purge staging+curated) ; `main(anciennete_mois: int = 18) -> int`.

- [ ] **Step 1 — Tests (ajouter à `tests/test_archive.py`)**
```python
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
    # raw déplacé (miroir) vers archive, supprimé de raw :
    assert ("archive", raw_key) in fake_s3.store
    assert ("raw", raw_key) not in fake_s3.store
    # dérivées purgées :
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
    # réintégration : recopier archive -> raw
    fake_s3.copy_object(Bucket="raw", Key=key, CopySource={"Bucket": "archive", "Key": key})
    from datalake.storage import list_keys
    assert list_keys(fake_s3, "raw", "production_lines/lineE/year=2025/month=01/") == [key]
```

- [ ] **Step 2 — Lancer (échoue)** : `.venv/bin/pytest tests/test_archive.py -k "archive_month or reintegration" -v` → `AttributeError`.

- [ ] **Step 3 — Ajouter à `datalake/archive.py`**
```python
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
    return Result(f"{line} {year}-{month:02d}", f"{len(raw_keys)} objet(s) archivé(s) + dérivés purgés", True)


def main(anciennete_mois: int = 18) -> int:
    """Archive toutes les (ligne, mois) éligibles. Lançable en CLI."""
    client = get_s3_client()
    mois = mois_a_archiver(client, date.today(), anciennete_mois)
    return run(lambda t: archive_month(client, *t), mois, "Archivage raw → archive/ (+ purge staging/curated)")


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4 — Vérifier** : `.venv/bin/pytest tests/test_archive.py -v` → PASS (tous) ; suite complète `.venv/bin/pytest -q` ; `.venv/bin/ruff check .` → **entièrement propre** (imports tous consommés).

---

## Task 1.4 : DAG `archivage`

**Files:** Créer `dags/archivage.py`

**Interfaces:**
- Consumes: `datalake.archive.mois_a_archiver`, `archive_month`; `datalake.runner.checked`; `datalake.storage.get_s3_client`.

- [ ] **Step 1 — Créer `dags/archivage.py`** (coquille fine ; **pas** encore d'`inlets`/`outlets` — ajoutés en Phase 2)
```python
"""DAG C20 — archivage : déplace les (ligne, mois) de plus de N mois de raw vers
archive/, purge staging/curated. Coquille fine (logique dans datalake.archive).
"""
from __future__ import annotations

from datetime import date, datetime

from airflow.decorators import dag, task

from datalake.archive import archive_month, mois_a_archiver
from datalake.runner import checked
from datalake.storage import get_s3_client


@dag(
    dag_id="archivage",
    description="Archive raw → archive/ (+ purge staging/curated) au-delà de 18 mois (démo).",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["c20", "archive", "cycle-de-vie"],
)
def archivage() -> None:
    @task
    def lister_mois() -> list[list]:
        return [list(t) for t in mois_a_archiver(get_s3_client(), date.today())]

    @task
    def archiver(mois: list) -> str:
        line, year, month = mois
        return checked(archive_month(get_s3_client(), line, year, month))

    archiver.expand(mois=lister_mois())


archivage()
```

- [ ] **Step 2 — Lint** : `.venv/bin/ruff check dags/archivage.py` → propre.

- [ ] **Step 3 — Intégrité DagBag** : `docker compose exec airflow-scheduler airflow dags list-import-errors` → aucune erreur ; `docker compose exec airflow-scheduler airflow dags list | grep archivage` → présent.

- [ ] **Step 4 — Test réel** : `docker compose exec airflow-scheduler airflow dags test archivage 2025-01-01` → tâches vertes ; log `lineE 2025-01 — … archivé(s)`. Vérifier ensuite dans `dev` : `raw` lineE janvier vide, `archive` contient l'objet, `staging`/`curated` lineE janvier purgés. **(⚠️ cela déplace réellement des données ; c'est l'effet voulu. Pour rejouer le pipeline ensuite, réintégrer via copie `archive→raw` ou relancer `download`+`ingestion`.)**

---

## Task 1.5 : Règle ILM d'expiration sur `archive/`

**Files:** Modifier `init-scripts/minio/setup.sh`

- [ ] **Step 1 — Ajouter à la fin de `init-scripts/minio/setup.sh`**
```sh
# --- Cycle de vie (ILM) : suppression des objets archivés après ~2 ans (730 j) ---
# Fondé sur l'ÂGE DES OBJETS (date d'upload). Nos objets datant de 2026, la règle
# est configurée mais ne se déclenchera pas avant ~2 ans (documenté, non démontrable).
# rm --all puis add -> idempotent (relançable sans empiler les règles).
mc ilm rule rm --all --force local/archive 2>/dev/null || true
mc ilm rule add local/archive --expire-days 730
echo "ILM : expiration 730 j configurée sur archive/."
```

- [ ] **Step 2 — Appliquer** : `docker compose up minio-init` (relance le job one-shot).
Expected : sortie se terminant par `ILM : expiration 730 j configurée sur archive/.`

- [ ] **Step 3 — Vérifier** : `docker compose exec -T dev sh -c "mc alias set local http://minio:9000 \$MINIO_ROOT_USER \$MINIO_ROOT_PASSWORD >/dev/null 2>&1; mc ilm rule ls local/archive"` — *(si `mc` absent de `dev`, utiliser un conteneur jetable `quay.io/minio/mc` sur le réseau `datalake_iot_datalake` comme dans les vérifications précédentes)*. Expected : une règle d'expiration à 730 jours listée.

> Note : la syntaxe `mc ilm rule add --expire-days` correspond aux versions récentes de `mc` (image `quay.io/minio/mc:latest`). Si la version diffère, adapter (`mc ilm add --expiry-days`).

---

## Task 1.6 : Documentation du cycle de vie

**Files:** Modifier `docs/architecture.md` (§8) · Créer `docs/gouvernance-cycle-de-vie.md` · Modifier `rapport/rapport.md`

- [ ] **Step 1 — Corriger `docs/architecture.md` §8** : remplacer l'affirmation « `archive/` alimenté automatiquement par les règles ILM » par la réalité : **archivage = DAG `archivage`** (`raw→archive` + purge des dérivés, seuil sur la **date des données**) car MinIO ILM ne sait pas transférer localement (transition = tier distant) ; **suppression = ILM expiration** (âge des objets, 730 j) sur `archive/`. Une ligne par bloc (pas de hard-wrap).

- [ ] **Step 2 — Créer `docs/gouvernance-cycle-de-vie.md`** : politique de cycle de vie complète — jalons (archivage 180 j selon l'énoncé ; **démo à 18 mois** pour n'archiver que janvier 2025, écart explicité), mécanisme (DAG d'archivage vs ILM, **pourquoi ILM non retenu pour l'archivage** avec renvoi à la doc MinIO), réintégration (copier `archive→raw`, le filigrane recalcule), et la note « expiration 730 j configurée mais non déclenchable ici (objets 2026) ».

- [ ] **Step 3 — `rapport/rapport.md`** : ajouter une entrée datée (date réelle) sous §2 pour le C20 (volet cycle de vie) : DAG d'archivage `raw→archive` + purge, ILM expiration, réintégration par le filigrane, écarts assumés (18 mois de démo, expiration non déclenchable).

- [ ] **Step 4 — Vérifs** : `grep -rnE "\b(tu|ton|ta|tes)\b" docs/architecture.md docs/gouvernance-cycle-de-vie.md rapport/rapport.md || echo "OK vouvoiement"` ; `.venv/bin/pytest -q` → tout PASS.

---

# PHASE 2 — Catalogue OpenMetadata (intégration outil tiers)

> **Nature des tâches.** Ce sont des tâches d'**intégration** avec OpenMetadata 1.5.6 (connecteur S3, API, `inlets`/`outlets`), pas du code déterministe. Chaque tâche donne l'**objectif**, la **démarche** et des **critères de vérification** ; l'implémenteur confirme les détails exacts (endpoints, jeton, format de manifest/lineage) **contre l'instance réelle** (`http://localhost:8585`, ingestion `:8082`). Les **captures d'écran** produites sont un livrable C20. **Aucune** modification de la logique des DAGs ni des buckets.

## Task 2.1 : Connexion OM ↔ MinIO + ingestion des conteneurs

**Files:** Créer `init-scripts/openmetadata/` (spec d'ingestion + notes)

- [ ] **Step 1 — Créer un service de stockage S3** dans OpenMetadata pointant sur MinIO : endpoint `http://minio:9000`, `awsAccessKeyId`/`awsSecretAccessKey` = compte MinIO (root pour cataloguer les 4 buckets), `endPointURL` + style *path*, région `us-east-1`. Le déclarer via une **spec d'ingestion YAML** (déposée dans `init-scripts/openmetadata/`) ou l'API — pas de dépendance à un clic manuel pour la reproduction.
- [ ] **Step 2 — Lancer l'ingestion de métadonnées** (workflow OpenMetadata) pour découvrir les conteneurs des buckets `raw`, `staging`, `curated`, `archive`. Si l'inférence de schéma des fichiers (colonnes) nécessite un **manifest** OpenMetadata (`openmetadata_storage_manifest.json`) dans les buckets, le créer et le déposer (documenter son contenu dans `init-scripts/openmetadata/`).
- [ ] **Step 3 — Vérifier** (API OM ou UI) : les conteneurs des 4 buckets apparaissent ; en particulier **5 entités côté `raw`** (une par ligne `LineA…E`). Capturer l'écran (liste des conteneurs).
- [ ] **Step 4 — Consigner** dans `init-scripts/openmetadata/README.md` la procédure exacte retenue (service, ingestion, manifest éventuel) pour la reproductibilité.

## Task 2.2 : Enrichissement des 5 fiches + colonnes

**Files:** `init-scripts/openmetadata/` (script d'enrichissement via API/SDK)

- [ ] **Step 1 — Créer/associer un propriétaire** : une équipe ou un utilisateur OpenMetadata « responsable maintenance » (owner des fiches).
- [ ] **Step 2 — Enrichir les 5 fiches `raw`** (une par ligne) via l'API/SDK OpenMetadata : **description**, **propriétaire**, **source** (Zenodo, record `15277168`), **fréquence de collecte** (1 relevé/minute). Script versionné dans `init-scripts/openmetadata/`.
- [ ] **Step 3 — Documenter les colonnes clés** (sur les fiches, ou sur `staging`/`curated` si le schéma y est plus propre) : `temperature` **°C**, `pressure` **bar**, `elapsed_time` (temps de fonctionnement, unité arbitraire, nullable), `label` **0 = nominal / 1 = anomalie**, `timestamp` ISO 8601 (UTC supposé).
- [ ] **Step 4 — Vérifier + capturer** : chaque fiche affiche description/propriétaire/source/fréquence + colonnes documentées (captures pour le rapport).

## Task 2.3 : Pipelines (DAGs) + lignage `inlets`/`outlets`

**Files:** Modifier `dags/*.py` (annotations lignage) · `init-scripts/openmetadata/` (ingestion Airflow)

- [ ] **Step 1 — Annoter le lignage** : ajouter aux tâches des 4 DAGs (`ingestion_raw`, `harmonisation_staging`, `consolidation_curated`, `archivage`) des `inlets`/`outlets` désignant les conteneurs source/cible, de sorte que le graphe soit `raw→staging→curated` **et** `raw→archive`. **Métadonnées uniquement** — ne changez aucune logique métier.
- [ ] **Step 2 — Contrôle DagBag** : `docker compose exec airflow-scheduler airflow dags list-import-errors` → **aucune erreur** (les annotations n'ont rien cassé). `.venv/bin/ruff check dags/` → propre.
- [ ] **Step 3 — Cataloguer les pipelines** : configurer le connecteur **Airflow** d'OpenMetadata vers l'**Airflow métier** (`:8080`) et lancer l'ingestion des pipelines + lignage. (Déposer la spec/notes dans `init-scripts/openmetadata/`.)
- [ ] **Step 4 — Vérifier + capturer** : dans OpenMetadata, les 4 DAGs apparaissent comme *Pipelines* et le **graphe de lignage** montre `raw→staging→curated` + `raw→archive`. Captures pour le rapport.

## Task 2.4 : Documentation du catalogue

**Files:** Modifier `README.md`, `rapport/rapport.md` · `docs/` (captures)

- [ ] **Step 1 — README** : ajouter une section « Catalogue OpenMetadata » (connexion MinIO, 5 fiches, pipelines + lignage) et mettre la ligne **C20** du tableau des livrables à ✅/◐ selon l'avancement.
- [ ] **Step 2 — rapport** : entrée C20 (volet catalogue) — fiches, colonnes documentées, pipelines + lignage `raw→staging→curated→archive`, méthode hybride (scripts + captures), et rappel de la non-invasivité.
- [ ] **Step 3 — Ranger les captures** dans `docs/` (ex. `docs/captures-openmetadata/`) et les référencer depuis le rapport.
- [ ] **Step 4 — Vérifs** : `grep -rnE "\b(tu|ton|ta|tes)\b" README.md rapport/rapport.md || echo "OK vouvoiement"`.

---

## Auto-revue (fin de plan)

- **Couverture spec :** cycle de vie — `copy_object` (T1.1), sélection (T1.2), déplacement+purge+main (T1.3), DAG (T1.4), ILM (T1.5), docs (T1.6) ; catalogue — connexion/ingestion (T2.1), fiches+colonnes (T2.2), pipelines+lignage (T2.3), docs/captures (T2.4). ✅
- **Non-invasivité :** aucune tâche ne modifie la logique des DAGs ; seule T2.3 ajoute des `inlets`/`outlets` (métadonnées), validés DagBag. ✅
- **AUCUN commit** dans les étapes ; **Polars** (aucun pandas introduit) ; typage `ANN`.
- **Cohérence des noms :** `mois_a_archiver`, `archive_month`, `main`, `ARCHIVE_BUCKET`, `_mois_index`, `_raw_months` — constants entre tâches ; réutilise `partition_prefix`/`curated_partition_prefix`/`RAW_BUCKET`/`STAGING_BUCKET`/`CURATED_BUCKET` de `ingestion.py`.
- **Ordre :** Phase 1 (déterministe) avant Phase 2 (le lignage T2.3 référence le DAG `archivage` créé en T1.4).
