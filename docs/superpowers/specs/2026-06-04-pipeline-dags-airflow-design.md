# Design — DAGs Airflow du pipeline (raw → staging → curated) — C19, Jours 3-4

> Spec issue d'une session de brainstorming. Objectif : orchestrer le pipeline du data lake avec **3 DAGs Airflow** qui ne sont que des **coquilles fines** appelant le package `datalake`. La conception *data* (couches, partitions, schéma cible, filigrane, cascade) est figée dans [docs/architecture.md](../../architecture.md) — on ne la rejoue pas ; ce spec décrit la **couche d'orchestration** et les **modules métier** qui restent à écrire (`harmonization`, `consolidation`).

## 1. Contexte & objectif

`raw/` est alimenté (DAG 1 réutilise l'`ingest_file` déjà écrit et testé). Restent l'**harmonisation** (`raw → staging`) et la **consolidation** (`staging → curated`), plus les 3 DAGs qui les déclenchent. Contraintes directrices :

- **Coquille fine** : zéro logique métier dans les DAGs (règle CLAUDE.md). Toute la logique vit dans `datalake/` et est **testée en pur Python** (faux client S3 `FakeS3` + Polars), comme l'existant.
- **Lançable hors Airflow** : chaque module expose un `main()` (`python -m datalake.harmonization`, `python -m datalake.consolidation`) pour une exécution manuelle dans le conteneur `dev`. Airflow ne fait qu'**ordonnancer** les mêmes appels.
- **Manuel == DAG** : la fonction métier appelée est identique des deux côtés.

## 2. Périmètre

**Dans le périmètre :** un helper `runner.checked`, le module `harmonization` (+ DAG 2), le module `consolidation` (+ DAG 3), le DAG 1 (réutilise `ingest_file`), le **filigrane auto-réparant**, l'extension de la **cascade d'invalidation** à `curated` (dans `ingest_file`), le montage de `data/` dans Airflow, et la **validation d'intégrité DagBag**.

**Hors périmètre :** la conception *data* elle-même (déjà figée) ; C20 (catalogue OpenMetadata, ILM) ; C21 (SSE-S3, audit, doc de gouvernance) ; toute optimisation de performance.

## 3. Patron commun aux 3 DAGs (approche retenue)

Chaque module métier expose :

- une fonction **« pas »** `*_step(client) -> Result` (resp. `ingest_file` par fichier) : **un incrément de travail** idempotent ;
- un **`main() -> int`** qui **draine** (boucle de pas via le `runner`) — exécution manuelle ;
- le package fournit **`runner.checked(result: Result) -> str`** : renvoie un libellé si `result.ok`, **lève `RuntimeError`** sinon (sémantique Airflow : tâche rouge sur échec). Testé en pur Python.

Le DAG est une coquille : une (ou des) tâche(s) appelant `checked(step(...))`. Granularité :

| DAG | Unité de travail | Tâches par run | Déclenchement |
|---|---|---|---|
| 1 `ingestion_raw` | un CSV de `data/` | une par CSV (dynamic task mapping) | `schedule=None` (manuel) |
| 2 `harmonisation_staging` | **une journée globale** (toutes lignes) | une (un pas) | `schedule="* * * * *"` |
| 3 `consolidation_curated` | **une journée globale** (toutes lignes) | une (un pas) | `schedule="* * * * *"` |

Pour les DAG 2/3, **un pas = une journée** : le fil-de-l'eau de l'architecture (une journée ingérée ~chaque minute) est réalisé en traitant **une seule journée par exécution**, choisie par le filigrane (§4).

## 4. Filigrane auto-réparant (DAG 2 et 3)

Le jour à traiter est **le plus ancien `(ligne, jour)` présent en amont mais absent en aval** :

- DAG 2 : amont = `raw`, aval = `staging`.
- DAG 3 : amont = `staging`, aval = `curated`.

```
jour_a_traiter = min { d : ∃ ligne, d ∈ jours_amont[ligne] ∧ d ∉ jours_aval[ligne] }
```

- Les **jours aval** se lisent dans les **chemins** (`…/day=DD/`) — `list_keys` suffit, pas de lecture de contenu.
- Les **jours amont `raw`** ne sont **pas** dans les chemins (partition au mois) : il faut **lire le CSV du mois** et extraire les jours distincts du `timestamp`.
- Les **jours amont `staging`** sont dans les chemins.

**Auto-réparation :** comme la cascade (§5) *supprime* la partition aval lors d'un réimport, « périmé » se réduit à « absent ». Un trou redevient donc automatiquement le prochain `jour_a_traiter`. En marche normale, on traite le plus ancien d'abord (janvier→mai, les lignes couvrant des mois disjoints).

> **Conséquence assumée :** les 5 lignes couvrant des mois disjoints, un « jour global » n'appartient en pratique qu'à **une** ligne ; le pipeline remplit donc `staging`/`curated` ligne par ligne, dans l'ordre chronologique. Le code reste **générique** (traite toutes les lignes ayant ce jour) et n'exploite pas la coïncidence de mois (cf. architecture §6, clé naturelle).

## 5. Cascade d'invalidation `raw → staging → curated`

**Décision : invalidation à la granularité `(ligne, mois)` dans `ingest_file`** (Option 1 du brainstorming). Au (ré)import d'une `(ligne, mois)` dans `raw` (MD5 différent ou absent), après confirmation de l'écriture `raw` (ordre sûr), `ingest_file` :

1. nettoie la partition `raw` du mois (objets périmés) — *existant* ;
2. `delete_prefix(staging, "production_lines/{line}/year={Y}/month={M}/")` — *existant* ;
3. **`delete_prefix(curated, "production_lines/line={line}/year={Y}/month={M}/")`** — *nouveau* (curated fusionne les lignes : `line=` en partition Hive).

Ainsi les deux couches dérivées redeviennent « absentes » pour cette `(ligne, mois)`, et les filigranes de DAG 2 **puis** DAG 3 les recalculent. Robuste même si le réimport change le nombre de jours (purge au niveau mois → pas de jour orphelin).

> `ingest_file` connaît donc les conventions de partition de `staging` **et** `curated` : couplage **assumé et documenté**, dans la lignée de l'hypothèse déjà posée en architecture §9. **`architecture.md` doit être mis à jour** (§3.3, §11, §12) pour étendre la cascade à `curated`.

## 6. Modules métier (interfaces)

### 6.1 `datalake/harmonization.py`

Constantes `RAW_BUCKET="raw"`, `STAGING_BUCKET="staging"`. Fonctions (toutes testables en pur Python via `FakeS3` + Polars) :

- `staging_prefix(line, year, month, day) -> str` → `production_lines/{line}/year={Y}/month={M:02d}/day={D:02d}/`.
- `raw_days(client) -> dict[str, set[date]]` : par ligne, jours présents dans `raw` (lit le CSV du mois).
- `staging_days(client) -> dict[str, set[date]]` : par ligne, jours présents dans `staging` (depuis les chemins).
- `jour_a_traiter(client) -> date | None` : filigrane (§4).
- `harmonize_day(client, jour) -> list[Result]` : pour **chaque ligne** ayant `jour` en `raw`-mais-pas-`staging` → lit l'objet `raw` du mois, **filtre le jour**, applique les **règles §12** (casse en minuscules ; `timestamp` parsé `%Y-%m-%d %H:%M:%S` → **ISO 8601 UTC** ; `elapsed_time` **nullable** → `null` si absent ; types `float`/`int` ; **schéma cible §6** figé ; colonnes `line/year/month/day`), **dédup `(line, timestamp)`**, écrit le **Parquet** dans la partition jour (`delete_prefix` du jour **puis** write → idempotent).
- `harmonize_step(client=None) -> Result` : crée le client si besoin (comme `ingest_file`), puis `jour_a_traiter` → `harmonize_day` (agrège), sinon `Result("staging", "à jour", True)`.
- `main() -> int` : draine (`harmonize_step` jusqu'à « à jour »).

### 6.2 `datalake/consolidation.py`

Constantes `STAGING_BUCKET`, `CURATED_BUCKET="curated"`. Même patron, `staging → curated` :

- `curated_day_prefix(line, year, month, day) -> str` → `production_lines/line={line}/year={Y}/month={M:02d}/day={D:02d}/` (table unifiée : `line=` en partition Hive).
- `staging_days` (jours `staging` par ligne), `curated_days` (jours `curated` par ligne), `jour_a_traiter` (filigrane `curated` vs `staging`).
- `consolidate_day(client, jour) -> list[Result]` : pour chaque ligne ayant `jour` en `staging`-mais-pas-`curated` → lit le Parquet `staging`, écrit le Parquet `curated` (la colonne `line` est déjà dans le schéma §6), `delete_prefix` du jour puis write (idempotent).
- `consolidate_step(client=None) -> Result`, `main() -> int`.

> **Partitionnement `curated`** : `production_lines/line={line}/year=/month=/day=/` — racine `production_lines` homogène avec staging, mais **`line=` en partition Hive** car curated **fusionne** les 5 lignes en table unifiée (≠ staging, segment simple `lineX`). Conforme à `architecture.md` §4. La fusion est **logique** (schéma unifié + colonne `line` + partition `line=`), **pas** un fichier unique par jour.

## 7. Les 3 DAGs (`dags/`)

Coquilles fines (TaskFlow API), important `datalake`, zéro logique :

- `dags/ingestion_raw.py` : `dag_id="ingestion_raw"`, `schedule=None` ; tâche `lister_csv` (→ `csv_paths`) puis tâche mappée `ingerer` = `checked(ingest_file(path))`.
- `dags/harmonisation_staging.py` : `dag_id="harmonisation_staging"`, `schedule="* * * * *"`, `catchup=False`, `max_active_runs=1` ; une tâche `harmoniser` = `checked(harmonize_step())`.
- `dags/consolidation_curated.py` : `dag_id="consolidation_curated"`, `schedule="* * * * *"`, `catchup=False`, `max_active_runs=1` ; une tâche `consolider` = `checked(consolidate_step())`.

`max_active_runs=1` évite que deux exécutions minute se chevauchent sur le même filigrane.

## 8. Tests

- **Pur Python (venv, `FakeS3` + Polars)** — le gros du travail :
  - `harmonization` : `jour_a_traiter` (marche normale, trou créé par cascade) ; `harmonize_day` (casse mixte → minuscules ; `elapsed_time` absent → `null` ; `timestamp` → ISO ; dédup `(line, timestamp)` ; idempotence = réécriture d'une partition jour) — vérifié en **relisant le Parquet** depuis les octets stockés par `FakeS3` (`pl.read_parquet(BytesIO(...))`).
  - `consolidation` : symétrique.
  - `ingest_file` : la cascade vide bien **staging ET curated** pour la `(ligne, mois)` (nouveau test) ; non-régression de l'existant.
  - `runner.checked` : lève sur `ok=False`, renvoie le libellé sinon.
- **Intégrité DagBag (conteneur Airflow)** — par phase : `airflow dags list-import-errors` doit être vide et les `dag_id` attendus présents. Ce n'est pas un test unitaire (les DAGs n'ont pas de logique) mais un **contrôle de déploiement** (imports, parsing, câblage).

## 9. Infrastructure (`compose.yaml`)

- Ajouter le montage **`./data:/opt/airflow/data:ro`** à `x-airflow-common` (DAG 1 lit `data/`). Les DAG 2/3 travaillent S3→S3 (aucun montage de données requis).
- Déjà en place : env MinIO (`x-minio-env`), montage `./datalake`, `PYTHONPATH=/opt/airflow`, `_PIP_ADDITIONAL_REQUIREMENTS: "boto3 polars"`.

## 10. Phasage du plan d'implémentation

1. **Phase 1 — socle d'orchestration + DAG 1.** `runner.checked` (TDD) ; extension cascade `curated` dans `ingest_file` (TDD) ; `dags/ingestion_raw.py` ; montage `data/` ; contrôle DagBag.
2. **Phase 2 — harmonisation.** Module `datalake/harmonization.py` (TDD : filigrane + `harmonize_day` + `main`) ; `dags/harmonisation_staging.py` ; contrôle DagBag.
3. **Phase 3 — consolidation.** Module `datalake/consolidation.py` (TDD) ; `dags/consolidation_curated.py` ; contrôle DagBag ; mise à jour `architecture.md` (cascade `curated`) + README/rapport.

Chaque phase est **livrable indépendamment** (testée, le pipeline fonctionne jusqu'à la couche atteinte).

## 11. Conformité

- **Énoncé Jours 3-4 (C19)** : DAG d'ingestion `raw/` (partition `year=/month=/line=/`) ✅ ; DAG d'harmonisation `staging/` ✅ ; LineA traitée par « chunks » (fil-de-l'eau, une journée/min) ✅ ; procédure d'intégration (README) ✅.
- **architecture.md** : partitions, schéma cible §6, contrat §12, filigrane §3.2/§10/§11. La cascade `curated` est une **extension à documenter** (§5).
- **CLAUDE.md** : logique dans `datalake/` (coquilles fines) ; **Polars** ; vouvoiement ; Markdown sans hard-wrap ; lançable hors Airflow.

## 12. Hypothèses

- L'`ETag`/MD5 et le `put_object` simple : inchangés (objets < quelques Mo).
- Écriture Parquet via Polars dans un `BytesIO` puis `put_object` (pas d'accès disque côté S3) — à valider en Phase 2.
- Le `client` S3 est créé une fois par exécution de tâche (les tâches Airflow sont des processus isolés) ; acceptable au vu du volume.
- `architecture.md` sera mis à jour pour acter la cascade `raw → staging → curated` (actuellement documentée jusqu'à `staging`).
