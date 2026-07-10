# Catalogue OpenMetadata — ingestion du data lake (C20)

Ce dossier contient l'**ingestion config-as-code** du data lake MinIO dans OpenMetadata : un service de stockage S3 qui découvre les conteneurs des 4 buckets (`raw`, `staging`, `curated`, `archive`) et documente le schéma des colonnes, ligne par ligne.

> **Config-as-code, pas de clic.** L'UI OpenMetadata génère exactement le même YAML sous le capot ; le décrire en fichier versionné le rend **reproductible** par un tiers (règle 2 du projet). L'UI reste utile pour la **consultation** et l'**enrichissement manuel** (captures du livrable).

## Contenu

| Fichier | Rôle |
|---|---|
| [`s3-storage-ingestion.yaml`](s3-storage-ingestion.yaml) | Spec d'ingestion du connecteur S3 → MinIO (secrets `${...}` substitués à l'exécution). |
| [`manifests/`](manifests/) | Un manifest `openmetadata.json` **par bucket** (schéma des conteneurs structurés). |
| [`run-ingestion.sh`](run-ingestion.sh) | Dépose les manifests, substitue les secrets, lance `metadata ingest`. |
| [`enrich.py`](enrich.py) + [`run-enrichment.sh`](run-enrichment.sh) | Enrichissement des 5 fiches `raw` via le SDK OpenMetadata (propriétaire, descriptions, unités des colonnes). |
| [`airflow-pipeline-ingestion.yaml`](airflow-pipeline-ingestion.yaml) + [`run-pipelines.sh`](run-pipelines.sh) | Ingestion des 4 DAGs Airflow (Pipelines) + lignage entre conteneurs. |

## Prérequis

1. Stack démarrée : `docker compose up -d` (dont `openmetadata-server`, `openmetadata-ingestion`, `minio`).
2. `.env` renseigné. En particulier **`OPENMETADATA_JWT_TOKEN`** : jeton du **bot d'ingestion**, à copier depuis l'UI OpenMetadata → **Settings → Bots → `ingestion-bot`** (jeton JWT). C'est l'authentification standard des ingestions externes OpenMetadata (le même mécanisme que celui employé par l'UI).
3. Les données présentes dans les buckets (au moins `raw`) — cf. [README racine](../../README.md) (ingestion `python -m datalake.ingestion` + DAGs).

## Lancer l'ingestion

```bash
./init-scripts/openmetadata/run-ingestion.sh
```

Le script :

1. **dépose les manifests** à la racine de chaque bucket sous le nom **`openmetadata.json`** — nom **imposé** par le connecteur S3 pour un manifest *local par bucket* (constante `OPENMETADATA_TEMPLATE_FILE_NAME`) ;
2. **substitue** `${MINIO_ROOT_USER}`, `${MINIO_ROOT_PASSWORD}`, `${OPENMETADATA_JWT_TOKEN}` depuis `.env` (aucun secret n'est écrit dans le dépôt ni affiché) ;
3. lance `metadata ingest -c …` dans le conteneur `dl-om-ingestion`.

Résultat attendu : `Success % : 100.0`, `Errors : 0`, **16 conteneurs structurés** traités.

## Comment fonctionne le manifest

Sans manifest, le connecteur S3 ne référence que les buckets (conteneurs « bruts », sans colonnes). Le manifest `openmetadata.json` déclare, pour chaque bucket, les **datasets structurés** :

```jsonc
{
  "entries": [
    {
      "dataPath": "production_lines/lineA",  // préfixe DANS le bucket (hors nom de bucket)
      "structureFormat": "csv",              // csv | parquet | …
      "isPartitioned": true,
      "partitionColumns": [                  // colonnes de partition ABSENTES du fichier
        { "name": "year", "dataType": "INT" },
        { "name": "month", "dataType": "INT" }
      ]
    }
  ]
}
```

Pour chaque entrée, OpenMetadata liste les objets sous `dataPath/`, **choisit un fichier échantillon** et **infère les colonnes** en le lisant. Les colonnes finales = `partitionColumns` (du manifest) **+** colonnes inférées du fichier.

**Choix par couche :**

- **`raw`** (CSV) : `year`/`month` ne vivent que dans le **chemin** (`year=YYYY/month=MM/`), pas dans le CSV → on les déclare en `partitionColumns`. Les colonnes métier (`Temperature`/`temperature`, `pressure`, `elapsed_time`…) sont inférées telles quelles : **l'hétérogénéité inter-lignes reste visible dans le catalogue** (point d'évaluation du brief).
- **`staging`/`curated`** (Parquet) : les fichiers contiennent **déjà** `line`/`year`/`month`/`day` comme colonnes → on **omet** `partitionColumns` (sinon doublons). On garde `isPartitioned: true` (le stockage l'est). Le schéma y est **harmonisé** (minuscules, `elapsed_time` présent).
- **`archive`** (CSV) : une entrée `lineE` (seule ligne archivée par la démo de cycle de vie).

## Enrichissement des fiches (propriétaire, descriptions, colonnes)

Après l'ingestion, enrichir les 5 fiches `raw` :

```bash
./init-scripts/openmetadata/run-enrichment.sh
```

Ce lanceur exécute [`enrich.py`](enrich.py) dans le conteneur d'ingestion (SDK Python OpenMetadata) et **comble tous les « no description »** :

- le **service** `datalake_minio` et les **4 buckets** (couches `raw`/`staging`/`curated`/`archive`) reçoivent une description ;
- un **propriétaire** — l'équipe « Responsable maintenance » (créée si absente) — sur les buckets et toutes les fiches ;
- une **description par fiche structurée** (16 conteneurs) : `raw` (source Zenodo record `15277168`, fréquence 1 relevé/minute, sémantique de `label`, rappel de l'hétérogénéité) ; `staging`/`curated`/`archive` (couche + DAG producteur) ;
- la **documentation des colonnes** — unités (`temperature` °C, `pressure` bar), `elapsed_time` (nullable), `label` (0 = nominal / 1 = anomalie), `timestamp` (ISO 8601), partitions `line`/`year`/`month`/`day` ;
- la **contrainte de nullité** (`constraint`) : `elapsed_time` → `NULL` (nullable, structurellement absent des lignes C/D/E → 100 % null après harmonisation) ; `timestamp`/`line`/`year`/`month`/`day` → `NOT_NULL` (garantis par construction) ; `temperature`/`pressure`/`label` laissés sans contrainte (non-nulls *observés* mais dépendant de la source).

> **Nullité visible à l'écran.** L'UI OpenMetadata n'affiche **pas** de colonne *Constraint* pour les *Containers* (rendu réservé aux *Table*). La nullité est donc **aussi** portée dans la **description** de chaque colonne (`*(nullable)*` / `*(non null)*`), seule colonne visible dans l'onglet *Schema* d'un conteneur ; le champ `constraint` reste renseigné pour les consommateurs par API.

> Le SDK expose `patch_column_description` uniquement pour les `Table` ; pour les colonnes d'un `Container`, `enrich.py` utilise le `patch()` générique (diff JSON entre l'entité source et une copie modifiée).

## Pipelines Airflow + lignage

Une fois les conteneurs ingérés (les deux extrémités d'une arête doivent exister) :

```bash
./init-scripts/openmetadata/run-pipelines.sh
```

Le connecteur Airflow lit les DAGs **sérialisés** dans la base Postgres de l'Airflow métier (table `serialized_dag`), catalogue les **4 DAGs comme *Pipelines*** et déduit le **lignage** entre conteneurs.

Le lignage vient des annotations `inlets`/`outlets` des DAGs (cf. [`dags/_om_lineage.py`](../../dags/_om_lineage.py)) : chaque tâche déclare ses conteneurs source/cible au format `OMEntity` (`{"entity": "container", "fqn": …, "key": …}`). OpenMetadata **regroupe les inlets/outlets d'un DAG par `key`** — on utilise le nom de la ligne comme clé pour apparier `raw.lineX → staging.lineX` (et non un produit croisé). Résultat :

```
raw.lineX ──(harmonisation_staging)──▶ staging.lineX ──(consolidation_curated)──▶ curated.line=lineX
raw.lineE ──(archivage)──▶ archive.lineE
```

> **Non-invasif.** `inlets`/`outlets` sont des **métadonnées pures** : aucun effet à l'exécution des DAGs (validé par `airflow dags list-import-errors` → aucune erreur). Un `DeprecationWarning` sur le style *dict* apparaît : c'est la voie documentée et fonctionnelle pour un DAG **sérialisé** (on ne peut pas importer les classes OpenMetadata dans l'Airflow métier).

## Ré-exécution

Idempotent : relancer les scripts met à jour les conteneurs (`Updated records`) et les fiches (`force=True`). `markDeletedContainers: true` retire du catalogue les conteneurs qui auraient disparu des buckets.

## Ordre complet de reproduction

```bash
./init-scripts/openmetadata/run-ingestion.sh    # 1. conteneurs + colonnes
./init-scripts/openmetadata/run-enrichment.sh   # 2. propriétaire + descriptions
./init-scripts/openmetadata/run-pipelines.sh    # 3. pipelines + lignage
```

## Vérification (API)

```bash
set -a; source .env; set +a
curl -s -H "Authorization: Bearer $OPENMETADATA_JWT_TOKEN" \
  "http://localhost:8585/api/v1/containers?service=datalake_minio&limit=200&fields=dataModel" \
  | python3 -c "import sys,json;[print(c['fullyQualifiedName']) for c in json.load(sys.stdin)['data']]"
```

Ou dans l'UI : **Explore → Containers → service `datalake_minio`**.
