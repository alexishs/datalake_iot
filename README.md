# Data Lake IoT industriel

Data lake pour la centralisation, la documentation et la gouvernance de données de capteurs issues de 5 lignes de production, en vue d'un futur projet de maintenance prédictive. Projet pédagogique — le brief complet est dans [ennonce.md](ennonce.md), les règles de travail dans [CLAUDE.md](CLAUDE.md).

## Livrables & progression (C18–C21)

Index des livrables par compétence / jour. ✅ produit · ◐ en cours · ⏳ à venir.

| Étape | Livrable | Fichier(s) | Statut |
|---|---|---|---|
| **C18 · J1** — Architecture | Exploration des 5 lignes (volumétrie, schémas, hétérogénéités) | [notebooks/exploration_jour1.ipynb](notebooks/exploration_jour1.ipynb) | ✅ |
| **C18 · J1** | Dossier d'architecture + schéma annoté (Mermaid) | [docs/architecture.md](docs/architecture.md) | ✅ |
| **C18 · J1** | Export PDF du schéma (rendu formel) | `docs/architecture.pdf` | ⏳ |
| **C19 · J2** — Intégration | Stack Docker (MinIO + buckets, Airflow, OpenMetadata) | [compose.yaml](compose.yaml), [init-scripts/](init-scripts/) | ✅ |
| **C19 · J2** | Téléchargement des sources (Zenodo + MD5) | [datalake/download.py](datalake/download.py) | ✅ |
| **C19 · J2** | Upload vers `raw/` + vérification MD5 | [datalake/ingestion.py](datalake/ingestion.py) | ✅ |
| **C19 · J3-4** | 3 DAGs : ingestion → harmonisation → consolidation | [dags/](dags/) *(à créer)* | ⏳ |
| **C19 · J3-4** | Procédure d'intégration | ce README + [docs/architecture.md](docs/architecture.md) | ◐ |
| **C20 · J5** — Catalogue | 5 fiches OpenMetadata + politique ILM | `docs/` + captures *(à créer)* | ⏳ |
| **C21 · J6-7** — Gouvernance | Matrice des droits + politique de gouvernance | `docs/` *(à créer)* | ⏳ |
| **J8** — Restitution | Rapport ≥ 5 pages (alimenté à la fin de chaque jour) | [rapport/rapport.md](rapport/rapport.md) | ◐ |

Le **package métier** [`datalake/`](datalake/) (réutilisé par le conteneur dev *et* les DAGs) : [`config.py`](datalake/config.py) (configuration via env), [`storage.py`](datalake/storage.py) (client MinIO + MD5), [`download.py`](datalake/download.py) (sources Zenodo), [`explore.py`](datalake/explore.py) (analyse du Jour 1), [`runner.py`](datalake/runner.py) (boucle/rapport partagés), [`ingestion.py`](datalake/ingestion.py) (data → raw).

## Architecture en couches

```
raw/      ← données brutes (partitionnement year=/month=/line=/)
staging/  ← données nettoyées, schéma harmonisé
curated/  ← données prêtes à l'analyse
archive/  ← données expirées (règle ILM MinIO)
```

> Modèle détaillé, justifications (volumétrie/fréquence), partitionnement (raw au **mois**, staging/curated au **jour**), schéma annoté et contrat d'implémentation : **[docs/architecture.md](docs/architecture.md)**.

## La stack

Tout est conteneurisé via Docker Compose. Trois briques fonctionnelles + une base de données mutualisée + un conteneur de développement.

| Service | Rôle | Accès hôte |
|---|---|---|
| **minio** | Stockage objet compatible S3 — le cœur du data lake | API `localhost:9000`, console `localhost:9001` |
| **minio-init** | Job jetable : crée les buckets `raw`/`staging`/`curated`/`archive` **+ les comptes de service et leurs policies** (droits par bucket) | — |
| **postgres** | Base **mutualisée** : 3 bases isolées (`airflow`, `om_airflow`, `openmetadata`) | `localhost:5432` |
| **airflow-webserver / -scheduler / -init** | **Airflow métier** : orchestre les DAGs d'ingestion et d'harmonisation | UI `localhost:8080` |
| **elasticsearch** | Index de recherche requis par OpenMetadata | `localhost:9200` |
| **execute-migrate-all** | Job jetable : crée/met à jour le schéma OpenMetadata *avant* le serveur | — |
| **openmetadata-server** | Catalogue de métadonnées / gouvernance | UI `localhost:8585` |
| **openmetadata-ingestion** | Airflow interne d'OpenMetadata (séparé de l'Airflow métier) | UI `localhost:8082` |
| **dev** | Conteneur de développement où VSCode s'attache pour tester/déboguer | — (réseau Docker) |

> Les services `airflow-init`, `minio-init` et `execute-migrate-all` sont des **jobs one-shot** : ils s'exécutent une fois puis s'arrêtent. Les voir en `Exited (0)` dans `docker compose ps -a` est **normal**, pas une erreur.

### Choix d'architecture notables

- **Postgres mutualisé** : un seul conteneur héberge 3 bases avec 3 utilisateurs distincts (isolation préservée). Gain de ressources sans couplage fonctionnel.
- **2 Airflow séparés** : l'Airflow métier (DAGs d'ingestion/harmonisation) et celui d'OpenMetadata (ingestion du catalogue) restent indépendants — responsabilités claires, moins de pièges de configuration qu'une mutualisation.
- **Conteneur `dev`** : permet de déboguer le code Python *dans le réseau Docker*, donc avec les mêmes noms d'hôte que la production (voir plus bas).

```
                  ┌──────────────────────┐
                  │ postgres (mutualisé) │
                  │   ├── airflow        │
                  │   ├── om_airflow     │
                  │   └── openmetadata   │
                  └──────────────────────┘
   ┌─────────┐  ┌────────────────┐  ┌─────────────────────────┐
   │ MinIO   │  │ Airflow métier │  │ OpenMetadata            │
   │ 9000/01 │  │ 8080           │  │ server 8585             │
   │         │  │                │  │ ingestion(Airflow) 8082 │
   │         │  │                │  │ elasticsearch 9200      │
   └─────────┘  └────────────────┘  └─────────────────────────┘
        │                │                       │
        └────────────────┼───────────────────────┘
                         │
                      ┌─────┐
                      │ dev │   ← VSCode s'y attache (debug)
                      └─────┘
```

> Tous les services partagent le réseau Docker `datalake`. Le conteneur `dev` y est rattaché : le code s'y exécute avec les mêmes noms d'hôte que dans Airflow.

## Prérequis

- Docker + Docker Compose
- ~6–8 Go de RAM disponibles (Elasticsearch + 2 Airflow + OpenMetadata)
- Python 3.12 (pour l'outillage local : édition, lint)

## Démarrage

### 1. Configurer l'environnement

```bash
cp .env.example .env
# Éditer .env et remplacer toutes les valeurs "change-me-*"

# Aligner les UID/GID du conteneur sur votre machine (évite les fichiers root/50000
# dans les bind mounts). À relancer si vous changez de machine :
sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/; \
        s/^HOST_UID=.*/HOST_UID=$(id -u)/; \
        s/^HOST_GID=.*/HOST_GID=$(id -g)/" .env
```

### 2. Lancer la stack

```bash
docker compose up -d
docker compose ps
```

> ⚠️ OpenMetadata met **plusieurs minutes** à être prêt au premier lancement (migration de schéma + indexation Elasticsearch). C'est normal que `openmetadata-server` reste `starting` un moment : `docker compose logs -f openmetadata-server`.

### 3. Points d'accès

| Interface | URL | Identifiants |
|---|---|---|
| Console MinIO | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Airflow métier | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| OpenMetadata | http://localhost:8585 | `admin@open-metadata.org` / `admin` |

### Comptes de service MinIO & droits par bucket

Le job `minio-init` ([init-scripts/minio/](init-scripts/minio/)) crée, en plus des buckets, 3 comptes aux droits différenciés (policies IAM **personnalisées**, restreintes par bucket — les policies intégrées de MinIO porteraient sur *tous* les buckets). Mots de passe via `.env`.

| Compte | `raw/` | `staging/` | `curated/` | `archive/` |
|---|---|---|---|---|
| `data-analyst` | — | — | **lecture** | — |
| `data-engineer` | lecture/écriture | lecture/écriture | lecture/écriture | — |
| `datalake-admin` | tous droits | tous droits | tous droits | tous droits |

> Étape C19 (« policies d'accès initiales selon bucket »). Le chiffrement SSE-S3, les logs d'audit et la matrice de gouvernance écrite relèvent du C21. `archive/` n'est attribué à aucun rôle (géré par l'ILM).

## Exécution du code Python

> **Principe directeur.** Toute la logique métier vit dans le package [`datalake/`](datalake/). Le code est **strictement identique** qu'il soit exécuté par Airflow ou débogué dans le conteneur `dev` : les deux tournent **dans le réseau Docker**, donc le nom d'hôte `minio` (et `postgres`, etc.) résout de la même façon. **On ne lance pas ce code sur l'hôte nu** — sinon `minio:9000` ne résout pas.

Le package lit sa configuration depuis l'environnement (endpoint MinIO par défaut `http://minio:9000`, secrets via variables). Voir [datalake/config.py](datalake/config.py).

### A. Débogage dans le conteneur `dev` (VSCode)

C'est le mode recommandé pour tester/déboguer.

1. Ouvrir le projet dans VSCode.
2. **Dev Containers : Reopen in Container** (extension *Dev Containers* requise). VSCode build l'image `dev`, démarre `minio`, et installe automatiquement les extensions de debug (Python, debugpy, Ruff) via [.devcontainer/devcontainer.json](.devcontainer/devcontainer.json).
3. Poser un point d'arrêt, puis lancer une config de [.vscode/launch.json](.vscode/launch.json) (ex. *« module datalake (smoke test MinIO) »*).

En ligne de commande, dans le conteneur :

```bash
docker compose up -d dev
docker compose exec dev python -m datalake     # smoke test : liste les buckets
```

### B. Exécution par Airflow

Les DAGs ([dags/](dags/)) sont des **coquilles fines** : ils importent le package `datalake` et appellent ses fonctions depuis des `PythonOperator`. Aucune logique métier dans les DAGs.

```python
# Exemple de squelette de DAG
from datalake.storage import get_s3_client
# ... PythonOperator(python_callable=ma_fonction_metier)
```

Le package est rendu importable côté Airflow via `PYTHONPATH=/opt/airflow` et le montage `./datalake:/opt/airflow/datalake` (voir [compose.yaml](compose.yaml)).

### Pourquoi pas d'exécution sur l'hôte ?

`minio`, `postgres`… sont des noms de services Docker : ils ne résolvent **que dans le réseau Docker**. Exécuter le code sur la machine hôte échouerait (nom d'hôte introuvable). Le conteneur `dev` existe précisément pour garantir un environnement d'exécution identique à celui d'Airflow.

## Dépendances Python (`requirements.txt`)

> Cette section documente le **rôle de chaque dépendance**. Le fichier [requirements.txt](requirements.txt) peut être régénéré (`pip freeze`), ce qui **écraserait ses commentaires** : la référence ci-dessous les préserve.

Périmètre : dépendances **locales** (conteneur `dev` / outillage). Airflow, OpenMetadata et leurs runtimes Python vivent dans leurs propres images Docker — ils ne figurent **pas** ici.

| Dépendance | Contrainte | Rôle |
|---|---|---|
| `boto3` | `~=1.35` | Client S3 pour MinIO (upload, list, get) |
| `botocore` | `~=1.35` | Dépendance de boto3, épinglée pour cohérence de version |
| `polars` | `~=1.41` | Lecture/analyse des CSV + Parquet — Arrow-natif, rapide, `null` natif (Jour 1, C18) |
| `python-dotenv` | `~=1.0` | Charge `.env` dans les scripts locaux |
| `ruff` | `~=0.8` | Linter + formateur (qualité de code, dev) |

Installation locale (hors Docker, pour l'outillage VSCode hôte) :

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

> `debugpy` n'est **pas** dans `requirements.txt` : il est installé directement dans l'image du conteneur de dev (voir [Dockerfile.dev](Dockerfile.dev)).

### Optionnel — développer les DAGs avec l'autocomplétion Airflow

Ne **pas** ajouter `apache-airflow` au `requirements.txt` tel quel : Airflow exige son *constraints file* sous peine de casser ses dépendances. L'installer ainsi :

```bash
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION=3.12
CONSTRAINT="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT}"
```

> ⚠️ Ne **pas** installer le paquet `openmetadata-ingestion` dans ce venv 3.12 : il tourne déjà dans le conteneur `openmetadata-ingestion`.

## Structure du dépôt

```
compose.yaml         stack Docker (MinIO, Airflow, OpenMetadata, dev)
Dockerfile.dev       image du conteneur de dev
.devcontainer/       config Dev Containers (extensions debug auto)
.vscode/             launch.json (debug)
.env.example         variables (copier en .env)
init-scripts/        init Postgres mutualisé (3 bases isolées)
datalake/            PACKAGE métier (boto3, ingestion, harmonisation)
dags/                DAGs Airflow (appellent datalake)
notebooks/           exploration des données (Jour 1, C18)
data/                CSV bruts (non versionnés)
docs/                architecture.md (C18), gouvernance/ILM (C20/C21), captures
rapport/             rapport final (≥ 5 pages)
```

## Notes & dépannage

- **Conteneurs `Exited (0)`** (`airflow-init`, `minio-init`, `execute-migrate-all`) : normal, ce sont des jobs one-shot. Seuls les services « longs » restent `Up`.
- **Modifier un mot de passe DB dans `.env` après le 1er démarrage** : le script d'init Postgres (création des 3 bases) ne s'exécute **qu'une fois**, à la création du volume `postgres-data`. Si vous changez un identifiant ensuite, vous aurez des erreurs d'authentification. Réinitialisez avec `docker compose down -v` puis `up -d`.
- **`airflow-webserver` qui redémarre en boucle** : vérifiez que `AIRFLOW_UID` vaut bien `id -u` dans `.env` (un UID absent du conteneur casse l'initialisation Airflow).
- **OpenMetadata long à démarrer** : plusieurs minutes au 1er lancement (migration + indexation). Suivre `docker compose logs -f openmetadata-server` jusqu'à `healthy`.
- **Elasticsearch qui s'arrête au boot** (`max virtual memory areas vm.max_map_count too low`) : `sudo sysctl -w vm.max_map_count=262144` (persister dans `/etc/sysctl.conf`).
- **Dossiers montés possédés par `root`** : `dags/` et `datalake/` sont versionnés (avec `.gitkeep`) pour exister avant le `up` ; sinon Docker les recrée en `root`.

## Arrêt

```bash
docker compose down            # arrête et supprime les conteneurs
docker compose down -v         # + supprime les volumes (RESET complet des données)
```
