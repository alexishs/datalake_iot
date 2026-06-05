# Rapport professionnel — Data Lake IoT industriel (épreuve E7)

> **Document de travail**, organisé en **journal de réalisation par date réelle** (cf. CLAUDE.md, règle 6). La chronologie réelle diffère volontairement du découpage théorique « Jour 1…8 » de l'énoncé (voir §1, *démarche socle d'abord*) ; les compétences (C18–C21) sont indiquées en étiquette de chaque entrée. La version finale (≥ 5 pages) est consolidée et mise au propre au Jour 8, puis exportée en PDF. Détails techniques : [docs/architecture.md](../docs/architecture.md) ; exploration des données : [notebooks/exploration_jour1.ipynb](../notebooks/exploration_jour1.ipynb).

## 1. Contexte & objectifs

Mission confiée par la DSI d'un équipementier automobile : concevoir et déployer un **data lake** pour centraliser, documenter, sécuriser et gouverner les données de **5 lignes de production** instrumentées (température, pression, temps de fonctionnement), aujourd'hui stockées « en vrac ». Finalité : constituer la fondation d'un futur projet de **maintenance prédictive** (détection d'anomalies, anticipation des pannes).

Stack imposée : **MinIO** (stockage objet S3), **Apache Airflow** (orchestration), **OpenMetadata** (catalogue), **Python/boto3**, **Docker Compose**, **Git**.

**Démarche — le socle technique d'abord.** La **stack complète a été montée en premier** (le 2 juin), avant l'analyse et le développement. Disposer d'emblée d'un environnement **opérationnel et reproductible** (relançable par un tiers via Docker Compose) a permis de mener ensuite l'analyse et la modélisation sur une base saine. *(Cela ne contrevient pas au principe « analyser avant de coder » du C18 : l'analyse des données précède toujours les **décisions d'architecture** ; seul l'**environnement d'exécution** est monté au préalable.)* C'est cette démarche qui explique pourquoi ce rapport est organisé par **date réelle** plutôt que selon la numérotation des « jours » de l'énoncé.

## 2. Journal de réalisation

### 2 juin 2026 — Socle technique (transverse, prérequis)

**Activités.** Mise en place d'un unique `docker compose` réunissant **tous les conteneurs** — MinIO (+ création des buckets), Postgres mutualisé, l'Airflow métier, OpenMetadata (serveur, ingestion, Elasticsearch) et un **conteneur de développement** où le code s'exécute *dans* le réseau Docker (mêmes noms d'hôte qu'en production) — **en respectant strictement les technologies demandées** par le brief.

**Choix justifiés — maîtrise du nombre de conteneurs.** Plusieurs décisions limitent la prolifération :

- **Postgres mutualisé** : un **seul** conteneur héberge **3 bases isolées** (Airflow métier, Airflow interne d'OpenMetadata, catalogue OpenMetadata) — au lieu de 3 conteneurs. Utilisateurs et bases séparés pour préserver l'isolation (gain de ressources sans couplage fonctionnel).
- **Airflow en `LocalExecutor`** (et non `CeleryExecutor`) : **pas** de broker Redis, ni de workers Celery, ni de Flower → plusieurs conteneurs évités par instance Airflow.
- **Jobs *one-shot*** (création des buckets, init Airflow, migration OpenMetadata) : conteneurs **éphémères** qui s'exécutent puis s'arrêtent (`Exited 0`), sans alourdir durablement la stack.
- **Périmètre minimal** : uniquement les briques de la stack imposée — aucun service annexe (pas de pgAdmin, Flower, proxy…).

**Limite assumée à la mutualisation** : les **2 Airflow restent séparés** (métier vs ingestion d'OpenMetadata). Mutualiser Postgres est un gain net ; mutualiser Airflow aurait introduit un **couplage fragile** pour un bénéfice faible — donc **non retenu** (cf. [docs/architecture.md](../docs/architecture.md)).

### 3 juin 2026 — C18 : analyse des données & architecture (Jour 1 de l'énoncé)

**Activités réalisées.**

- **Téléchargement des sources** : récupération des 5 CSV depuis Zenodo via son **API REST**, avec **vérification d'intégrité MD5** et idempotence ([datalake/download.py](../datalake/download.py)).
- **Exploration** des 5 lignes (volumétrie, schémas, types, distribution du `label`, couverture temporelle) au moyen de fonctions réutilisables ([datalake/explore.py](../datalake/explore.py)) pilotées depuis un notebook.
- **Modélisation** de l'architecture en couches et **schéma technique annoté** ([docs/architecture.md](../docs/architecture.md)) ; **index des livrables** (README) et squelette de ce rapport.

**Analyse des données & hétérogénéités identifiées.** Constats clés (30 000 enregistrements, 1 relevé/minute, janvier→mai 2025, ~1 % d'anomalies) :

- **Casse des colonnes non uniforme** (et parfois incohérente au sein d'un fichier) : `Temperature`/`temperature`, `Pressure`/`pressure`, `Elapsed_time`/`elapsed_time`.
- **`elapsed_time` optionnel** : présent uniquement sur Line A et Line B.
- **`timestamp`** au format `YYYY-MM-DD HH:MM:SS` (non ISO-8601, sans fuseau), cadence régulière d'1 minute, **continu** (ni trou ni doublon).
- **`label`** binaire {0, 1} (0 = nominal, 1 = anomalie), **fortement déséquilibré** (~1 %).
- **Écart documentation ↔ données** : LineE annoncée « 0 % d'anomalies » mais en contient 0,5 % — *la donnée fait foi*, à tracer comme avertissement qualité.
- **Un fichier source = un seul mois** (vérifié) : déterminant pour le partitionnement.

**Modélisation de l'architecture & choix justifiés.** Architecture en **4 couches** (raw → staging → curated → archive). Principaux choix (détail dans [docs/architecture.md](../docs/architecture.md)) :

- **Partitionnement à deux granularités** : `raw` au **mois** (permet de déposer les fichiers *tels quels* + MD5) ; `staging`/`curated` au **jour**, avec un traitement **au fil de l'eau** (un jour à la fois) qui simule un flux temps réel.
- **Formats** : CSV en `raw` (fidélité à la source), **Parquet** en aval (typé, compressé, colonnaire — adapté à l'analytique et au ML).
- **Schéma cible unifié** harmonisant les hétérogénéités (minuscules, `timestamp` ISO-8601, `elapsed_time` en `NULL` si absent) ; **clé naturelle `(line, timestamp)`** garantissant l'idempotence.
- **3 DAGs** (ingestion → harmonisation → consolidation) ; le 3ᵉ (`staging → curated`) n'est pas exigé par l'énoncé mais **assumé** pour la cohérence du flux.
- **Schéma en Mermaid** (diagramme-as-code, versionnable, rendu sur GitHub) plutôt que draw.io.

**Notions abordées.** Architecture en couches d'un data lake ; partitionnement (style Hive) et son lien avec volumétrie/fréquence ; **idempotence** et clé naturelle ; **intégrité** par hash MD5 ; gestion des valeurs manquantes (**`null` natif Polars**, distinct de `NaN` tel que géré par Pandas) ; **déséquilibre de classes** (enjeu central de la détection d'anomalies) ; gouvernance des métadonnées (réconcilier doc et données) ; diagramme-as-code.

### 4 juin 2026 — C19 : ingestion brute + policies d'accès (Jour 2 de l'énoncé)

**Activités réalisées.**

- **Ingestion `data/` → `raw/`** : module réutilisable [datalake/ingestion.py](../datalake/ingestion.py) (appelé par le CLI `python -m datalake.ingestion` et, à terme, par le DAG d'ingestion) — dépôt byte-identique, partition au mois, **vérification MD5** (ETag), **idempotence** (skip si MD5 identique) et **cascade** d'invalidation de `staging`. Mécanique mutualisée via [datalake/runner.py](../datalake/runner.py). Vérifié sur MinIO réel (5 fichiers déposés, mois 01→05, idempotence confirmée). *(Buckets, upload boto3, MD5 = exigences Jour 2 ✅.)*
- **Policies d'accès par bucket** : le job `minio-init` crée 3 comptes de service aux droits différenciés au moyen de **policies IAM personnalisées** (restreintes à des ARN de buckets précis, là où les policies intégrées de MinIO porteraient sur *tous* les buckets) — `data-analyst` (lecture seule sur `curated/`), `data-engineer` (lecture/écriture sur `raw/`+`staging/`+`curated/`), `datalake-admin` (tous droits). Droits **vérifiés** (un compte ne peut agir hors de son périmètre). Script et JSON versionnés dans [init-scripts/minio/](../init-scripts/minio/). *(Réalise l'exigence Jour 2 « policies d'accès initiales selon bucket » ✅ et, par anticipation, la **gestion des comptes du C21**.)*
- **Qualité & outillage** : migration de pandas vers **Polars** (Arrow-natif, `null` distinct de `NaN`) ; config **ruff + pytest unifiée** dans `pyproject.toml` (règles `E,W,F,I,UP,B,ANN`) ; **typage strict** des paramètres (client boto3 = `botocore.client.BaseClient`) ; développement en **TDD** avec un faux client S3 en pur Python.

**Notions abordées.** Modèle **IAM/policies S3** (actions, ressources/ARN, distinction bucket vs objets) et **RBAC** (utilisateur → policy → buckets) ; `ETag = MD5` d'un upload simple ; **idempotence** d'un pipeline et **cascade** d'invalidation ; **TDD** et injection de dépendance (client S3 factice) ; typage statique et linting.

### 5 juin 2026 — C19 : DAGs du pipeline (Jours 3-4)

**Activités réalisées.**

- **3 DAGs Airflow** en **coquilles fines** réutilisant le package : [ingestion_raw](../dags/) (data → raw, déclenché manuellement), [harmonisation_staging](../dags/) et [consolidation_curated](../dags/) (toutes les minutes, **une journée par exécution**) — zéro logique métier dans les DAGs, qui se contentent d'appeler les fonctions du package depuis des `PythonOperator`.
- **Nouveaux modules métier** : [datalake/harmonization.py](../datalake/harmonization.py) (raw → staging : normalisation de la **casse** des colonnes, `timestamp` **ISO 8601**, `elapsed_time` **nullable**, écriture **Parquet**, **partition au jour**, dédoublonnage) et [datalake/consolidation.py](../datalake/consolidation.py) (staging → curated : **table unifiée** avec colonne `line=`).
- **Fil-de-l'eau** : le **filigrane** est **auto-réparant** — il désigne le plus ancien `(ligne, jour)` présent en amont mais absent en aval ; un trou (créé par la cascade) redevient simplement le prochain jour traité, sans intervention.
- **Cascade `raw → staging → curated`** : un réimport `raw` d'une `(ligne, mois)` vide `staging` **ET** `curated` pour cette période → recalcul automatique des deux couches aval via leurs filigranes.
- **Tests** : développement en **TDD pur Python** (faux client S3 + Polars, relecture des Parquet écrits pour vérifier le contenu) ; les DAGs (coquilles) sont validés par un contrôle d'intégrité **DagBag** exécuté dans le conteneur Airflow.
- **Vérification de bout en bout** sur MinIO réel : **23 partitions** `staging` puis **23** `curated` produites ; auto-réparation confirmée via `airflow dags test` (suppression d'une partition → recréée à l'exécution suivante).
- **Exploration des données livrées (SQL, DuckDB)** : interrogation du **contenu** de `curated` en SQL via **DuckDB** — en ligne de commande **et** via l'interface **DBeaver** (moteur embarqué, lecture seule) — directement sur les Parquet de MinIO (extension `httpfs`/S3, **secret persistant**, **vues** visibles dans l'explorateur). Effectuée avec le compte **`data-analyst`** (lecture seule `curated/`), ce qui **démontre concrètement la gouvernance par bucket** (les autres couches restent inaccessibles). Outil d'analyse **hors stack déployée**, documenté dans le [README](../README.md) (section « Explorer les données en SQL »).

**Notions abordées.** Orchestration **Airflow** (DAG, `schedule`, `PythonOperator`, déclenchement manuel vs planifié) et **coquille fine** (séparation logique métier / orchestration) ; **filigrane** (*watermark*) comme état dérivé des données plutôt que de la date d'exécution ; **idempotence par partition** et pipeline **auto-réparant** par cascade ; harmonisation de schémas hétérogènes ; **contrôle d'intégrité DagBag** ; interrogation SQL d'un lac via **DuckDB** (lecture directe de Parquet sur S3, *predicate pushdown* et **élagage par statistiques** de partition) ; distinction **data lake vs lakehouse** (formats de table **Iceberg/Delta/Hudi**, ACID et mutation **ligne-à-ligne** vs **réécriture de partition** sur Parquet immuable).

## 3. Auto-évaluation par compétence

- **C18 — Architecture & analyse** : *acquis*. Les 5 lignes ont été analysées **avant** toute décision technique (volumétrie, schémas, hétérogénéités) ; l'architecture en couches est justifiée au regard de la volumétrie et de la fréquence ; le schéma annoté est lisible et exploitable par un tiers.
- **C19 — Intégration** : *acquis*. Stack reproductible via Docker Compose ; ingestion vers `raw/` avec **vérification MD5** et **idempotence** ; **3 DAGs** (ingestion → harmonisation → consolidation) en fil-de-l'eau ; LineA traitée par jour (chunks) ; procédure d'intégration documentée dans le README.
- **C20 — Catalogue & cycle de vie** : *à venir* (Jour 5) — fiches OpenMetadata et politique ILM.
- **C21 — Sécurité & gouvernance** : *partiellement anticipé*. Les **3 comptes de service** aux droits différenciés par bucket sont déjà en place (réalisés dès le C19) ; restent le chiffrement **SSE-S3**, les **logs d'audit** et la rédaction de la **politique de gouvernance** écrite.
- **Recul sur les choix** : plusieurs décisions vont **au-delà de l'énoncé** tout en restant justifiées et documentées — 3ᵉ DAG de consolidation, cadence minute, **filigrane auto-réparant**, exploration des données en SQL (**DuckDB**) — sans complexifier inutilement le projet.

## 4. Annexes

- Schéma d'architecture annoté : [docs/architecture.md](../docs/architecture.md) (§2).
- Exploration des données : [notebooks/exploration_jour1.ipynb](../notebooks/exploration_jour1.ipynb).
- Dépôt Git organisé : voir [README.md](../README.md).
