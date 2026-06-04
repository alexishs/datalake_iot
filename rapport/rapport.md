# Rapport professionnel — Data Lake IoT industriel (épreuve E7)

> **Document de travail**, alimenté à la fin de chaque « jour » de l'énoncé (cf. CLAUDE.md, règle 6). La version finale (≥ 5 pages) est consolidée et mise au propre au Jour 8, puis exportée en PDF. Détails techniques : [docs/architecture.md](../docs/architecture.md) ; exploration des données : [notebooks/exploration_jour1.ipynb](../notebooks/exploration_jour1.ipynb).

## 1. Contexte & objectifs

Mission confiée par la DSI d'un équipementier automobile : concevoir et déployer un **data lake** pour centraliser, documenter, sécuriser et gouverner les données de **5 lignes de production** instrumentées (température, pression, temps de fonctionnement), aujourd'hui stockées « en vrac ». Finalité : constituer la fondation d'un futur projet de **maintenance prédictive** (détection d'anomalies, anticipation des pannes).

Stack imposée : **MinIO** (stockage objet S3), **Apache Airflow** (orchestration), **OpenMetadata** (catalogue), **Python/boto3**, **Docker Compose**, **Git**.

**Démarche — le socle technique d'abord.** La **stack complète a été réalisée en premier** : un unique `docker compose` réunissant **tous les conteneurs** — MinIO (+ création des buckets), Postgres mutualisé, l'Airflow métier, OpenMetadata (serveur, ingestion, Elasticsearch) et un conteneur de développement — **en respectant strictement les technologies demandées** par le brief. Disposer d'emblée d'un environnement **opérationnel et reproductible** (relançable par un tiers via Docker Compose) a permis de mener ensuite l'analyse et la modélisation sur une base saine. *(Cela ne contrevient pas au principe « analyser avant de coder » du C18 : l'analyse des données précède toujours les **décisions d'architecture** ; seul l'**environnement d'exécution** est monté au préalable.)*

**Maîtrise du nombre de conteneurs.** Plusieurs choix limitent la prolifération des conteneurs :

- **Postgres mutualisé** : un **seul** conteneur Postgres héberge **3 bases isolées** (Airflow métier, Airflow interne d'OpenMetadata, catalogue OpenMetadata) — au lieu de **3 conteneurs** distincts. Utilisateurs et bases séparés pour préserver l'isolation (gain de ressources sans couplage fonctionnel).
- **Airflow en `LocalExecutor`** (et non `CeleryExecutor`) : **pas** de broker Redis, ni de workers Celery, ni de Flower → plusieurs conteneurs évités par instance Airflow.
- **Jobs *one-shot*** (création des buckets, init Airflow, migration OpenMetadata) : conteneurs **éphémères** qui s'exécutent puis s'arrêtent (`Exited 0`), sans alourdir durablement la stack.
- **Périmètre minimal** : uniquement les briques de la stack imposée — aucun service annexe (pas de pgAdmin, Flower, proxy…).

**Limite assumée à la mutualisation** : les **2 Airflow restent séparés** (métier vs ingestion d'OpenMetadata). Mutualiser Postgres est un gain net ; mutualiser Airflow aurait introduit un **couplage fragile** pour un bénéfice faible — donc **non retenu** (cf. [docs/architecture.md](../docs/architecture.md)).

## 2. C18 — Architecture & analyse des données (Jour 1)

### 2.1 Activités réalisées

- **Socle technique** : stack conteneurisée (MinIO + buckets, Postgres mutualisé, Airflow, OpenMetadata, Elasticsearch) et un conteneur de développement où le code s'exécute *dans* le réseau Docker (mêmes noms d'hôte qu'en production).
- **Téléchargement des sources** : récupération des 5 CSV depuis Zenodo via son **API REST**, avec **vérification d'intégrité MD5** et idempotence ([datalake/download.py](../datalake/download.py)).
- **Exploration** des 5 lignes (volumétrie, schémas, types, distribution du `label`, couverture temporelle) au moyen de fonctions réutilisables ([datalake/explore.py](../datalake/explore.py)) pilotées depuis un notebook.
- **Modélisation** de l'architecture en couches et **schéma technique annoté** ([docs/architecture.md](../docs/architecture.md)).

### 2.2 Analyse des données & hétérogénéités identifiées

Constats clés (30 000 enregistrements, 1 relevé/minute, janvier→mai 2025, ~1 % d'anomalies) :

- **Casse des colonnes non uniforme** (et parfois incohérente au sein d'un fichier) : `Temperature`/`temperature`, `Pressure`/`pressure`, `Elapsed_time`/`elapsed_time`.
- **`elapsed_time` optionnel** : présent uniquement sur Line A et Line B.
- **`timestamp`** au format `YYYY-MM-DD HH:MM:SS` (non ISO-8601, sans fuseau), cadence régulière d'1 minute, **continu** (ni trou ni doublon).
- **`label`** binaire {0, 1} (0 = nominal, 1 = anomalie), **fortement déséquilibré** (~1 %).
- **Écart documentation ↔ données** : LineE annoncée « 0 % d'anomalies » mais en contient 0,5 % — *la donnée fait foi*, à tracer comme avertissement qualité.
- **Un fichier source = un seul mois** (vérifié) : déterminant pour le partitionnement.

### 2.3 Modélisation de l'architecture & choix justifiés

Architecture en **4 couches** (raw → staging → curated → archive). Principaux choix (détail et justifications dans [docs/architecture.md](../docs/architecture.md)) :

- **Partitionnement à deux granularités** : `raw` au **mois** (permet de déposer les fichiers *tels quels* + MD5) ; `staging`/`curated` au **jour**, avec un traitement **au fil de l'eau** (un jour à la fois) qui simule un flux temps réel.
- **Formats** : CSV en `raw` (fidélité à la source), **Parquet** en aval (typé, compressé, colonnaire — adapté à l'analytique et au ML).
- **Schéma cible unifié** harmonisant les hétérogénéités (minuscules, `timestamp` ISO-8601, `elapsed_time` en `NULL` si absent) ; **clé naturelle `(line, timestamp)`** garantissant l'idempotence.
- **3 DAGs** (ingestion → harmonisation → consolidation) ; le 3ᵉ (`staging → curated`) n'est pas exigé par l'énoncé mais **assumé** pour la cohérence du flux.
- **Schéma en Mermaid** (diagramme-as-code, versionnable, rendu sur GitHub) plutôt que draw.io.

### 2.4 Notions abordées

Architecture en couches d'un data lake ; partitionnement (style Hive) et son lien avec volumétrie/fréquence ; **idempotence** et clé naturelle ; **intégrité** par hash MD5 ; distinction **`NaN` vs `NULL`** ; **déséquilibre de classes** (enjeu central de la détection d'anomalies) ; gouvernance des métadonnées (réconcilier doc et données) ; diagramme-as-code.

## 3. C19 — Intégration (Jours 2 à 4)

*(À compléter : MinIO & buckets, script d'upload + MD5, DAGs d'ingestion/harmonisation/consolidation, partitionnement et traitement au fil de l'eau, procédure d'intégration.)*

## 4. C20 — Catalogue & cycle de vie (Jour 5)

*(À compléter : connexion OpenMetadata ↔ MinIO, 5 fiches métadonnées, politique ILM 180 j / 2 ans.)*

## 5. C21 — Sécurité & gouvernance (Jours 6-7)

*(À compléter : 3 rôles différenciés, chiffrement SSE-S3, logs d'audit, politique de gouvernance.)*

## 6. Notions, difficultés & auto-évaluation

*(À compléter au fil de l'eau : notions mobilisées, difficultés rencontrées et résolues, auto-évaluation par compétence.)*

## 7. Annexes

- Schéma d'architecture annoté : [docs/architecture.md](../docs/architecture.md) (§2).
- Exploration des données : [notebooks/exploration_jour1.ipynb](../notebooks/exploration_jour1.ipynb).
- Dépôt Git organisé : voir [README.md](../README.md).
