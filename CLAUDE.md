# CLAUDE.md

## Nature du projet

Ce dépôt est un **exercice pédagogique** (titre professionnel, épreuve E7). L'énoncé complet
fait foi : voir [ennonce.md](ennonce.md). Le but est l'apprentissage du métier de Data Engineer,
pas seulement la livraison d'un résultat.

**Posture attendue de Claude :** privilégier l'explication et l'accompagnement plutôt que la
production de code « clé en main ». Expliquer le *pourquoi* des choix techniques, proposer des
options et leurs compromis, et laisser l'apprenant comprendre chaque étape. Ne pas survoler les
notions : c'est l'occasion de les apprendre.

## Objectif

Concevoir et déployer un **data lake** pour des données de capteurs IoT industriels (5 lignes de
production), en vue d'un futur projet de maintenance prédictive : centraliser, documenter,
sécuriser et gouverner les flux de données.

## Stack imposée

- **Stockage objet** : MinIO Community (Docker)
- **Orchestration** : Apache Airflow (DAGs d'ingestion + de transformation)
- **Catalogue / métadonnées** : OpenMetadata
- **Code** : Python / boto3
- **Infra** : Docker Compose
- **Versionnage** : Git

Ne pas introduire d'autres technologies sans raison justifiée — rester sur la stack du brief.

## Architecture en couches (à respecter)

```
raw/      ← données brutes, telles que reçues (partitionnement year=/month=/line=/)
staging/  ← données nettoyées, schéma harmonisé
curated/  ← données prêtes à l'analyse
archive/  ← données expirées (règle ILM MinIO)
```

Partitionnement raw imposé : `raw/production_lines/lineX/year=YYYY/month=MM/...`

## Données

- Source : Synthetic Data from Industrial Sensor Monitoring (Zenodo, https://zenodo.org/records/15277168)
- 5 fichiers CSV (Line A à E), comportements distincts. Line A = 10 000 enregistrements
  (à traiter **en chunks** pour simuler un flux réel).
- **Point de vigilance central :** les schémas diffèrent entre lignes (casse des colonnes
  `Temperature` vs `temperature`, présence/absence de `elapsed_time`). L'harmonisation de cette
  hétérogénéité est une difficulté volontaire et un point d'évaluation.
- Champ `label` : `0` = nominal, `1` = anomalie.

## Règles de base

1. **Analyser avant de coder** (C18) : explorer les 5 lignes et documenter les écarts de schéma
   *avant* toute décision technique.
2. **Reproductibilité** : tout doit pouvoir être relancé par un tiers via Docker Compose + README.
   Documenter chaque procédure.
3. **Intégrité** : vérifier les fichiers ingérés (hash MD5).
4. **Sécurité & gouvernance** (C21) : 3 rôles strictement différenciés
   (`data-analyst` lecture seule sur `curated/`, `data-engineer` lecture/écriture sur
   `raw/` + `staging/` + `curated/`, `admin` tous droits), chiffrement SSE-S3, logs d'audit.
5. **Pas de secrets en clair commités** : credentials via variables d'environnement / `.env`
   (non versionné). Les identifiants `minioadmin/minioadmin` de l'énoncé sont des valeurs de
   démo locale uniquement.
6. **Documenter au fil de l'eau** (exigé) : chaque livrable a une trace écrite produite au moment
   où le travail est fait (README, procédure d'intégration, doc de gouvernance, politique ILM).
   - **Mettre à jour le rapport à la fin de chaque « jour » de l'énoncé** (Jour 1, Jour 2,
     Jours 3-4, Jour 5, Jours 6-7) : consigner dans `rapport/rapport.md` les **activités** réalisées,
     les **notions** abordées et les **choix justifiés** (le *quoi* et le *pourquoi*).
   - *(L'énoncé n'exige formellement que le rapport final de fin de Jour 8 ; cette cadence — une mise
     à jour par « jour » — est une **règle de travail adoptée pour ce projet**, afin de ne pas tout
     rédiger le dernier jour.)*
   - Le **rapport final** (≥ 5 pages, Jour 8) est la **consolidation et la mise au propre** de ces
     mises à jour, exporté en PDF.
7. **Langue** : documentation et rapport en français. **Vouvoiement obligatoire** dans tous les
   fichiers (README, docs, commentaires de code, rapport) comme dans les échanges — jamais de
   tutoiement.

## Organisation du dépôt (cible)

Garder une structure claire et lisible. Pistes :

```
ennonce.md           ← brief (ne pas modifier le fond)
CLAUDE.md            ← ce fichier
README.md            ← installation + reproduction de l'environnement
compose.yaml         ← MinIO, Airflow, OpenMetadata + service `dev`
Dockerfile.dev       ← image du conteneur de dev (VSCode s'y attache)
.devcontainer/       ← config Dev Containers (extensions debug auto)
.vscode/             ← launch.json (debug)
.env.example         ← variables (copier en .env ; AUCUN nom d'hôte dedans)
init-scripts/        ← init Postgres mutualisé (3 bases isolées)
datalake/            ← PACKAGE métier (boto3, ingestion, harmonisation)
dags/                ← DAGs Airflow : coquilles fines qui importent `datalake`
data/                ← CSV bruts téléchargés (non versionnés si volumineux)
docs/                ← architecture, gouvernance, ILM, captures
rapport/             ← rapport final (≥ 5 pages)
```

**Règle code/orchestration :** toute la logique métier vit dans le package
`datalake/` (lançable seul : `python -m datalake...`). Les DAGs n'en sont que des
appelants (`from datalake.x import run` dans un `PythonOperator`), zéro logique
métier dedans. Le code est **strictement identique** en debug (conteneur `dev`)
et dans Airflow : les deux tournent dans le réseau Docker, donc `minio:9000`
résout pareil. **Aucun nom d'hôte dans `.env`** (défaut `http://minio:9000` dans
le code/compose) ; seuls les secrets y figurent.

## Livrables (rappel — détail dans l'énoncé)

Architecture annotée (C18) · `docker-compose.yml` + script upload/intégrité (C19) ·
2 DAGs Airflow + README (C19) · catalogue OpenMetadata 5 fiches + ILM (C20) ·
matrice des droits + politique de gouvernance (C21) · rapport ≥ 5 pages.
