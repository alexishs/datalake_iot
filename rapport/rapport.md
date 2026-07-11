# Rapport — Data Lake IoT industriel

## 1. Contexte & objectifs

Mission confiée par la DSI d'un équipementier automobile : concevoir et déployer un **data lake** pour centraliser, documenter, sécuriser et gouverner les données de **5 lignes de production** instrumentées (température, pression, temps de fonctionnement), aujourd'hui stockées « en vrac ». Finalité : constituer la fondation d'un futur projet de **maintenance prédictive** (détection d'anomalies, anticipation des pannes).

Stack imposée : **MinIO** (stockage objet S3), **Apache Airflow** (orchestration), **OpenMetadata** (catalogue), **Python/boto3**, **Docker Compose**, **Git**.

**Démarche — le socle technique d'abord.** La **stack complète a été montée en premier** (le 2 juin), avant l'analyse et le développement. Disposer d'emblée d'un environnement **opérationnel et reproductible** (relançable par un tiers via Docker Compose) a permis de mener ensuite l'analyse et la modélisation sur une base saine. *(Cela ne contrevient pas au principe « analyser avant de coder » du C18 : l'analyse des données précède toujours les **décisions d'architecture** ; seul l'**environnement d'exécution** est monté au préalable.)* C'est cette démarche qui explique pourquoi le [journal de réalisation](journal.md) est organisé par **date réelle** plutôt que selon la numérotation des « jours » de l'énoncé.

## 2. Auto-évaluation par compétence

- **C18 — Architecture & analyse** : *acquis*. Les 5 lignes ont été analysées **avant** toute décision technique (volumétrie, schémas, hétérogénéités) ; l'architecture en couches est justifiée au regard de la volumétrie et de la fréquence ; le schéma annoté est lisible et exploitable par un tiers.
- **C19 — Intégration** : *acquis*. Stack reproductible via Docker Compose ; ingestion vers `raw/` avec **vérification MD5** et **idempotence** ; **3 DAGs** (ingestion → harmonisation → consolidation) en fil-de-l'eau ; LineA traitée par jour (chunks) ; procédure d'intégration documentée dans le README.
- **C20 — Catalogue & cycle de vie** : *acquis*. **Catalogue OpenMetadata** config-as-code : service S3 → MinIO, 5 fiches `raw` (colonnes documentées, propriétaire, source, fréquence), 4 pipelines et **lignage** `raw→staging→curated` + `raw→archive` ; catalogue **non-invasif**. **Cycle de vie** : archivage `raw→archive` par DAG (l'ILM MinIO ne copiant pas localement) et **expiration** par règle ILM (730 j), réintégration auto-réparante par filigrane.
- **C21 — Sécurité & gouvernance** : *acquis (hors audit)*. **3 comptes** aux droits différenciés par bucket (C19), **chiffrement SSE-S3** au repos sur les 4 buckets (KMS intégré), **matrice des droits + politique de gouvernance** écrite. Les **logs d'audit** ne sont pas activés (choix assumé, documenté en évolution possible), de même que la ségrégation par ligne.
- **Recul sur les choix** : plusieurs décisions vont **au-delà de l'énoncé** tout en restant justifiées et documentées — 3ᵉ DAG de consolidation, cadence minute, **filigrane auto-réparant**, exploration des données en SQL (**DuckDB**) — sans complexifier inutilement le projet.

## 3. Annexes

**Documentation du projet** (également indexée dans le [README](../README.md)) :

- [README.md](../README.md) — **installation et utilisation** : reproduction de l'environnement, exécution du code, sommaire + index de la documentation.
- [docs/architecture.md](../docs/architecture.md) — **architecture** en couches, partitionnement, schéma annoté, contrat d'implémentation (C18).
- [docs/gouvernance-cycle-de-vie.md](../docs/gouvernance-cycle-de-vie.md) — politique de **cycle de vie** : archivage `raw→archive` par DAG + expiration ILM (C20).
- [docs/gouvernance-acces-securite.md](../docs/gouvernance-acces-securite.md) — **matrice des droits** + politique de gouvernance & chiffrement SSE-S3 (C21).
- [init-scripts/openmetadata/README.md](../init-scripts/openmetadata/README.md) — procédure d'ingestion du **catalogue OpenMetadata** (config-as-code) : conteneurs, fiches, lignage.
- [docs/captures-openmetadata/README.md](../docs/captures-openmetadata/README.md) — **captures d'écran** du catalogue (C20).
- [journal.md](journal.md) — **journal de réalisation** au fil de l'eau (activités, notions, choix par date) dont ce rapport est la consolidation.

**Autres ressources :**

- Exploration des données (Jour 1) : [notebooks/exploration_jour1.ipynb](../notebooks/exploration_jour1.ipynb).
- Schéma d'architecture annoté : [docs/architecture.md](../docs/architecture.md) (§2).
- Dépôt Git organisé (structure du dépôt) : voir le [README](../README.md).
