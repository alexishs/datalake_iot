# Gouvernance des accès & sécurité du data lake (C21)

Ce document formalise **qui accède à quoi, sous quelles conditions, avec quelles responsabilités**, ainsi que le chiffrement au repos. Il complète la politique de cycle de vie ([gouvernance-cycle-de-vie.md](gouvernance-cycle-de-vie.md)).

## Matrice des droits d'accès

Trois comptes de service MinIO aux policies IAM différenciées par bucket (créés par [../init-scripts/minio/setup.sh](../init-scripts/minio/setup.sh)).

| Rôle (compte) | raw | staging | curated | archive |
|---|---|---|---|---|
| `data-analyst` | — | — | lecture | — |
| `data-engineer` | lecture/écriture | lecture/écriture | lecture/écriture | lecture/écriture |
| `datalake-admin` | tous droits | tous droits | tous droits | tous droits |

Le compte **root** MinIO (`minioadmin`) n'est pas utilisé au quotidien ; il sert à l'amorçage (job `minio-init`) et à l'administration exceptionnelle.

## Qui accède à quelles lignes

Le contrôle d'accès est **par couche** (bucket), **uniforme sur les 5 lignes de production** (`lineA`…`lineE`) : un rôle qui accède à une couche accède à toutes ses lignes. Ce choix reflète le besoin métier — la maintenance prédictive exploite l'ensemble des lignes. Une ségrégation **par ligne** (policies restreintes aux préfixes `production_lines/lineX/`) est possible mais n'est pas requise ici (cf. « Évolutions possibles »).

## Sous quelles conditions

- **Comptes de service dédiés** : chaque usage passe par le compte de son rôle, pas par le compte root.
- **Secrets hors dépôt** : identifiants et clé de chiffrement dans `.env` (non versionné) ; seuls des placeholders figurent dans `.env.example`.
- **Périmètre réseau** : MinIO est exposé sur le réseau Docker interne ; les clients (Airflow, conteneur `dev`, DuckDB) s'y connectent par le nom d'hôte `minio:9000`.
- **Moindre privilège** : `data-analyst` est strictement en **lecture seule** sur la seule couche `curated` (données prêtes à l'analyse).

## Responsabilités par rôle

- **`data-analyst`** — consultation et analyse des données `curated` (SQL/DuckDB, tableaux de bord). Aucune écriture, aucun accès aux couches amont.
- **`data-engineer`** — pipelines d'ingestion et de transformation (`raw`→`staging`→`curated`), qualité des données, et **cycle de vie** (archivage `raw`→`archive` et réintégration). Dispose d'un accès lecture/écriture sur les 4 buckets.
- **`datalake-admin`** — infrastructure, création et attachement des comptes/policies, configuration du chiffrement, supervision des règles ILM.

## Chiffrement au repos (SSE-S3)

Les 4 buckets sont en **auto-chiffrement SSE-S3** (chiffrement côté serveur, clés gérées par le KMS intégré de MinIO). Le chiffrement est **transparent** pour les clients : aucune gestion de clé côté boto3, `mc` ou DuckDB. Il porte sur les objets écrits après activation ; idéalement la règle est posée **avant** toute ingestion (cf. rapport pour la note sur les données de démo déjà présentes).

## Évolutions possibles

- **Ségrégation par ligne** : policies IAM restreintes aux préfixes `production_lines/lineX/` pour limiter un compte à certaines lignes.
- **Logs d'audit** : activation de l'audit MinIO vers un webhook (collecteur) pour tracer et analyser les accès.
- **Gestion des clés en production** : remplacer le KMS intégré à clé unique par **KES** adossé à un magasin de clés (rotation, séparation des secrets).
