# C21 — Sécurité & gouvernance : conception

## Contexte & objectif

C21 (Jours 6-7 de l'énoncé) couvre la sécurité et la gouvernance du data lake. Les **3 comptes de service aux droits différenciés par bucket** existent déjà (réalisés au C19 : `data-analyst` RO sur `curated`, `data-engineer` RW sur `raw`/`staging`/`curated`, `admin` tous droits). Restent à traiter : le **chiffrement au repos SSE-S3** et la **formalisation écrite** de la gouvernance (matrice des droits + politique). Les logs d'audit sont **écartés** de ce périmètre (décision projet, cf. « Hors périmètre »).

## Périmètre

**Dans le périmètre :**
- Chiffrement **SSE-S3** actif sur les 4 buckets (`raw`, `staging`, `curated`, `archive`).
- Extension de la policy `data-engineer` au bucket `archive` (lecture/écriture complète).
- Document de gouvernance : **matrice des droits** + **politique écrite** (qui accède à quoi, sous quelles conditions, responsabilités par rôle).
- Mises à jour du README (tableau des livrables) et du rapport (entrée + auto-évaluation C21).

**Hors périmètre (assumé et justifié) :**
- **Logs d'audit MinIO** : écartés. MinIO n'écrit pas l'audit dans un fichier (cible webhook/Kafka requise) ; le surcoût d'infra n'est pas retenu pour ce projet. Mentionné en « évolutions possibles » dans la politique.
- **Ségrégation d'accès par ligne de production** : non implémentée. L'accès est uniforme sur les 5 lignes (A→E) ; la maintenance prédictive les exploite toutes. Mentionnée en « évolutions possibles » (policies par préfixe `production_lines/lineX/`).
- **KES / magasin de clés externe** : non retenu. Contredit la simplicité de l'énoncé (installation MinIO mono-conteneur). Mentionné comme piste production.

## Décisions

1. **KMS intégré à clé unique** (`MINIO_KMS_SECRET_KEY`) plutôt que KES : seule option fidèle à la simplicité du brief, suffisante pour une démo locale.
2. **Chiffrement des 4 buckets** (et non seulement `raw`/`staging`/`curated`) : tous contiennent des données de capteurs de production, `archive` compris ; plus cohérent.
3. **`data-engineer` en lecture/écriture complète sur `archive`** (suppression incluse) : ce rôle porte le cycle de vie (archivage + réintégration). Extension au-delà de la lettre de l'énoncé (l.76 ne listait que raw/staging/curated), assumée.
4. **Pas de régénération des données déjà ingérées** : SSE-S3 ne chiffre que les écritures futures ; les objets de démo présents restent en clair. Le rapport précise qu'il faudrait idéalement chiffrer **avant** l'ingestion, non fait ici volontairement (pour ne pas rejouer le pipeline).

## 1. Chiffrement SSE-S3

**Mécanisme.** MinIO exige un KMS pour le SSE-S3. On active le **KMS intégré à clé unique** via la variable d'environnement `MINIO_KMS_SECRET_KEY` (format `nom-clé:<32 octets encodés base64>`) sur le service `minio`. Puis on pose l'**auto-chiffrement** SSE-S3 sur chaque bucket : tout objet écrit ensuite est chiffré côté serveur, de façon transparente pour les clients (boto3, `mc`, DuckDB — aucune gestion de clé côté client).

**Implémentation.**
- `compose.yaml` : ajout de `MINIO_KMS_SECRET_KEY: ${MINIO_KMS_SECRET_KEY}` sur le service `minio`.
- `.env` (secret) + `.env.example` (placeholder + commande de génération d'une clé de démo).
- `init-scripts/minio/setup.sh` : après la création des buckets, `mc encrypt set sse-s3 local/<bucket>` pour les 4 buckets. Idempotent.

**Données existantes.** L'auto-chiffrement ne s'applique qu'aux écritures postérieures ; les objets déjà présents restent en clair (conservés, lisibles). Pour une reproduction *from scratch*, la règle étant posée par `minio-init` **avant** tout chargement, l'intégralité des données est chiffrée. Sur l'instance courante déjà peuplée, on ne régénère pas (cf. décision 4) — documenté dans le rapport.

**Vérification.**
- `mc encrypt info local/<bucket>` → règle SSE-S3 active sur les 4 buckets.
- Upload d'un objet témoin puis `mc stat` → en-tête de chiffrement SSE-S3 présent (preuve que la règle chiffre les nouveaux objets).

## 2. Matrice des droits & politique de gouvernance

**Nouveau document** `docs/gouvernance-acces-securite.md` (français, vouvoiement).

**a. Matrice des droits** (rôle × bucket × permission) :

| Rôle (compte) | raw | staging | curated | archive |
|---|---|---|---|---|
| `data-analyst` | — | — | lecture | — |
| `data-engineer` | RW | RW | RW | RW |
| `admin` | tous droits | tous droits | tous droits | tous droits |

**b. Politique de gouvernance écrite :**
- **Qui accède à quelles lignes** : chaque rôle accède à **toutes les lignes de production (A→E)** de sa/ses couche(s) — pas de ségrégation par ligne (besoin métier : les 5 lignes servent la maintenance prédictive).
- **Sous quelles conditions** : comptes de service dédiés (le compte root n'est pas utilisé au quotidien) ; secrets via `.env` non versionné ; accès réseau interne au réseau Docker ; `data-analyst` strictement en lecture seule sur `curated`.
- **Responsabilités par rôle** : `data-analyst` (consultation et analyse sur `curated`) ; `data-engineer` (pipelines d'ingestion et de transformation, qualité, **cycle de vie** archivage/réintégration) ; `admin` (infrastructure, gestion des comptes et policies, chiffrement, supervision de l'ILM).
- **Chiffrement** : rappel du SSE-S3 au repos sur les 4 buckets (transparence côté client).
- **Évolutions possibles** : ségrégation par ligne (policies par préfixe) ; logs d'audit (webhook) ; KES pour la gestion des clés en production.

**Implémentation associée.**
- `init-scripts/minio/policies/data-engineer.json` : ajout du bucket `archive` (mêmes actions que les autres buckets).
- `init-scripts/minio/setup.sh` : inchangé sur l'attachement (la policy modifiée suffit).
- `README.md` : ligne C21 du tableau des livrables → ✅ + brève section « Sécurité & gouvernance ».
- `rapport/rapport.md` : entrée datée C21 + mise à jour de l'auto-évaluation C21.

## Fichiers touchés

- Modifiés : `compose.yaml`, `.env.example`, `init-scripts/minio/setup.sh`, `init-scripts/minio/policies/data-engineer.json`, `README.md`, `rapport/rapport.md`.
- Créés : `docs/gouvernance-acces-securite.md`.
- Secret local (non versionné) : `MINIO_KMS_SECRET_KEY` dans `.env`.

## Vérification globale

- `docker compose up minio-init` rejoue sans erreur (idempotent) : buckets, policies (dont `archive` pour data-engineer), chiffrement SSE-S3, ILM.
- Chiffrement : `mc encrypt info` (×4) + objet témoin chiffré (`mc stat`).
- Droits : `mc admin policy info local data-engineer` inclut `archive` ; session `data-analyst` (lecture `curated` OK, écriture refusée).
- Pas de test unitaire (périmètre infra/documentation, pas de logique Python).
