# Sécurité & gouvernance (C21) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Chiffrer le data lake au repos (SSE-S3 sur les 4 buckets) et formaliser la gouvernance (matrice des droits + politique écrite), en étendant l'accès `data-engineer` au bucket `archive`.

**Architecture:** Deux volets sans logique Python. (A) **Chiffrement** — KMS intégré MinIO (`MINIO_KMS_SECRET_KEY`) + auto-chiffrement `mc encrypt set sse-s3` posé par le job `minio-init` ; agit sur les écritures futures. (B) **Gouvernance** — mise à jour d'une policy IAM (`data-engineer.json`) + document Markdown (matrice + politique). Spec : [docs/superpowers/specs/2026-07-10-c21-securite-gouvernance-design.md](../specs/2026-07-10-c21-securite-gouvernance-design.md).

**Tech Stack:** MinIO `RELEASE.2025-09-07` + `mc`, Docker Compose, Markdown. Aucun code Python, aucun test unitaire (périmètre infra/documentation) : la validation se fait par commandes `mc` avec sortie attendue.

## Global Constraints

- **AUCUN commit** : ne lancez ni `git add` ni `git commit`. L'utilisateur committe après revue. Les tâches s'arrêtent à « vérification verte ». Les messages de commit proposés sont **en français** et **sans aucune mention d'agent IA** (pas de trailer `Co-Authored-By:`).
- **Vouvoiement** dans toute la documentation et les commentaires. **Markdown sans hard-wrap** (un paragraphe/puce = une seule ligne).
- **Secrets** : la clé KMS va dans `.env` (non versionné) ; `.env.example` ne contient qu'un placeholder.
- **Idempotence** : `docker compose up minio-init` doit rester rejouable sans erreur.
- **Ne pas régénérer** les données déjà ingérées (décision spec 4) : le chiffrement ne vaut que pour les écritures futures ; assumé et documenté.
- **Commandes** : exécutées depuis l'hôte via `docker exec dl-minio sh -c '…'` (l'image MinIO embarque `mc` ; l'alias `local` doit être posé dans chaque invocation, comme dans les vérifications existantes).

---

## Task 1 : Activer le KMS + chiffrement SSE-S3 des 4 buckets

**Files:**
- Modify: `compose.yaml` (service `minio`, bloc `environment`, après la ligne `MINIO_ROOT_PASSWORD`)
- Modify: `.env.example` (nouvelle variable, après le bloc MinIO)
- Modify: `.env` (clé réelle de démo)
- Modify: `init-scripts/minio/setup.sh` (bloc chiffrement, après la boucle de création des buckets)

- [ ] **Step 1 — Générer une clé de démo et renseigner `.env`**

Générer la partie aléatoire :

```bash
openssl rand -base64 32
```

Ajouter à la fin de `.env` (remplacer `<base64>` par la sortie ci-dessus) :

```
# --- Chiffrement au repos SSE-S3 (C21) : clé du KMS intégré MinIO ---
MINIO_KMS_SECRET_KEY=datalake-demo-key:<base64>
```

- [ ] **Step 2 — Ajouter le placeholder à `.env.example`** (après le bloc « Comptes de service MinIO »)

```
# --- Chiffrement au repos SSE-S3 (C21) : clé du KMS intégré MinIO ---
# Active le SSE-S3 (chiffrement côté serveur, transparent pour les clients).
# Format : <nom-clé>:<32 octets aléatoires en base64>. Générer la partie clé par :
#   openssl rand -base64 32
# Secret : ne pas committer (le vrai .env est ignoré par git).
MINIO_KMS_SECRET_KEY=datalake-demo-key:change-me-base64-32-bytes
```

- [ ] **Step 3 — Déclarer la variable sur le service `minio` de `compose.yaml`**

Dans le bloc `environment:` du service `minio` (actuellement lignes 104-106), ajouter sous `MINIO_ROOT_PASSWORD` :

```yaml
      # KMS intégré à clé unique : active le chiffrement SSE-S3 (C21).
      MINIO_KMS_SECRET_KEY: ${MINIO_KMS_SECRET_KEY}
```

- [ ] **Step 4 — Ajouter l'auto-chiffrement à `init-scripts/minio/setup.sh`**

Juste **après** la boucle `for bucket in raw staging curated archive; do mc mb … done` (création des buckets), insérer :

```sh
# --- Chiffrement au repos SSE-S3 sur les 4 buckets (C21) -------------------
# Auto-chiffrement : tout objet écrit ENSUITE est chiffré côté serveur via le
# KMS intégré (clé MINIO_KMS_SECRET_KEY). Non rétroactif : les objets déjà
# présents restent en clair (cf. rapport). Requiert le KMS actif sur le serveur.
for bucket in raw staging curated archive; do
  mc encrypt set sse-s3 "local/$bucket"
done
echo "SSE-S3 : auto-chiffrement activé sur raw/staging/curated/archive."
```

- [ ] **Step 5 — Recréer le conteneur MinIO pour charger le KMS, puis rejouer l'init**

Le changement d'environnement impose de recréer `minio` (le volume de données persiste) :

```bash
docker compose up -d minio
docker compose up minio-init
```

Expected : `minio-init` se termine sans erreur, avec les lignes `SSE-S3 : auto-chiffrement activé …` et `MinIO prêt : …`.

- [ ] **Step 6 — Vérifier que le chiffrement est actif sur les 4 buckets**

```bash
docker exec dl-minio sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; for b in raw staging curated archive; do echo "== $b =="; mc encrypt info "local/$b"; done'
```

Expected : pour chaque bucket, `Auto encryption 'sse-s3' is enabled`.

- [ ] **Step 7 — Prouver qu'un nouvel objet est chiffré (objet témoin)**

```bash
docker exec dl-minio sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; echo test-sse | mc pipe local/curated/_sse_test.txt; mc stat local/curated/_sse_test.txt; mc rm local/curated/_sse_test.txt'
```

Expected : `mc stat` affiche une ligne d'chiffrement, p. ex. `Encryption: SSE-S3` (ou en-tête `X-Amz-Server-Side-Encryption: AES256`). L'objet témoin est supprimé à la fin.

> Vérification finale de Task 1 : l'implémenteur **s'arrête ici** (pas de commit). Message de commit proposé (pour l'utilisateur) : `feat(securite) : chiffrement SSE-S3 au repos sur les 4 buckets via KMS intégré (C21)`.

---

## Task 2 : Étendre la policy `data-engineer` au bucket `archive`

**Files:**
- Modify: `init-scripts/minio/policies/data-engineer.json`

**Interfaces:**
- Consumes: le job `minio-init` (Task 1 confirme qu'il rejoue proprement) ; `mc admin policy create` **écrase** une policy existante (vérifié), donc relancer l'init applique la nouvelle version.

- [ ] **Step 1 — Remplacer le contenu de `init-scripts/minio/policies/data-engineer.json`**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListerRawStagingCuratedArchive",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": [
        "arn:aws:s3:::raw",
        "arn:aws:s3:::staging",
        "arn:aws:s3:::curated",
        "arn:aws:s3:::archive"
      ]
    },
    {
      "Sid": "LireEcrireObjetsRawStagingCuratedArchive",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::raw/*",
        "arn:aws:s3:::staging/*",
        "arn:aws:s3:::curated/*",
        "arn:aws:s3:::archive/*"
      ]
    }
  ]
}
```

- [ ] **Step 2 — Rejouer l'init pour appliquer la policy mise à jour**

```bash
docker compose up minio-init
```

Expected : se termine sans erreur (le `mc admin policy create … || true` écrase la policy existante).

- [ ] **Step 3 — Vérifier que la policy inclut `archive`**

```bash
docker exec dl-minio sh -c 'mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; mc admin policy info local data-engineer' | grep -c archive
```

Expected : `2` (le bucket `archive` apparaît dans les deux Statements : ARN bucket + ARN objets).

- [ ] **Step 4 — Vérifier l'accès réel `data-engineer` sur `archive` (écriture + lecture)**

```bash
docker exec dl-minio sh -c '
mc alias set de http://localhost:9000 "$MINIO_ENGINEER_USER" "$MINIO_ENGINEER_PASSWORD" >/dev/null 2>&1
echo de-archive-test | mc pipe de/archive/_de_test.txt && echo "ECRITURE OK"
mc cat de/archive/_de_test.txt && echo "LECTURE OK"
mc rm de/archive/_de_test.txt >/dev/null 2>&1
'
```

Expected : `ECRITURE OK` puis `de-archive-test` + `LECTURE OK`.

> Vérification finale de Task 2 : **arrêt** (pas de commit). Message proposé : `feat(gouvernance) : data-engineer en lecture/écriture sur le bucket archive (C21)`.

---

## Task 3 : Document de gouvernance (matrice + politique)

**Files:**
- Create: `docs/gouvernance-acces-securite.md`

- [ ] **Step 1 — Créer `docs/gouvernance-acces-securite.md`** avec le contenu suivant

```markdown
# Gouvernance des accès & sécurité du data lake (C21)

Ce document formalise **qui accède à quoi, sous quelles conditions, avec quelles responsabilités**, ainsi que le chiffrement au repos. Il complète la politique de cycle de vie ([gouvernance-cycle-de-vie.md](gouvernance-cycle-de-vie.md)).

## Matrice des droits d'accès

Trois comptes de service MinIO aux policies IAM différenciées par bucket (créés par [../init-scripts/minio/setup.sh](../init-scripts/minio/setup.sh)).

| Rôle (compte) | raw | staging | curated | archive |
|---|---|---|---|---|
| `data-analyst` | — | — | lecture | — |
| `data-engineer` | lecture/écriture | lecture/écriture | lecture/écriture | lecture/écriture |
| `admin` | tous droits | tous droits | tous droits | tous droits |

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
- **`admin`** — infrastructure, création et attachement des comptes/policies, configuration du chiffrement, supervision des règles ILM.

## Chiffrement au repos (SSE-S3)

Les 4 buckets sont en **auto-chiffrement SSE-S3** (chiffrement côté serveur, clés gérées par le KMS intégré de MinIO). Le chiffrement est **transparent** pour les clients : aucune gestion de clé côté boto3, `mc` ou DuckDB. Il porte sur les objets écrits après activation ; idéalement la règle est posée **avant** toute ingestion (cf. rapport pour la note sur les données de démo déjà présentes).

## Évolutions possibles

- **Ségrégation par ligne** : policies IAM restreintes aux préfixes `production_lines/lineX/` pour limiter un compte à certaines lignes.
- **Logs d'audit** : activation de l'audit MinIO vers un webhook (collecteur) pour tracer et analyser les accès.
- **Gestion des clés en production** : remplacer le KMS intégré à clé unique par **KES** adossé à un magasin de clés (rotation, séparation des secrets).
```

- [ ] **Step 2 — Vérifier le vouvoiement**

```bash
grep -rnE "\b(tu|ton|ta|tes|toi)\b" docs/gouvernance-acces-securite.md || echo "OK vouvoiement"
```

Expected : `OK vouvoiement`.

> Vérification finale de Task 3 : **arrêt**. Message proposé : `docs(gouvernance) : matrice des droits + politique de gouvernance & sécurité (C21)`.

---

## Task 4 : Mises à jour README + rapport

**Files:**
- Modify: `README.md` (tableau des livrables, ligne C21 ; + brève section)
- Modify: `rapport/rapport.md` (entrée datée C21 sous §2 + auto-évaluation §3)

- [ ] **Step 1 — Mettre la ligne C21 du tableau des livrables du `README.md` à ✅**

Repérer la ligne actuelle (statut ◐) :

```
| **C21 · J6-7** — Gouvernance | Comptes & droits différenciés ✅ ; SSE-S3, audit, politique écrite ⏳ | [init-scripts/minio/](init-scripts/minio/), `docs/` *(à créer)* | ◐ |
```

La remplacer par :

```
| **C21 · J6-7** — Gouvernance | Droits différenciés par bucket + **SSE-S3** au repos + matrice & politique écrite | [init-scripts/minio/](init-scripts/minio/), [docs/gouvernance-acces-securite.md](docs/gouvernance-acces-securite.md) | ✅ |
```

- [ ] **Step 2 — Ajouter une brève section « Sécurité & gouvernance » au `README.md`**

Juste avant la section `## Dépendances Python (`requirements.txt`)`, insérer :

```markdown
## Sécurité & gouvernance (C21)

- **Droits par bucket** : 3 comptes de service (`data-analyst` lecture seule sur `curated` ; `data-engineer` lecture/écriture sur les 4 buckets ; `admin` tous droits), créés par [init-scripts/minio/setup.sh](init-scripts/minio/setup.sh).
- **Chiffrement au repos SSE-S3** sur les 4 buckets, via le KMS intégré de MinIO (`MINIO_KMS_SECRET_KEY`) — transparent pour les clients. Vérifier : `mc encrypt info local/<bucket>`.
- **Politique complète** (matrice des droits, conditions, responsabilités) : [docs/gouvernance-acces-securite.md](docs/gouvernance-acces-securite.md).
```

- [ ] **Step 3 — Ajouter l'entrée C21 datée au `rapport/rapport.md`** (sous §2, après l'entrée « 9 juillet 2026 — C20 : catalogue OpenMetadata »)

```markdown
### 10 juillet 2026 — C21 : sécurité & gouvernance (Jours 6-7)

**Activités réalisées.**

- **Chiffrement au repos SSE-S3** activé sur les 4 buckets via le **KMS intégré** de MinIO (`MINIO_KMS_SECRET_KEY`), auto-chiffrement posé par `mc encrypt set sse-s3` dans [../init-scripts/minio/setup.sh](../init-scripts/minio/setup.sh). Chiffrement **côté serveur, transparent** pour les clients (boto3, `mc`, DuckDB).
- **Matrice des droits + politique de gouvernance** écrite : [../docs/gouvernance-acces-securite.md](../docs/gouvernance-acces-securite.md) (qui accède à quelles lignes, sous quelles conditions, responsabilités par rôle).
- **Extension `data-engineer` → `archive`** (lecture/écriture) : ce rôle porte le cycle de vie (archivage + réintégration). Extension assumée au-delà de la lettre de l'énoncé (qui ne listait que raw/staging/curated).
- **Comptes différenciés** déjà en place depuis le C19 (3 rôles, policies par bucket).

**Écart assumé.** SSE-S3 ne chiffre que les écritures postérieures à son activation. Idéalement, la règle est posée **avant** toute ingestion ; sur l'instance de démo déjà peuplée, les objets existants n'ont **pas** été régénérés (pour ne pas rejouer le pipeline) — ils restent en clair, tandis qu'une reproduction *from scratch* chiffre l'intégralité (l'init pose la règle avant tout chargement).

**Choix assumé.** Les **logs d'audit** MinIO ne sont pas activés (cible webhook/Kafka requise, surcoût d'infra non retenu) ; documentés en « évolutions possibles ». La **ségrégation par ligne** et **KES** (gestion des clés en production) sont également mentionnés comme évolutions.

**Notions abordées.** Chiffrement au repos **SSE-S3** et rôle d'un **KMS** ; **moindre privilège** et policies IAM par bucket (ARN, actions S3) ; gouvernance des accès (matrice, conditions, responsabilités) ; distinction chiffrement **côté serveur** (transparent) vs côté client.
```

- [ ] **Step 4 — Mettre à jour l'auto-évaluation C21 (§3 du rapport)**

Remplacer la puce actuelle :

```
- **C21 — Sécurité & gouvernance** : *partiellement anticipé*. Les **3 comptes de service** aux droits différenciés par bucket sont déjà en place (réalisés dès le C19) ; restent le chiffrement **SSE-S3**, les **logs d'audit** et la rédaction de la **politique de gouvernance** écrite.
```

par :

```
- **C21 — Sécurité & gouvernance** : *acquis (hors audit)*. **3 comptes** aux droits différenciés par bucket (C19), **chiffrement SSE-S3** au repos sur les 4 buckets (KMS intégré), **matrice des droits + politique de gouvernance** écrite. Les **logs d'audit** ne sont pas activés (choix assumé, documenté en évolution possible), de même que la ségrégation par ligne.
```

- [ ] **Step 5 — Vérifier le vouvoiement**

```bash
grep -rnE "\b(tu|ton|ta|tes|toi)\b" README.md rapport/rapport.md || echo "OK vouvoiement"
```

Expected : `OK vouvoiement`.

> Vérification finale de Task 4 : **arrêt**. Message proposé : `docs(c21) : README + rapport — SSE-S3, matrice des droits, gouvernance`.

---

## Auto-revue (fin de plan)

- **Couverture spec :** SSE-S3 4 buckets (T1), extension data-engineer→archive (T2), matrice + politique écrite (T3), README + rapport (T4). Audit / ségrégation par ligne / KES explicitement **hors périmètre** et documentés « évolutions possibles » (T3, T4). ✅
- **Données existantes non régénérées** : décision reflétée dans la doc (T3) et le rapport (T4). ✅
- **Pas de placeholder** : commandes exactes + sorties attendues ; contenu complet des fichiers JSON/Markdown.
- **Cohérence des noms :** `MINIO_KMS_SECRET_KEY`, buckets `raw/staging/curated/archive`, policy `data-engineer`, doc `docs/gouvernance-acces-securite.md` — constants entre tâches et conformes à la spec.
- **AUCUN commit** dans les étapes ; messages proposés en français, sans mention IA.
