# Design — Catalogue OpenMetadata & cycle de vie des données (C20, Jour 5)

> Spec issue d'une session de brainstorming. Objectif : **cataloguer** le data lake dans OpenMetadata (fiches par ligne + lignage `raw → staging → curated → archive` via les DAGs) et définir le **cycle de vie** (archivage à 180 j, suppression à 2 ans). Contrat data de référence : [docs/architecture.md](../../architecture.md). Énoncé : [ennonce.md](../../ennonce.md) (Jour 5, C20).

## 1. Contexte & objectif

Le pipeline `raw → staging → curated` est en place et alimenté (DAGs actifs). Le C20 ajoute la **gouvernance documentaire** (catalogue OpenMetadata) et le **cycle de vie** (archivage puis suppression). OpenMetadata (server + ingestion, 1.5.6) et les 4 buckets tournent déjà dans la stack.

Exigences de l'énoncé (C20) : connecter OpenMetadata à MinIO ; **une fiche par ligne de production** (description, propriétaire, source, fréquence) ; **documenter les colonnes clés** (unités, plages, sens de `label`) ; **règles ILM** (archivage 180 j, suppression 2 ans) **configurées et documentées**.

## 2. Périmètre

**Dans le périmètre :**
- Connexion **OpenMetadata ↔ MinIO** + ingestion des conteneurs des **3 couches** (`raw`, `staging`, `curated`) et de `archive`.
- **5 fiches par ligne** (sur `raw`, les 5 CSV sources) enrichies ; `staging`/`curated` documentés (schéma harmonisé).
- **Pipelines** : les DAGs Airflow catalogués comme entités *Pipeline*, portant le **lignage `raw → staging → curated → archive`**.
- **Cycle de vie** : **DAG d'archivage** (`raw → archive/` + **purge** de `staging`/`curated` de la même période, selon la **date des données**) + **règle ILM d'expiration** sur `archive/` (suppression selon l'**âge des objets**).
- **Documentation** : politique de cycle de vie, correction de `architecture.md` §8, README, rapport ; **captures** OpenMetadata (livrable C20).

**Hors périmètre :** C21 (SSE-S3, audit, matrice de gouvernance écrite) ; toute modification du pipeline `raw/staging/curated` existant.

## 3. Méthode : hybride (scriptée + captures)

On **scripte** ce qui est reproductible (connexion OM, ingestion, enrichissement, lignage, règle ILM, DAG d'archivage) — fidèle au principe « relançable par un tiers ». L'**UI OpenMetadata** sert à **visualiser et capturer** (les captures sont un livrable C20). Aucun clic manuel n'est un prérequis à la reproduction.

> **Non-invasivité.** Le catalogue OpenMetadata est **purement observationnel** : OM **lit** MinIO (métadonnées, en lecture seule) et la **liste** des DAGs, et écrit ses fiches dans **sa propre base** (Postgres `openmetadata` + Elasticsearch). Il **ne modifie ni les données, ni les buckets, ni le comportement des DAGs**. **Seule** touche au code existant : l'ajout d'annotations de lignage (`inlets`/`outlets`) sur les tâches des DAGs — **métadonnées pures, aucun effet sur l'exécution**, couvert par le contrôle DagBag. La seule partie du C20 qui agit sur les buckets est le **cycle de vie** (§5), via un **nouveau** DAG `archivage` + une règle ILM — les 3 DAGs existants gardent leur logique intacte.

## 4. Catalogue OpenMetadata

### 4.1 Connexion & ingestion
- **Storage Service S3** pointant sur MinIO (`http://minio:9000`, `URL_STYLE=path`, creds root) — déclaré via l'**API/SDK OpenMetadata** (`openmetadata-ingestion`) ou une spec d'ingestion, **pas** de saisie manuelle de la connexion.
- L'**ingestion de métadonnées** crée les entités **Container** (bucket → conteneurs par préfixe/objet) pour `raw`, `staging`, `curated`, `archive`. Le schéma des fichiers structurés (CSV `raw`, Parquet `staging`/`curated`) est inféré (un **manifest** OpenMetadata dans le bucket peut être nécessaire pour typer les colonnes — à valider sur l'instance réelle).

### 4.2 Fiches (une par ligne de production)
- **5 fiches sur `raw`** (les fichiers `LineA…E.csv`), chacune enrichie de : **description**, **propriétaire** (rôle « responsable maintenance », modélisé en *Team*/*User* OpenMetadata), **source** (Zenodo — record 15277168), **fréquence de collecte** (1 relevé/minute).
- **Colonnes clés documentées** (sur les fiches et/ou le schéma harmonisé) : `temperature` **°C**, `pressure` **bar**, `elapsed_time` (temps de fonctionnement, unité arbitraire, **nullable**), `label` **0 = nominal / 1 = anomalie**, `timestamp` ISO 8601 (UTC supposé).
- `staging` et `curated` : conteneurs documentés (schéma cible §6) pour porter le lignage et la vue « données propres ».

### 4.3 Pipelines & lignage
- Les **DAGs** `ingestion_raw`, `harmonisation_staging`, `consolidation_curated`, **`archivage`** sont catalogués comme entités **Pipeline** (connecteur Airflow d'OpenMetadata vers l'**Airflow métier** `:8080`).
- Le **lignage au niveau dataset** est **déclaré via `inlets`/`outlets`** sur les tâches des DAGs (lus par le connecteur Airflow d'OpenMetadata, qui construit le lignage à l'ingestion). Ce sont des **annotations de métadonnées** — **aucun changement de comportement** des DAGs (validé par le contrôle DagBag). Il n'est de toute façon pas auto-déduit pour des `PythonOperator` S3→S3. Graphe cible :
  ```
  raw ─[harmonisation_staging]→ staging ─[consolidation_curated]→ curated
  raw ─[archivage]→ archive
  ```
- `archive/` **entre ainsi dans le lignage** catalogué : il dérive de **`raw`** (on archive la source, cf. §5.1), branche parallèle au flux analytique.

### 4.4 Enrichissement
Descriptions, propriétaires, tags, documentation des colonnes et arêtes de lignage sont posés **par script** (SDK/REST OpenMetadata) → **reproductible**. L'UI sert à vérifier + **capturer**.

## 5. Cycle de vie des données

### 5.1 Archivage — DAG de déplacement (par date des **données**)
MinIO ne sait **pas** archiver localement via ILM (la *transition* exige un **tier distant** ; aucune action ne déplace vers un bucket voisin — [doc MinIO](https://docs.min.io/enterprise/aistor-object-store/reference/cli/mc-ilm-rule/)). L'archivage est donc réalisé par un **DAG dédié**, ce qui est **explicitement documenté**.

On **archive `raw`** (la **source de vérité**, byte-identique) et on **purge** les couches dérivées `staging`/`curated` de la même période. Motivation : `raw` suffit à tout reconstruire → **réintégration triviale** (remettre l'objet dans `raw` relance le filigrane, qui recalcule `staging` puis `curated`). C'est le mécanisme d'auto-réparation déjà en place, appliqué « en avant ».

Nouveau module **`datalake/archive.py`** (logique métier, testée en pur Python) :
- `mois_a_archiver(client, reference, anciennete_mois) -> list[(line, year, month)]` : les `(ligne, mois)` présents dans **`raw`** dont **le mois est antérieur ou égal** à `reference − anciennete_mois` (date des **données**, déduite du chemin `year=/month=`).
- `archive_month(client, line, year, month) -> Result` : **déplace** l'objet `raw` de cette `(ligne, mois)` vers `archive/` en **miroir du chemin `raw`** (`archive/production_lines/lineX/year=/month=/<fichier>.csv`) — copie serveur `copy_object` + suppression de la source ; puis **`delete_prefix` de `staging` ET `curated`** pour la même `(ligne, mois)` (purge des dérivés). **Idempotent**.
- `main(reference=maintenant, anciennete_mois=18) -> int` : `runner.run(archive_month, mois_a_archiver(...), "Archivage raw → archive/")`. Lançable en CLI (`python -m datalake.archive`).

**Réintégration** (documentée) : copier `archive/production_lines/lineX/…` **de retour dans `raw/`** → le DAG d'harmonisation voit le jour présent en `raw` mais absent de `staging` (filigrane) et **recalcule** `staging` puis `curated`. Aucune action manuelle sur les couches dérivées.

**DAG `archivage`** (coquille fine) : mappe `archive_month` sur `mois_a_archiver(...)` via `checked` ; `schedule` quotidien, `catchup=False`.

**Seuil & granularité :**
- Décision **au niveau `(ligne, mois)`** (uniforme entre `raw` mensuel et `staging`/`curated` journalier).
- **Politique documentée = 180 jours** (énoncé). **La démonstration** est jouée avec **`anciennete_mois = 18`** : à la date de démo (mi-2026), cela n'archive **que janvier 2025** (lineE), gardant l'illustration minimale — alors qu'un seuil de 180 j, appliqué à la date des **données** en 2026, archiverait **tout** 2025. Le seuil est un **paramètre** ; cet écart est **explicité** dans la doc.

### 5.2 Suppression — règle ILM d'expiration (par **âge des objets**)
- Règle **ILM native** sur `archive/` : **expiration à 730 jours** (~24 mois), posée par `mc ilm rule add` — **scriptée** (dans `init-scripts/minio/setup.sh` ou un script dédié) et vérifiable (`mc ilm rule ls`).
- Elle se fonde sur l'**âge des objets** (date d'upload). Nos objets datant de **2026**, la règle **ne se déclenchera pas** avant ~2 ans : **configurée mais non démontrable ici** — **explicité** dans la doc.
- (Option documentée : la même règle peut servir de garde-fou de rétention sur les couches actives ; non retenue par défaut, l'archivage gérant déjà la sortie des données anciennes.)

## 6. Fichiers touchés

| Fichier | Rôle | Action |
|---|---|---|
| `datalake/archive.py` | archive `raw` `(ligne, mois)` anciennes → `archive/` + purge `staging`/`curated` (métier) | Créer |
| `dags/archivage.py` | DAG coquille fine (archivage) | Créer |
| `tests/test_archive.py` | tests pur Python (FakeS3) | Créer |
| `init-scripts/minio/setup.sh` | + règle ILM d'expiration sur `archive/` | Modifier |
| `init-scripts/openmetadata/` | specs d'ingestion + script d'enrichissement OpenMetadata (API/SDK REST, exécuté contre l'instance OM) | Créer |
| `dags/*.py` (les 4 DAGs) | + annotations `inlets`/`outlets` pour le lignage OM (métadonnées uniquement, aucune logique modifiée) | Modifier |
| `docs/architecture.md` §8 | corriger : archivage **par DAG** (pas ILM), suppression **par ILM** | Modifier |
| `docs/` (gouvernance/ILM) | politique de cycle de vie + rationale ILM | Créer |
| `README.md`, `rapport/rapport.md` | catalogue + cycle de vie ; entrée C20 | Modifier |

## 7. Tests & validation

- **Pur Python (venv, FakeS3)** : `mois_a_archiver` (sélection par date de donnée, seuil, bord de mois), `archive_month` (déplacement `copy_object`+delete de l'objet `raw` vers `archive/` en miroir **+ purge `staging`/`curated`**, idempotence), et un test de **réintégration** (remettre dans `raw` → jour redevient « à traiter » pour le filigrane). Le faux client S3 devra fournir `copy_object` (ajout à `FakeS3`).
- **ILM** : vérification `mc ilm rule ls local/archive` (règle présente).
- **OpenMetadata** : validation via l'**API** (les entités Container/Pipeline et les arêtes de lignage existent) + **captures** de l'UI (livrable).
- **DAG** : contrôle d'intégrité **DagBag** (`airflow dags list-import-errors`).

## 8. Phasage du plan

1. **Phase 1 — Catalogue OpenMetadata** : connexion OM↔MinIO, ingestion des conteneurs, enrichissement des 5 fiches + colonnes, pipelines + lignage `raw→staging→curated(→archive)`, captures. *(Phase à plus fort risque d'inconnues — connecteur S3 et lignage sur OM 1.5.6 ; on valide contre l'instance réelle.)*
2. **Phase 2 — Cycle de vie** : module `archive.py` (TDD) + DAG `archivage_curated` + règle ILM d'expiration + doc de politique + correction `architecture.md` §8.

Chaque phase est livrable indépendamment.

## 9. Conformité (énoncé C20)

- « Connecter OpenMetadata à MinIO » ✅ (§4.1).
- « Fiches par ligne (description, propriétaire, source, fréquence) » ✅ (§4.2, 5 fiches).
- « Documenter les colonnes clés (unités, plages, `label`) » ✅ (§4.2).
- « Règles ILM (archivage 180 j, suppression 2 ans) **configurées et documentées** » : **suppression** = ILM expiration réellement configurée (§5.2) ; **archivage** = réalisé par DAG faute d'ILM local, **avec justification documentée** (§5.1). La politique (180 j / 2 ans) est **écrite** ; l'implémentation et ses écarts (démo 18 mois, expiration non déclenchable) sont **explicités**.

## 10. Hypothèses & risques

- **OpenMetadata 1.5.6** : le connecteur S3/Storage (et l'éventuel *manifest* pour typer les colonnes) et l'ingestion des **Pipelines Airflow** + lignage sont **à valider sur l'instance réelle** ; possibles ajustements de version. C'est le point le plus incertain (Phase 1).
- **Lignage** non auto-déduit sur stockage objet → **déclaré** (inlets/outlets ou API).
- **Ancrage temporel** : archivage par **date de donnée** (démontrable, seuil démo 18 mois) ; expiration ILM par **âge d'objet** (non démontrable ici) — assumé et documenté.
- **`copy_object`** doit être ajouté au faux client `FakeS3` pour tester l'archivage.
- **ILM** : `mc ilm rule add` — syntaxe exacte selon la version de `mc`, à figer au plan.
