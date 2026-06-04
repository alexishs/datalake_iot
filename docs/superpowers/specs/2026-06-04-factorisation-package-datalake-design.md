# Design — Factorisation du package `datalake/` : ingestion vers `raw/` (C19)

> Spec issue d'une session de brainstorming. Objectif : structurer le package pour que le **lancement manuel** et les **DAGs** partagent le même code métier, en commençant par l'**ingestion des CSV dans `raw/`** (Jour 2 + DAG des Jours 3-4). Contrat technique de référence : [docs/architecture.md](../../architecture.md) §12.

## 1. Contexte & objectif

Le package `datalake/` contient la logique métier du data lake ; les DAGs Airflow n'en sont que des appelants (« coquilles fines », cf. CLAUDE.md). Aujourd'hui `download.py` (acquisition Zenodo → `data/`) possède sa propre boucle/rapport, et l'ingestion vers `raw/` reste à écrire. On veut éviter de dupliquer la mécanique et garantir que **manuel et DAG exécutent strictement la même fonction métier**.

Objectifs (par priorité) : **(a)** manuel == DAG (même fonction métier appelée des deux côtés) ; **(b)** anti-duplication de la mécanique (boucle, rapport, idempotence, erreurs) ; **(c)** testabilité en pur Python (hors Airflow).

## 2. Périmètre

**Dans le périmètre :** un `runner` partagé, le module d'`ingestion` (data → raw) avec son **idempotence (MD5)** et sa **cascade d'invalidation vers `staging`**, une primitive S3 `delete_prefix`, et le réalignement de `download.py` sur le `runner`.

**Hors périmètre :** les DAGs eux-mêmes (Jours 3-4), l'harmonisation (`staging/`) et la consolidation (`curated/`) — conçus plus tard, mais le design doit leur être compatible. Les policies d'accès détaillées (C21).

## 3. Architecture du package

```
datalake/
  config.py        socle (existant) — configuration MinIO via env
  storage.py       socle (existant + delete_prefix) — get_s3_client(), md5_file(), delete_prefix()
  explore.py       analyse Jour 1 (existant) — line_id(), csv_paths(), TS_FORMAT
  runner.py        NOUVEAU — Result + run(action, items, titre) : boucle, rapport, code de sortie
  download.py      REFACTORÉ — acquisition Zenodo → data/ (utilise runner) ; MANUEL uniquement
  ingestion.py     NOUVEAU — data/ → raw/ (upload + MD5 + partition) ; MANUEL + DAG
```

Distinction nette entre **acquisition** (`download` : Internet → `data/`, manuel, pas de DAG) et **ingestion** (`ingestion` : `data/` → `raw/`, manuel + DAG). Elles ne partagent que les **briques bas niveau** (`storage`, `runner`) — aucune logique métier commune.

## 4. Composants

### 4.1 `runner.py` (mutualise les CLI — objectif b)

Contrat de résultat uniforme renvoyé par chaque action :

```python
@dataclass
class Result:
    label: str    # ce qui est traité (nom de fichier)
    statut: str   # texte court : "déposé", "inchangé (MD5)", "ré-importé", "ÉCHEC MD5"…
    ok: bool      # succès
```

```python
def run(action, items, titre) -> int
```

- parcourt `items`, appelle `action(item) -> Result` ;
- **capture les exceptions par item** → `Result(label, "ERREUR : <msg>", ok=False)` (un item en échec n'interrompt pas les autres) ;
- affiche un **rapport uniforme** : une ligne `✓/✗ <label> — <statut>` par item, puis un résumé `N OK, M échec(s)` ;
- renvoie le **code de sortie** : `0` si tout OK, `1` sinon.

### 4.2 `storage.py` — ajout `delete_prefix`

```python
def delete_prefix(client, bucket: str, prefix: str) -> int
```

Liste (`list_objects_v2`, paginé) puis supprime (`delete_objects`, par lots) tous les objets sous `prefix`. Retourne le nombre d'objets supprimés. Primitive réutilisée par l'ingestion **et** les futures étapes (remplacement de partition par jour en `staging`/`curated`).

### 4.3 `ingestion.py` — cœur, manuel == DAG (objectif a)

**`ingest_file(path, client=None) -> Result`** — unité métier partagée (appelée par le CLI manuel **et** par le DAG d'ingestion) :

1. charge le `timestamp` (Polars) ; dérive `line` (`explore.line_id`), `year`, `month` ; **garde-fou impératif** : si le fichier couvre plus d'un `(year, month)`, lève `ValueError` (cf. §12, règle 12) ;
2. calcule `prefix = production_lines/{line}/year={YYYY}/month={MM}/` et `key = prefix + <fichier>.csv` ;
3. `local_md5 = md5_file(path)` ; lit l'`ETag` de l'objet `key` dans `raw` s'il existe ;
4. **décision fondée sur le MD5** :
   - `ETag == local_md5` → **skip** → `Result(fichier, "inchangé (MD5)", True)` ;
   - sinon (absent ou différent) → **(ré)import**, dans un **ordre sûr** (on n'invalide l'aval qu'une fois `raw` confirmé) : (a) `put_object("raw", key, octets du fichier)` puis **vérifier `ETag == local_md5`** — si échec → `Result(fichier, "ÉCHEC MD5", False)` **sans toucher à `staging`** ; (b) **nettoyer la partition `raw`** : supprimer les éventuels **autres** objets sous `prefix` (≠ `key`, cas d'un fichier renommé), l'objet courant reste ; (c) **invalider l'aval** : `delete_prefix(client, "staging", prefix)` **seulement après** confirmation de `raw` → `Result(fichier, "ré-importé", True)`.

Le `prefix` est identique pour les deux couches (`production_lines/{line}/year={YYYY}/month={MM}/`) ; en `staging`, il englobe **tous les `day=DD/`** du mois (qui en dérivent). On ne touche à `staging` **que** sur la branche (ré)import — un `skip` (MD5 identique) laisse `staging` intact. **Cascade d'invalidation :** remplacer une `(ligne, mois)` en `raw` supprime ses données dérivées en `staging`, ce qui force leur recalcul par le DAG aval (cf. §10, filigrane).

Le dépôt est **byte-identique** (on envoie les octets du fichier tels quels) ; l'`ETag` d'un `put_object` simple vaut le MD5 → vérification directe.

**`main() -> int`** crée un **client S3 unique** et le lie : `client = get_s3_client(); return runner.run(lambda p: ingest_file(p, client), explore.csv_paths(), "Ingestion → raw/")`. Lançable : `python -m datalake.ingestion` (dans le conteneur dev).

**DAG (Jours 3-4, hors périmètre ici)** : `PythonOperator(python_callable=ingest_file, op_args=[chemin])` en *task mapping* sur les CSV → **exactement la même fonction** que le CLI.

### 4.4 `download.py` — réaligné sur le `runner`

On conserve `list_files(record_id)` et `download_one(meta, dest)` (unité par fichier), mais : `download_one` renvoie désormais un `Result`, et `main()` lie l'argument `dest` pour obtenir une action à un seul argument, p. ex. `runner.run(lambda meta: download_one(meta, dest), files, "Téléchargement Zenodo → data/")`. La boucle/rapport maison disparaît. Idempotence inchangée (skip si le fichier local existe avec le MD5 attendu de Zenodo).

> Convention `runner` : l'`action` est un **callable à un seul argument** (`item -> Result`). Les paramètres supplémentaires (`dest`, client S3 partagé…) sont liés côté `main()` via une `lambda`/`functools.partial`. `ingest_file(path, client=None)` est directement utilisable comme action (le `client` par défaut est créé au besoin) ; pour réutiliser un client unique, le lier de même.

## 5. Flux & idempotence (re-run sans doublon)

- **Manuel** : `python -m datalake.ingestion` traite les 5 CSV via le `runner`.
- **DAG** : une tâche par CSV, appelant `ingest_file`.
- **Idempotence** : contenu identique (MD5) ⇒ skip ; contenu changé/absent ⇒ `raw` **réécrit (clé écrasée) puis nettoyé** des objets périmés de la partition, et `staging` **invalidé ensuite** (ordre sûr) → aucun objet périmé ni dupliqué. Rejouable à volonté.
- **Cascade `raw` → `staging`** : un (ré)import en `raw` **vide aussi la même `(ligne, mois)` en `staging`**. Le DAG `raw → staging` étant piloté par un **filigrane** (dernier jour présent en `staging`), la suppression fait « reculer » ce filigrane et déclenche le **recalcul automatique** des jours concernés — sans signal explicite entre étapes (couplage faible, DAGs indépendants).

## 6. Gestion d'erreurs

- **Garde-fou §12** (fichier multi-mois) : `ValueError` levée par `ingest_file`, **capturée par le `runner`** → item marqué `✗`, exit code `1`, les autres fichiers continuent.
- **Échec d'intégrité** (`ETag != local_md5`) : `Result(ok=False)` → `✗`, exit code `1`.
- **Erreurs réseau/S3** : capturées par le `runner` par item.

## 7. Tests

- `ingest_file` et `download_one` : testables en **pur Python** (client injectable) — y compris le garde-fou (multi-mois → `ValueError`), l'idempotence (skip si MD5 identique) et le remplacement de partition.
- `runner.run` : testable avec une action factice (mélange de `Result` ok/ko et d'exceptions) → vérifier rapport et code de sortie.
- `delete_prefix` : testable contre un client factice ou un MinIO de test.

## 8. Conformité

- **Énoncé Jour 2 (C19)** : upload des 5 CSV dans `raw/…` + intégrité MD5. ✅
- **Contrat §12** : partition au mois, dépôt byte-identique, garde-fou « un seul mois », idempotence. ✅
- **CLAUDE.md** : logique dans `datalake/`, DAG = coquille fine (a) ; **Polars** (pas pandas) ; vouvoiement ; Markdown sans hard-wrap. ✅

## 9. Hypothèses

- Dans `raw`, une partition `…/{line}/year/month/` ne contient **qu'un seul fichier** (un CSV source = une ligne + un mois) — cohérent avec le garde-fou « un seul mois ». Vider la partition avant réécriture est donc sûr.
- L'`ETag` MinIO d'un `put_object` simple (non multipart) vaut le **MD5** de l'objet (vrai pour des fichiers < quelques Mo, ce qui est le cas ici).
- L'ingestion connaît la **convention de partition de `staging`** (`production_lines/{line}/year/month/…`) pour l'invalidation en cascade — couplage **assumé et documenté** (même convention de nommage dans `raw` et `staging`).

## 10. Suite (hors périmètre)

Les modules `harmonization.py` (raw → staging) et `consolidation.py` (staging → curated) suivront le **même patron** : fonction par unité (un jour, fil de l'eau) + `runner` + `delete_prefix` (remplacement de la partition `…/day=DD/`), appelée à l'identique par le CLI et le DAG.

**Contrat du DAG `raw → staging` (filigrane), activé par la cascade ci-dessus :** le DAG **se déclenche toutes les minutes** et traite **une seule journée par exécution** — pour chaque ligne, il lit le **dernier jour présent en `staging`** (le filigrane) et traite le **jour suivant** disponible en `raw` (et **non** la date d'exécution Airflow), ce qui **simule un flux** (une journée ingérée ~chaque minute). Puisque l'ingestion **vide `staging`** sur (ré)import d'une `(ligne, mois)`, le filigrane recule et le DAG **re-traite automatiquement** les jours invalidés depuis le `raw` à jour — sans coordination directe entre étapes.
