# Captures d'écran — catalogue OpenMetadata (C20)

Ces captures illustrent le livrable C20 dans l'UI OpenMetadata (`http://localhost:8585`, `admin@open-metadata.org` / `admin`). Elles sont produites **après** les trois scripts d'ingestion (cf. [init-scripts/openmetadata/README.md](../../init-scripts/openmetadata/README.md)).

| Fichier | Où dans l'UI | Ce que la capture doit montrer |
|---|---|---|
| [`01-service-containers.png`](01-service-containers.png) | **Explore → Storage → `datalake_minio`** (ou Containers) | Le service et ses **4 buckets** + les conteneurs structurés (arborescence). |
| [`02-fiche-raw-lineA.png`](02-fiche-raw-lineA.png) | Conteneur `raw.production_lines/lineA` | **Description** (source Zenodo, fréquence), **propriétaire** *Responsable maintenance*. |
| `03-colonnes-raw-lineA.png` *non fournie car informations présentes dans le fichier précédent.* | Onglet *Schema* de `raw...lineA` | Les **colonnes documentées** avec unités (`temperature` °C, `pressure` bar, `elapsed_time`, `label`). |
| [`04-heterogeneite-schemas.png`](04-heterogeneite-schemas.png) | Comparaison `raw...lineA` vs `raw...lineC` (ou lineB) | L'**hétérogénéité** : `Temperature`/`temperature`, présence/absence d'`elapsed_time`. |
| [`05-schema-harmonise-staging.png`](05-schema-harmonise-staging.png) | Conteneur `staging...lineA` (onglet *Schema*) | Le schéma **harmonisé** (minuscules, `elapsed_time` présent). |
| [`06-pipelines.png`](06-pipelines.png) | **Explore → Pipelines → `datalake_airflow`** | Les **4 pipelines** (`ingestion_raw`, `harmonisation_staging`, `consolidation_curated`, `archivage`). |
| [`07-lignage.png`](07-lignage.png) | Onglet *Lineage* d'un conteneur (p. ex. `raw...lineE`) | Le **graphe de lignage** `raw→staging→curated` et `raw→archive`. |
