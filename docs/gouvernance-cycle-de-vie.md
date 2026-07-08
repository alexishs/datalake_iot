# Politique de cycle de vie des données (C20)

> Politique de **rétention** des données du data lake : **archivage** des données anciennes, puis **suppression** définitive. Ce document décrit les jalons, les mécanismes retenus et leur justification, la procédure de réintégration, les écarts assumés en démonstration et les commandes de vérification. Références techniques : [architecture.md §8](architecture.md#8-cycle-de-vie-des-données) et le module [../datalake/archive.py](../datalake/archive.py).

## 1. Objectif

Maîtriser le volume et la durée de conservation du lac en organisant le vieillissement de la donnée en **deux temps** : d'abord la sortir des couches actives (`raw`/`staging`/`curated`) vers une couche `archive/` dédiée, puis la supprimer définitivement une fois la durée de rétention atteinte. L'objectif est de conserver les couches actives légères et pertinentes tout en gardant une trace archivée jusqu'à son expiration.

## 2. Jalons de rétention

- **Données actives** : conservées en `raw`/`staging`/`curated` tant qu'elles n'ont pas atteint le seuil d'archivage.
- **Archivage** : au-delà du seuil (politique de l'énoncé : **180 jours**), la donnée est déplacée vers `archive/` et retirée des couches actives.
- **Suppression** : au-delà de **2 ans** (**730 jours**), la donnée archivée est supprimée définitivement.

## 3. Mécanismes

- **Archivage — DAG `archivage`** ([../datalake/archive.py](../datalake/archive.py)) : pour chaque `(ligne, mois)` dont la **date des données** dépasse le seuil, le DAG **copie** l'objet de `raw` vers `archive/` (copie en **miroir** du chemin : `archive/production_lines/lineX/year=YYYY/month=MM/…`), supprime l'objet de `raw`, puis **purge** les partitions correspondantes de `staging` et de `curated`. Le seuil porte sur le **mois des données** (déduit du chemin de partition), non sur la date d'exécution.
- **Suppression — règle ILM d'expiration** : une règle **ILM MinIO** posée sur le bucket `archive/` supprime les objets âgés de plus de **730 jours**. Le seuil porte ici sur l'**âge de l'objet** (sa date d'upload), et non sur la date des données. Elle est configurée dans [../init-scripts/minio/setup.sh](../init-scripts/minio/setup.sh) (`mc ilm rule add local/archive --expire-days 730`).

## 4. Pourquoi deux mécanismes distincts

L'ILM de MinIO ne propose que deux opérations : l'**expiration** (suppression native des objets) et la **transition** vers un **tier de stockage distant** (un autre système). Il n'existe **aucun transfert local de bucket à bucket** : « déplacer `raw/` vers `archive/` dans la même instance MinIO » n'est donc **pas exprimable** en ILM. C'est pourquoi l'archivage local est confié à un **DAG** dédié, tandis que l'ILM reste employé pour la **suppression** — l'expiration étant précisément l'opération de cycle de vie que l'énoncé nomme.

## 5. Réintégration d'une donnée archivée

- Recopier l'objet voulu de `archive/` vers son emplacement d'origine dans `raw/` (le chemin est identique, la copie étant en miroir).
- Aucune action manuelle sur `staging`/`curated` : le **filigrane** du pipeline (cf. [architecture.md §3.2](architecture.md#32-staging--harmonisation)) constate que le jour réapparu en `raw` est absent de `staging`, et le DAG d'harmonisation **recalcule** `staging` puis, par cascade, `curated`.
- La réintégration est ainsi **auto-réparante** et repose sur la même mécanique que l'ingestion courante.

## 6. Écarts assumés (démonstration)

- **Seuil d'archivage** : la **démo** applique **18 mois** au lieu des **180 jours** de la politique. Motif : à ~mi-2026, un seuil de 180 jours appliqué à la **date des données** archiverait **tout l'année 2025** ; le seuil de 18 mois ne retient que **janvier 2025**, ce qui rend la démonstration lisible (une seule `(ligne, mois)` archivée).
- **Suppression** : la règle **730 j / âge d'objet** est **configurée** mais **non déclenchable** en démonstration, car tous les objets ont été uploadés en 2026 (moins de 730 jours d'ancienneté).

## 7. Vérification

- **Archivage** : `airflow dags test archivage 2026-07-08` (ou `python -m datalake.archive`) déplace les `(ligne, mois)` éligibles. Vérifié en réel : `lineE` janvier 2025 se retrouve en `archive/production_lines/lineE/year=2025/month=01/LineE_SmoothRun.csv`, l'objet `raw` correspondant a disparu, et les partitions dérivées `staging`/`curated` de cette `(ligne, mois)` sont purgées.
- **Suppression (ILM)** : `mc ilm rule ls local/archive` affiche la règle d'expiration à **730 jours** sur le bucket `archive/`.
- **Réintégration** : après recopie `archive/ → raw/`, l'exécution suivante du DAG d'harmonisation recrée les partitions `staging` puis `curated` du jour concerné, sans intervention manuelle.
