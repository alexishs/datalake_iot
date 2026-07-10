"""C20 — Enrichissement des fiches du catalogue OpenMetadata (config-as-code).

Complète le catalogue après l'ingestion (run-ingestion.sh) :
  - description du **service** de stockage et des **4 buckets** ;
  - pour chaque conteneur structuré (une fiche par ligne et par couche) :
    description, propriétaire (équipe « Responsable maintenance »),
    documentation des colonnes (unités : °C, bar ; `elapsed_time` nullable ; etc.).

Exécuté DANS le conteneur d'ingestion (SDK `metadata` disponible). Le lanceur
`run-enrichment.sh` fournit l'hôte du serveur et le jeton via l'environnement.
Aucun secret en dur.
"""
from __future__ import annotations

import os
from copy import deepcopy

from metadata.generated.schema.api.teams.createTeam import (
    CreateTeamRequest as CreateTeam,
)
from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.table import Constraint
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    AuthProvider,
    OpenMetadataConnection,
)
from metadata.generated.schema.entity.services.storageService import StorageService
from metadata.generated.schema.entity.teams.team import TeamType
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.type.basic import Markdown
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.type.entityReferenceList import EntityReferenceList
from metadata.ingestion.ometa.ometa_api import OpenMetadata

SERVICE = "datalake_minio"
LINES = ["lineA", "lineB", "lineC", "lineD", "lineE"]

SERVICE_DESCRIPTION = (
    "Data lake des **capteurs IoT industriels** sur MinIO (stockage objet "
    "compatible S3). Organisé en quatre couches — `raw`, `staging`, `curated`, "
    "`archive` — en vue de la maintenance prédictive (5 lignes de production)."
)

# Description des 4 buckets (conteneurs parents).
BUCKETS = {
    "raw": (
        "Couche **raw** — données brutes telles que reçues, copie byte-identique "
        "des CSV source (partition `year=/month=/`). Les schémas **diffèrent d'une "
        "ligne à l'autre** (casse des colonnes, présence/absence d'`elapsed_time`)."
    ),
    "staging": (
        "Couche **staging** — données nettoyées et **harmonisées** (Parquet, schéma "
        "unifié), partitionnées par jour (`year=/month=/day=`)."
    ),
    "curated": (
        "Couche **curated** — données **prêtes à l'analyse** (Parquet), table unifiée "
        "partitionnée `line=/year=/month=/day=`."
    ),
    "archive": (
        "Couche **archive** — données expirées **déplacées depuis `raw`** par le DAG "
        "`archivage` (cycle de vie) ; suppression finale par règle **ILM** "
        "(expiration 730 j)."
    ),
}

# Documentation des colonnes (clé = nom normalisé en minuscules).
COLONNES = {
    "timestamp": "Horodatage du relevé, ISO 8601 (UTC supposé).",
    "temperature": "Température du capteur, en °C.",
    "pressure": "Pression, en bar.",
    "elapsed_time": (
        "Temps de fonctionnement cumulé (unité arbitraire). Absent de certaines "
        "lignes source → colonne nullable après harmonisation."
    ),
    "label": "Étiquette qualité : 0 = fonctionnement nominal, 1 = anomalie.",
    "line": "Ligne de production (partition Hive `line=` de la table curated unifiée).",
    "year": "Partition : année des données (déduite du chemin de stockage).",
    "month": "Partition : mois des données (déduit du chemin de stockage).",
    "day": "Partition : jour des données (couches staging/curated).",
}

# Contrainte de nullité des colonnes (clé = nom normalisé). Politique :
#   - `elapsed_time` NULLABLE : structurellement absent de certaines lignes
#     (100 % null en staging/curated pour C/D/E) → rempli à null à l'harmonisation ;
#   - `timestamp`/`line`/`year`/`month`/`day` NOT_NULL : garantis par construction
#     (timestamp parsé, partitions dérivées) ;
#   - `temperature`/`pressure`/`label` : sans contrainte (non-nulls *observés* mais
#     dépendant de la source — on ne sur-promet pas).
CONTRAINTES = {
    "elapsed_time": Constraint.NULL,
    "timestamp": Constraint.NOT_NULL,
    "line": Constraint.NOT_NULL,
    "year": Constraint.NOT_NULL,
    "month": Constraint.NOT_NULL,
    "day": Constraint.NOT_NULL,
}


def description_raw(line: str) -> str:
    """Description markdown d'une fiche `raw` (une ligne de production)."""
    return (
        f"Données **brutes** de capteurs — **{line}**, telles que reçues "
        "(copie byte-identique du CSV source, aucune transformation).\n\n"
        "- **Source** : *Synthetic Data from Industrial Sensor Monitoring* — "
        "Zenodo, record [15277168](https://zenodo.org/records/15277168).\n"
        "- **Fréquence de collecte** : 1 relevé/minute.\n"
        f"- **Partitionnement** : `production_lines/{line}/year=YYYY/month=MM/`.\n"
        "- **Champ `label`** : 0 = nominal, 1 = anomalie.\n\n"
        "> Couche `raw` : le schéma reflète l'**hétérogénéité des sources** "
        "(casse des colonnes, présence/absence d'`elapsed_time`). "
        "L'harmonisation intervient en couche `staging`."
    )


def _fqn(bucket: str, path: str) -> str:
    return f"{SERVICE}.{bucket}.production_lines/{path}"


def structured_targets() -> list[tuple[str, str]]:
    """(fqn, description) de chaque conteneur structuré à enrichir."""
    targets: list[tuple[str, str]] = []
    for line in LINES:
        targets.append((_fqn("raw", line), description_raw(line)))
        targets.append((
            _fqn("staging", line),
            f"Données **harmonisées** de **{line}** (schéma unifié). "
            f"Produites à partir de `raw.{line}` par le DAG `harmonisation_staging`.",
        ))
        targets.append((
            _fqn("curated", f"line={line}"),
            f"Données **prêtes à l'analyse** de **{line}**. "
            f"Produites à partir de `staging.{line}` par le DAG `consolidation_curated`.",
        ))
    targets.append((
        _fqn("archive", "lineE"),
        "Copie **archivée** de **lineE** — déplacée de `raw` par le DAG `archivage` "
        "(cycle de vie).",
    ))
    return targets


def enrich_container(
    metadata: OpenMetadata,
    fqn: str,
    description: str,
    owners: EntityReferenceList,
    document_columns: bool,
) -> bool:
    """Applique description + propriétaire (+ colonnes) à un conteneur. Retourne False si absent."""
    container = metadata.get_by_name(
        entity=Container, fqn=fqn, fields=["dataModel", "owners"]
    )
    if container is None:
        print(f"  ⚠ conteneur introuvable : {fqn}")
        return False

    metadata.patch_description(
        entity=Container, source=container, description=description, force=True
    )
    metadata.patch_owner(entity=Container, source=container, owners=owners, force=True)

    touched = 0
    if document_columns and container.dataModel and container.dataModel.columns:
        # Container n'a pas de patch_column_description dédié (réservé aux Table) :
        # on modifie une copie puis on laisse le patch générique calculer le diff.
        destination = deepcopy(container)
        changed = False
        for col in destination.dataModel.columns:
            name = col.name.root.lower()
            contrainte = CONTRAINTES.get(name)
            doc = COLONNES.get(name)
            if doc:
                # L'UI OpenMetadata n'affiche pas la colonne `constraint` pour les
                # Containers (réservée aux Table) : on porte aussi la nullité dans
                # la description, seule colonne visible à l'écran.
                marque = {
                    Constraint.NOT_NULL: " *(non null)*",
                    Constraint.NULL: " *(nullable)*",
                }.get(contrainte, "")
                col.description = Markdown(doc + marque)
                touched += 1
                changed = True
            if contrainte is not None:
                col.constraint = contrainte
                changed = True
        if changed:
            metadata.patch(entity=Container, source=container, destination=destination)
    print(f"  ✓ {fqn.split('.', 1)[1]} : description + propriétaire"
          + (f" + {touched} colonne(s)" if touched else ""))
    return True


def main() -> int:
    server = OpenMetadataConnection(
        hostPort=os.environ["OPENMETADATA_HOST_PORT"],
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(
            jwtToken=os.environ["OPENMETADATA_JWT_TOKEN"],
        ),
    )
    metadata = OpenMetadata(server)
    if not metadata.health_check():
        print("ERREUR : serveur OpenMetadata injoignable.")
        return 1

    # 1. Propriétaire : équipe « Responsable maintenance ».
    team = metadata.create_or_update(
        CreateTeam(
            name="responsable-maintenance",
            displayName="Responsable maintenance",
            description="Équipe responsable des données de maintenance prédictive.",
            teamType=TeamType.Group,
        )
    )
    owners = EntityReferenceList(root=[EntityReference(id=team.id, type="team")])
    print(f"Propriétaire : équipe '{team.displayName}' ({team.id.root}).")

    # 2. Description du service de stockage.
    service = metadata.get_by_name(entity=StorageService, fqn=SERVICE)
    if service is not None:
        metadata.patch_description(
            entity=StorageService, source=service, description=SERVICE_DESCRIPTION, force=True
        )
        print(f"Service '{SERVICE}' : description posée.")

    # 3. Description + propriétaire des 4 buckets (conteneurs parents, sans colonnes).
    print("Buckets :")
    for bucket, desc in BUCKETS.items():
        enrich_container(metadata, f"{SERVICE}.{bucket}", desc, owners, document_columns=False)

    # 4. Fiches structurées (une par ligne et par couche) : description + propriétaire + colonnes.
    print("Fiches structurées :")
    for fqn, desc in structured_targets():
        enrich_container(metadata, fqn, desc, owners, document_columns=True)

    print("Enrichissement terminé.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
