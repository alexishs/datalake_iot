"""Configuration centralisée, lue depuis l'environnement.

Aucun nom d'hôte n'est défini dans .env : `MINIO_ENDPOINT` a pour défaut
`http://minio:9000` (le service Docker), valeur valable aussi bien dans le
conteneur dev que dans Airflow. Seuls les secrets viennent de l'environnement.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# Charge .env s'il est présent (utile hors Docker). override=False par défaut :
# une variable déjà posée par Docker Compose n'est JAMAIS écrasée.
load_dotenv()


@dataclass(frozen=True)
class MinioSettings:
    endpoint: str
    access_key: str
    secret_key: str


def get_minio_settings() -> MinioSettings:
    """Construit les paramètres de connexion MinIO depuis l'environnement."""
    return MinioSettings(
        endpoint=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    )
