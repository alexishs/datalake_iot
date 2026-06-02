"""Accès au stockage objet MinIO (S3) : factory client + utilitaires."""
from __future__ import annotations

import hashlib
from pathlib import Path

import boto3
from botocore.client import Config

from .config import get_minio_settings


def get_s3_client():
    """Retourne un client boto3 configuré pour MinIO (signature S3v4)."""
    s = get_minio_settings()
    return boto3.client(
        "s3",
        endpoint_url=s.endpoint,
        aws_access_key_id=s.access_key,
        aws_secret_access_key=s.secret_key,
        config=Config(signature_version="s3v4"),
    )


def md5_file(path: str | Path, chunk_size: int = 8192) -> str:
    """Hash MD5 d'un fichier local — vérification d'intégrité à l'upload (C19)."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()
