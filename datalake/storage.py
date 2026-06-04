"""Accès au stockage objet MinIO (S3) : factory client + utilitaires."""
from __future__ import annotations

import hashlib
from pathlib import Path

import boto3
from botocore.client import BaseClient, Config

from .config import get_minio_settings


def get_s3_client() -> BaseClient:
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


def list_keys(client: BaseClient, bucket: str, prefix: str) -> list[str]:
    """Toutes les clés d'objets sous `prefix` (pagination gérée)."""
    keys, token = [], None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        keys.extend(obj["Key"] for obj in resp.get("Contents", []))
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            return keys


def delete_keys(client: BaseClient, bucket: str, keys: list[str]) -> int:
    """Supprime les clés données (par lots de 1000). Retourne le nombre supprimé."""
    total = 0
    for i in range(0, len(keys), 1000):
        batch = keys[i : i + 1000]
        if batch:
            client.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch]})
            total += len(batch)
    return total


def delete_prefix(client: BaseClient, bucket: str, prefix: str) -> int:
    """Supprime tous les objets sous `prefix`. Retourne le nombre supprimé."""
    return delete_keys(client, bucket, list_keys(client, bucket, prefix))
