"""Tests unitaires pour les primitives S3 : list_keys, delete_keys, delete_prefix."""
from __future__ import annotations

import io
from typing import TYPE_CHECKING

from datalake.storage import delete_keys, delete_prefix, list_keys

if TYPE_CHECKING:
    from conftest import FakeS3


def _put(client: FakeS3, bucket: str, key: str, data: bytes = b"x") -> None:
    client.put_object(Bucket=bucket, Key=key, Body=io.BytesIO(data))


def test_list_keys_filters_by_prefix(fake_s3: FakeS3) -> None:
    _put(fake_s3, "raw", "p/a")
    _put(fake_s3, "raw", "p/b")
    _put(fake_s3, "raw", "autre/c")
    assert list_keys(fake_s3, "raw", "p/") == ["p/a", "p/b"]


def test_delete_keys_returns_count_and_removes(fake_s3: FakeS3) -> None:
    _put(fake_s3, "raw", "p/a")
    _put(fake_s3, "raw", "p/b")
    assert delete_keys(fake_s3, "raw", ["p/a", "p/b"]) == 2
    assert list_keys(fake_s3, "raw", "p/") == []


def test_delete_prefix_removes_all_under_prefix(fake_s3: FakeS3) -> None:
    _put(fake_s3, "raw", "p/a")
    _put(fake_s3, "raw", "p/sub/b")
    _put(fake_s3, "raw", "garde/c")
    assert delete_prefix(fake_s3, "raw", "p/") == 2
    assert list_keys(fake_s3, "raw", "") == ["garde/c"]
