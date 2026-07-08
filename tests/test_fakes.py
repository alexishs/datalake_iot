from __future__ import annotations

import io
from typing import TYPE_CHECKING

import pytest
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from conftest import FakeS3


def test_fake_put_head_list_delete(fake_s3: FakeS3) -> None:
    fake_s3.put_object(Bucket="raw", Key="a/x.csv", Body=io.BytesIO(b"hello"))
    # MD5("hello") = 5d41402abc4b2a76b9719d911017c592
    assert fake_s3.head_object(Bucket="raw", Key="a/x.csv")["ETag"].strip('"') == \
        "5d41402abc4b2a76b9719d911017c592"
    keys = [o["Key"] for o in fake_s3.list_objects_v2(Bucket="raw", Prefix="a/")["Contents"]]
    assert keys == ["a/x.csv"]
    fake_s3.delete_objects(Bucket="raw", Delete={"Objects": [{"Key": "a/x.csv"}]})
    with pytest.raises(ClientError):
        fake_s3.head_object(Bucket="raw", Key="a/x.csv")


def test_fake_get_object_renvoie_le_corps(fake_s3: FakeS3) -> None:
    fake_s3.put_object(Bucket="raw", Key="a/x.csv", Body=io.BytesIO(b"coucou"))
    assert fake_s3.get_object(Bucket="raw", Key="a/x.csv")["Body"].read() == b"coucou"


def test_fake_get_object_absent_leve(fake_s3: FakeS3) -> None:
    with pytest.raises(ClientError):
        fake_s3.get_object(Bucket="raw", Key="absent")


def test_fake_copy_object(fake_s3: FakeS3) -> None:
    fake_s3.put_object(Bucket="raw", Key="a/x.csv", Body=io.BytesIO(b"data"))
    fake_s3.copy_object(Bucket="archive", Key="a/x.csv",
                        CopySource={"Bucket": "raw", "Key": "a/x.csv"})
    assert fake_s3.get_object(Bucket="archive", Key="a/x.csv")["Body"].read() == b"data"
