import io

import pytest
from botocore.exceptions import ClientError


def test_fake_put_head_list_delete(fake_s3):
    fake_s3.put_object(Bucket="raw", Key="a/x.csv", Body=io.BytesIO(b"hello"))
    # MD5("hello") = 5d41402abc4b2a76b9719d911017c592
    assert fake_s3.head_object(Bucket="raw", Key="a/x.csv")["ETag"].strip('"') == \
        "5d41402abc4b2a76b9719d911017c592"
    keys = [o["Key"] for o in fake_s3.list_objects_v2(Bucket="raw", Prefix="a/")["Contents"]]
    assert keys == ["a/x.csv"]
    fake_s3.delete_objects(Bucket="raw", Delete={"Objects": [{"Key": "a/x.csv"}]})
    with pytest.raises(ClientError):
        fake_s3.head_object(Bucket="raw", Key="a/x.csv")
