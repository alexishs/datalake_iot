"""Outils de test partagés : faux client S3 en mémoire (aucun MinIO requis)."""
import hashlib

import pytest
from botocore.exceptions import ClientError


class FakeS3:
    """Client S3 minimal en mémoire : put / head / list / delete.

    Imite le strict nécessaire de l'API boto3 utilisée par le code :
    - put_object renvoie un ETag = MD5 du contenu (comme un upload simple MinIO) ;
    - head_object lève botocore ClientError si la clé est absente ;
    - list_objects_v2 filtre par préfixe (sans pagination) ;
    - delete_objects supprime les clés données.
    """

    def __init__(self):
        self.store: dict[tuple[str, str], bytes] = {}

    def put_object(self, Bucket, Key, Body):
        data = Body.read() if hasattr(Body, "read") else Body
        self.store[(Bucket, Key)] = data
        return {"ETag": '"' + hashlib.md5(data).hexdigest() + '"'}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.store:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"ETag": '"' + hashlib.md5(self.store[(Bucket, Key)]).hexdigest() + '"'}

    def list_objects_v2(self, Bucket, Prefix="", **kwargs):
        keys = sorted(k for (b, k) in self.store if b == Bucket and k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def delete_objects(self, Bucket, Delete):
        for obj in Delete["Objects"]:
            self.store.pop((Bucket, obj["Key"]), None)
        return {"Deleted": [{"Key": o["Key"]} for o in Delete["Objects"]]}


@pytest.fixture
def fake_s3():
    return FakeS3()
