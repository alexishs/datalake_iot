from __future__ import annotations

import hashlib
from pathlib import Path

from datalake import download
from datalake.runner import Result


def test_download_one_skip_when_present(tmp_path: Path) -> None:
    content = b"timestamp,label\n2025-01-01 00:00:00,0\n"
    (tmp_path / "f.csv").write_bytes(content)
    meta = {
        "key": "f.csv",
        "md5": hashlib.md5(content).hexdigest(),
        "url": "http://invalid.invalid/should-not-be-called",
        "size": len(content),
    }
    res = download.download_one(meta, tmp_path)
    assert isinstance(res, Result)
    assert res.ok and "déjà présent" in res.statut
