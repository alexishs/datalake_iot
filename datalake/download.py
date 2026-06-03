"""Téléchargement des données sources depuis Zenodo (Jour 1, C18).

Interroge l'API REST de Zenodo pour lister les fichiers du dépôt, les télécharge
dans `data/` et vérifie leur intégrité via le MD5 fourni par Zenodo. Idempotent :
un fichier déjà présent avec le bon MD5 n'est pas re-téléchargé.

Lancement :  python -m datalake.download
Options   :  --record-id ID   --dest DOSSIER   --csv-only
"""
from __future__ import annotations

import argparse
import json
import urllib.parse
import urllib.request
from pathlib import Path

from datalake.storage import md5_file

ZENODO_RECORD_API = "https://zenodo.org/api/records/{record_id}"
ZENODO_FILE_URL = "https://zenodo.org/api/records/{record_id}/files/{key}/content"
DEFAULT_RECORD_ID = "15277168"
DEFAULT_DEST = Path("data")
CHUNK = 1 << 16  # 64 Kio


def human(size: int) -> str:
    s = float(size)
    for unit in ("o", "Ko", "Mo", "Go"):
        if s < 1024 or unit == "Go":
            return f"{s:.0f} {unit}"
        s /= 1024
    return f"{s:.0f} Go"


def list_files(record_id: str) -> list[dict]:
    """Retourne la liste des fichiers du dépôt : key, url, md5, size."""
    url = ZENODO_RECORD_API.format(record_id=record_id)
    req = urllib.request.Request(url, headers={"User-Agent": "datalake-iot/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        record = json.load(resp)
    files = []
    for f in record.get("files", []):
        key = f["key"]
        files.append({
            "key": key,
            "url": ZENODO_FILE_URL.format(
                record_id=record_id, key=urllib.parse.quote(key)
            ),
            "md5": f["checksum"].split(":", 1)[-1],  # "md5:abc" -> "abc"
            "size": int(f.get("size", 0)),
        })
    return files


def download_one(meta: dict, dest: Path) -> str:
    """Télécharge un fichier (avec vérif MD5). Retourne le statut : ok|skip|error."""
    target = dest / meta["key"]

    # Idempotence : déjà là et intègre -> on saute
    if target.exists() and md5_file(target) == meta["md5"]:
        print(f"  {meta['key']:<42} {human(meta['size']):>8}  = déjà présent (MD5 ok)")
        return "skip"

    tmp = target.with_suffix(target.suffix + ".part")
    req = urllib.request.Request(meta["url"], headers={"User-Agent": "datalake-iot/1.0"})
    with urllib.request.urlopen(req, timeout=120) as resp, open(tmp, "wb") as out:
        while chunk := resp.read(CHUNK):
            out.write(chunk)

    got = md5_file(tmp)
    if got != meta["md5"]:
        tmp.unlink(missing_ok=True)
        print(f"  {meta['key']:<42} {human(meta['size']):>8}  ✗ MD5 INVALIDE "
              f"(attendu {meta['md5']}, obtenu {got})")
        return "error"

    tmp.replace(target)
    print(f"  {meta['key']:<42} {human(meta['size']):>8}  ✓ MD5 ok")
    return "ok"


def main() -> int:
    parser = argparse.ArgumentParser(description="Télécharge les sources depuis Zenodo.")
    parser.add_argument("--record-id", default=DEFAULT_RECORD_ID)
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--csv-only", action="store_true",
                        help="ne télécharger que les fichiers .csv (ignore les PDF)")
    args = parser.parse_args()

    args.dest.mkdir(parents=True, exist_ok=True)
    files = list_files(args.record_id)
    if args.csv_only:
        files = [f for f in files if f["key"].lower().endswith(".csv")]

    print(f"Dépôt Zenodo {args.record_id} — {len(files)} fichier(s) vers {args.dest}/")
    stats = {"ok": 0, "skip": 0, "error": 0}
    for meta in sorted(files, key=lambda f: f["key"]):
        stats[download_one(meta, args.dest)] += 1

    print(f"→ {stats['ok']} téléchargé(s), {stats['skip']} déjà présent(s), "
          f"{stats['error']} erreur(s).")
    return 1 if stats["error"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
