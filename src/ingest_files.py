import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

RAW_INPUT_DIR = Path("data")  # not data/raw
RAW_FILES_DEST = Path("data/raw/files")
MANIFEST_PATH = RAW_FILES_DEST / "manifest.json"
FILES_TO_INGEST = ["customers.csv", "orders.json", "products.parquet"]

def compute_sha256(file_path: Path) -> str:
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()

def ingest_files():
    RAW_FILES_DEST.mkdir(parents=True, exist_ok=True)
    manifest = {}
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            manifest = json.load(f)

    ingested_hashes = {entry["sha256"] for entry in manifest.values()}

    for filename in FILES_TO_INGEST:
        src = RAW_INPUT_DIR / filename
        if not src.exists():
            continue

        f_hash = compute_sha256(src)
        if f_hash in ingested_hashes:
            print(f"[IDEMPOTENT SKIP] {filename} already ingested.")
            continue

        shutil.copy2(src, RAW_FILES_DEST / filename)
        manifest[filename] = {
            "source_file": filename,
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "byte_size": src.stat().st_size,
            "sha256": f_hash
        }
        ingested_hashes.add(f_hash)
        print(f"[INGESTED] {filename}")

    tmp = MANIFEST_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    tmp.replace(MANIFEST_PATH)

if __name__ == "__main__":
    ingest_files()