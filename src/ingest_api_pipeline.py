import csv
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
import requests

API_URL = "http://127.0.0.1:8000/api/events"
RAW_INPUT_DIR = Path("data")
RAW_API_DIR = Path("data/raw/api")
EVENTS_FILE = RAW_API_DIR / "events.jsonl"
STATE_DIR = Path("state")
WATERMARK_FILE = STATE_DIR / "api_watermark.json"
RUN_LOG_FILE = Path("data/pipeline_run_log.csv")

def get_last_watermark():
    if WATERMARK_FILE.exists():
        try:
            with open(WATERMARK_FILE, "r", encoding="utf-8") as f:
                return json.load(f).get("max_updated_at")
        except Exception:
            return None
    return None

def update_watermark(new_wm: str):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = WATERMARK_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"max_updated_at": new_wm}, f, indent=2)
    tmp.replace(WATERMARK_FILE)

def log_run(run_id, start, end, status, read_c, written_c, dups, wm_bef, wm_aft, err=""):
    RUN_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "run_id", "started_at", "ended_at", "source_name", "status",
        "records_read", "records_written", "duplicates_removed",
        "watermark_before", "watermark_after", "error_message"
    ]
    exists = RUN_LOG_FILE.exists()
    with open(RUN_LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if not exists:
            writer.writeheader()
        writer.writerow({
            "run_id": run_id, "started_at": start, "ended_at": end,
            "source_name": "rest_api_events", "status": status,
            "records_read": read_c, "records_written": written_c,
            "duplicates_removed": dups, "watermark_before": wm_bef,
            "watermark_after": wm_aft, "error_message": err
        })

def run():
    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc).isoformat()
    wm_before = get_last_watermark()
    raw_records = []
    page = 1

    try:
        while True:
            params = {"page": page, "per_page": 20}
            if wm_before:
                params["updated_after"] = wm_before

            res = requests.get(API_URL, params=params, timeout=20)
            res.raise_for_status()
            payload = res.json()

            items = payload.get("items", [])
            for item in items:
                item["_ingested_at"] = datetime.now(timezone.utc).isoformat()
                item["_source"] = API_URL
                raw_records.append(item)

            if not payload.get("has_more", False):
                break
            page += 1

        records_read = len(raw_records)

        existing = {}
        if EVENTS_FILE.exists():
            with open(EVENTS_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        r = json.loads(line)
                        existing[r["event_id"]] = r

        initial_total = len(existing) + len(raw_records)
        for r in raw_records:
            eid = r["event_id"]
            if eid not in existing or r["updated_at"] > existing[eid]["updated_at"]:
                existing[eid] = r

        deduped = list(existing.values())
        duplicates_removed = initial_total - len(deduped)
        records_written = records_read - duplicates_removed if records_read > 0 else 0

        RAW_API_DIR.mkdir(parents=True, exist_ok=True)
        tmp_out = EVENTS_FILE.with_suffix(".tmp")
        with open(tmp_out, "w", encoding="utf-8") as f:
            for r in deduped:
                f.write(json.dumps(r) + "\n")
        tmp_out.replace(EVENTS_FILE)

        wm_after = wm_before
        if raw_records:
            max_up = max(r["updated_at"] for r in raw_records)
            if not wm_before or max_up > wm_before:
                wm_after = max_up
                update_watermark(wm_after)

        end_time = datetime.now(timezone.utc).isoformat()
        log_run(run_id, start_time, end_time, "SUCCESS", records_read, records_written, duplicates_removed, wm_before, wm_after)
        print(f"[SUCCESS] Read: {records_read}, Deduped: {duplicates_removed}, New Watermark: {wm_after}")

    except Exception as e:
        end_time = datetime.now(timezone.utc).isoformat()
        log_run(run_id, start_time, end_time, "FAILED", len(raw_records), 0, 0, wm_before, wm_before, str(e))
        print(f"[FAILED] {e}")
        raise e

if __name__ == "__main__":
    run()