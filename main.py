# main.py
"""Airtable ↔ BigQuery sync for ad ROAS metrics.

This script reads rows from BigQuery and syncs a subset of fields into
Airtable. It performs **creates** (optional, currently disabled) and
**updates** (enabled), but **no deletes**. The sync can be run locally
(via the `__main__` guard) or invoked as a Google Cloud Function via
`my_function`.

Notes
-----
- Authentication for BigQuery and Secret Manager is expected via
  Application Default Credentials (ADC) in Cloud Functions.
- The Airtable API key is fetched from Secret Manager.
- Updates are de-duplicated per Airtable record ID to avoid the
  "duplicate updates in one request" error.
"""

from __future__ import annotations

import math
import os
import time
from decimal import Decimal
from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging, sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
log = logging.getLogger(__name__)

class BadGateway(Exception): ...
class GatewayTimeout(Exception): ...


def _retry_session() -> requests.Session:
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PATCH", "PUT", "DELETE"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

HTTP = _retry_session()

from google.cloud import bigquery  # noqa: F401 (imported for context)
from google.cloud import secretmanager  # noqa: F401 (imported for context)
from tqdm import tqdm

from common_functions import get_bq_client, get_credentials_from_secret_manager

# === CONFIG (hardcoded for local) ===
GCP_PROJECT_ID = "snocks-analytics"
BQ_DATASET = "marts_finance_euw3"
BQ_TABLE = "ad_create_roas"

# Airtable
AIRTABLE_BASE_SNOCKS_DEV = "app91jmdmjCxpVmig"
AIRTABLE_BASE_OA_DEV = "apprHKYwMV8JWxeYk"  
 
AIRTABLE_BASE_SNOCKS = "appSC6U99N8701HKf"  
AIRTABLE_TABLE_SNOCKS = "tblkAk6Q9ZgG9LyTz"  
AIRTABLE_BASE_OA = "app0N8ejLt45NY0Hg"  
AIRTABLE_TABLE_OA = "tblkAk6Q9ZgG9LyTz"

# Les Lunes (Leslunes) — production base; `company` in BQ is lowercase `leslunes`
AIRTABLE_BASE_LESLUNES = "appUqN503Qs24duGz"
AIRTABLE_TABLE_LESLUNES = "tblkAk6Q9ZgG9LyTz"

# Field mapping: BigQuery -> Airtable
BQ_EXTERNAL_ID_COL = "extracted_CR_number"
BQ_FIELD1_COL = "revenue"
BQ_FIELD2_COL = "spend"
BQ_FIELD3_COL = "roas"
BQ_FIELD4_COL = "first_date"
BQ_FIELD5_COL = "poas"
BQ_FIELD6_COL = "cm2_with_estimates"

AT_EXTERNAL_ID = "Creative ID"
AT_FIELD1 = "revenue"
AT_FIELD2 = "spend"
AT_FIELD3 = "roas"
AT_FIELD4 = "first_date"
AT_FIELD5 = "poas"
AT_FIELD6 = "cm2_with_estimates"

# === SECRETS ===
# Either set via env var (export AIRTABLE_API_KEY="pat_...") or fetch from Secret Manager
AIRTABLE_SECRET_NAME = "airtable_full_acces_api_key"

# === AUTH ===
# Pulls default credentials from the Cloud Functions environment (ADC)

bq_client = get_bq_client()


# ------------------------------
# Airtable helpers
# ------------------------------

def _airtable_headers(api_key: str) -> Dict[str, str]:
    """Standard headers for Airtable API calls."""
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


essential_tables = {
    "Snocks": (AIRTABLE_BASE_SNOCKS, AIRTABLE_TABLE_SNOCKS),
    "OA": (AIRTABLE_BASE_OA, AIRTABLE_TABLE_OA),
    "Leslunes": (AIRTABLE_BASE_LESLUNES, AIRTABLE_TABLE_LESLUNES),
}


def _airtable_url(company: str) -> str:
    """Return the v0 Airtable endpoint URL for a given company label."""
    company = _normalize_company_label(company)   # <-- NEW
    AIRTABLE_BASE, AIRTABLE_TABLE = essential_tables[company]
    return f"https://api.airtable.com/v0/{AIRTABLE_BASE}/{AIRTABLE_TABLE}"

def _normalize_company_label(label: str) -> str:
    """Return canonical label: 'Snocks', 'OA', or 'Leslunes'.

    Accepts common aliases (spaces/underscores stripped, case-insensitive).
    """
    c = (label or "").strip().lower().replace(" ", "").replace("_", "")
    if c in ("snocks",):
        return "Snocks"
    if c in ("oa", "oceansapart"):
        return "OA"
    if c == "leslunes":
        return "Leslunes"
    raise ValueError("company must be 'Snocks', 'OA', or 'Leslunes' (aliases: leslunes, les lunes, …)")

def _bq_company_value(company_label: str) -> str:
    """Return the value used in BigQuery's 'company' column for a given label."""
    if company_label == "OA":
        return "Oceansapart"   # <-- OA steht für Oceansapart
    if company_label == "Snocks":
        return "snocks"         # <-- so steht es in deiner Query/Tabelle
    if company_label == "Leslunes":
        return "leslunes"
    raise ValueError("Unsupported company label")


# ------------------------------
# BigQuery fetch
# ------------------------------

def fetch_bq_rows(company: str = "Snocks") -> List[Dict[str, Any]]:
    """Load rows for the given company based on the explicit 'company' column from BigQuery."""
    company_label = _normalize_company_label(company)
    bq_company = _bq_company_value(company_label)

    sql = f"""
    SELECT
      company,
      CAST({BQ_EXTERNAL_ID_COL} AS STRING) AS external_id,
      {BQ_FIELD1_COL} AS f1,
      {BQ_FIELD2_COL} AS f2,
      {BQ_FIELD3_COL} AS f3,
      FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', CAST({BQ_FIELD4_COL} AS TIMESTAMP)) AS f4,
      {BQ_FIELD5_COL} AS f5,
      {BQ_FIELD6_COL} AS f6
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}`
    WHERE company = @company
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("company", "STRING", bq_company)]
    )

    rows: List[Dict[str, Any]] = []
    for r in bq_client.query(sql, job_config=job_config).result():
        rows.append({
            "company": r["company"],      # für Debug/Transparenz
            "external_id": r["external_id"],
            "f1": r.get("f1"),
            "f2": r.get("f2"),
            "f3": r.get("f3"),
            "f4": r.get("f4"),
            "f5": r.get("f5"),
            "f6": r.get("f6"),
        })
    return rows

def fetch_airtable_index(company: str, api_key: str) -> Dict[str, Dict[str, Any]]:
    """Fetch all rows from Airtable and index them by the external id.

    Returns a mapping `{external_id: {rid, f1, f2, f3, f4, f5}}` for quick lookup.
    """
    url = _airtable_url(company)
    headers = _airtable_headers(api_key)
    by_ext: Dict[str, Dict[str, Any]] = {}
    offset: str | None = None

    while True:
        params: Dict[str, Any] = {
            "pageSize": 50,
            "fields[]": [AT_EXTERNAL_ID, AT_FIELD1, AT_FIELD2, AT_FIELD3, AT_FIELD4, AT_FIELD5, AT_FIELD6],
        }
        if offset:
            params["offset"] = offset

        try:
            r = HTTP.get(url, headers=headers, params=params, timeout=(5, 15))
            if r.status_code == 429:
                log.error("Airtable 429 after retries (company=%s)", company)
                raise BadGateway("airtable_rate_limited")
            r.raise_for_status()
        except requests.Timeout:
            log.exception("Airtable timeout while listing (company=%s)", company)
            raise GatewayTimeout("airtable_timeout")
        except requests.RequestException:
            log.exception("Airtable request failed while listing (company=%s)", company)
            raise BadGateway("airtable_request_failed")

        data = r.json()

        for rec in data.get("records", []):
            f = rec.get("fields", {})
            ext = f.get(AT_EXTERNAL_ID)
            if ext:
                by_ext[ext] = {
                    "rid": rec["id"],
                    "f1": f.get(AT_FIELD1),
                    "f2": f.get(AT_FIELD2),
                    "f3": f.get(AT_FIELD3),
                    "f4": f.get(AT_FIELD4),
                    "f5": f.get(AT_FIELD5),
                    "f6": f.get(AT_FIELD6),
                }

        offset = data.get("offset")
        if not offset:
            break

    return by_ext


# ------------------------------
# Value sanitizers
# ------------------------------

def _sanitize_for_airtable(v: Any) -> Any:
    """Prepare Python values for Airtable JSON payloads.

    - Convert Decimal to float
    - Convert NaN/Inf to None
    - Coerce empty strings to None so Airtable clears the field
    - Pass through ints, floats, and strings otherwise
    - Fall back to string conversion if possible
    """
    if v is None or v == "":
        return None
    if isinstance(v, Decimal):
        v = float(v)
    if isinstance(v, float):
        if not math.isfinite(v):
            return None
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        return v
    try:
        fv = float(v)
        if not math.isfinite(fv):
            return None
        return fv
    except Exception:
        try:
            s = str(v)
            return s if s != "" else None
        except Exception:
            return None


def build_fields_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    """Map a BigQuery row to the Airtable fields payload."""
    return {
        AT_FIELD1: _sanitize_for_airtable(row.get("f1")),
        AT_FIELD2: _sanitize_for_airtable(row.get("f2")),
        AT_FIELD3: _sanitize_for_airtable(row.get("f3")),
        AT_FIELD4: _sanitize_for_airtable(row.get("f4")),
        AT_FIELD5: _sanitize_for_airtable(row.get("f5")),
        AT_FIELD6: _sanitize_for_airtable(row.get("f6")),
    }


def chunk(lst: List[Any], n: int = 10):
    """Yield successive `n`-sized chunks from `lst`.

    Airtable limits create/update batch sizes to 10 records per request.
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _retryable_request(method: str, url: str, headers: Dict[str, str], json: Dict[str, Any], timeout: int = 60):
    """HTTP call with simple exponential backoff for 429/5xx and verbose logging."""
    last_resp = None
    for attempt in range(6):
        try:
            r = HTTP.request(method, url, headers=headers, json=json, timeout=(5, timeout))
        except requests.Timeout:
            log.warning("Airtable %s timeout on attempt %d", method, attempt + 1)
            time.sleep(2 ** attempt * 0.5)
            continue
        last_resp = r
        if r.status_code in (429, 500, 502, 503, 504):
            log.warning("Airtable %s retryable status %s: %s", method, r.status_code, r.text[:200])
            time.sleep(2 ** attempt * 0.5)
            continue
        if r.status_code >= 400:
            log.error("Airtable %s error %s: %s", method, r.status_code, r.text[:200])
            r.raise_for_status()
        r.raise_for_status()
        return r

    if last_resp is not None:
        msg = f"Airtable request failed after retries. Status {last_resp.status_code}: {last_resp.text[:200]}"
        raise BadGateway(msg)

    raise requests.HTTPError("Airtable request failed with no response received.")


def write_creates(company: str, api_key: str, records: List[Dict[str, Any]]):
    """Create new Airtable records in batches of 10 (if `records` is not empty)."""
    if not records:
        return
    url = _airtable_url(company)
    headers = _airtable_headers(api_key)

    batches = list(chunk(records, 10))
    for batch in tqdm(batches, desc="Creating in Airtable", unit="batch"):
        _retryable_request("POST", url, headers, {"records": batch})


def write_updates(company: str, api_key: str, records: List[Dict[str, Any]]):
    """Update Airtable records in batches of 10 (if `records` is not empty)."""
    if not records:
        return
    url = _airtable_url(company)
    headers = _airtable_headers(api_key)

    batches = list(chunk(records, 10))
    for batch in tqdm(batches, desc="Updating Airtable", unit="batch"):
        _retryable_request("PATCH", url, headers, {"records": batch})


# ------------------------------
# Main sync
# ------------------------------

def run_sync(company: str) -> Dict[str, int]:
    
    """Run a sync for the given company label ('Snocks', 'OA', or 'Leslunes')."""
    company = _normalize_company_label(company)  # <-- NEW (zentrale Normalisierung)

    api_key = get_credentials_from_secret_manager(AIRTABLE_SECRET_NAME)

    # 1) Airtable-Index (per Base der Company)
    at_index = fetch_airtable_index(company, api_key)

    # 2) BigQuery-Zeilen nur für diese Company
    rows = fetch_bq_rows(company)
    log.info("Fetched %d BQ rows for %s", len(rows), company)


    # 3) Diff: create / update / skip  
    creates: List[Dict[str, Any]] = []
    
    updates_by_id: Dict[str, Dict[str, Any]] = {}  # rid -> payload (dedupe so each record appears once per batch)
    seen_creates_ext: set[str] = set()
    skipped = 0
    deleted_full_clears: list[dict[str, str]] = []
    deleted_partial_clears: list[dict[str, str]] = []

    def _merge_update(upd_by_id: Dict[str, Dict[str, Any]], rec_id: str, fields: Dict[str, Any]) -> None:
        """Merge fields into an existing update for rec_id instead of overwriting it.
        Clears (None) should override previous values for the same keys."""
        if rec_id in upd_by_id:
            upd_by_id[rec_id]["fields"].update(fields)
        else:
            upd_by_id[rec_id] = {"id": rec_id, "fields": dict(fields)}

    for r in rows:
        ext = r["external_id"]
        payload_fields = build_fields_payload(r)

        if ext not in at_index:
            # Create: include external id and dedupe by external id
            if ext not in seen_creates_ext:
                log.info("Create planned: %s", ext)
                create_fields = {**payload_fields, AT_EXTERNAL_ID: ext}
                creates.append({"fields": create_fields})
                seen_creates_ext.add(ext)
            else:
                skipped += 1
        else:
            # Update: dedupe by Airtable record id; merge so we don't lose fields later
            cur = at_index[ext]
            desired_tuple = (
                payload_fields.get(AT_FIELD1),
                payload_fields.get(AT_FIELD2),
                payload_fields.get(AT_FIELD3),
                payload_fields.get(AT_FIELD4),
                payload_fields.get(AT_FIELD5),
                payload_fields.get(AT_FIELD6),
            )
            f1 = cur.get("f1") or 0.0
            f2 = cur.get("f2") or 0.0
            f3 = cur.get("f3") or 0.0
            f4 = cur.get("f4")
            f5 = cur.get("f5")
            f6 = cur.get("f6")

            current_tuple = (f1, f2, f3, f4, f5, f6)
            if current_tuple != desired_tuple:
                log.info("Update planned for %s", ext)
                _merge_update(updates_by_id, cur["rid"], payload_fields)
            else:
                skipped += 1

    # 3b) Clear Airtable fields when the external_id is gone in BQ
    #     or when BQ has NULL for a field that is still set in Airtable.
    bq_by_ext = {r["external_id"]: r for r in rows}

    for ext, rec in at_index.items():
        rec_id = rec["rid"]
        bq_row = bq_by_ext.get(ext)

        if bq_row is None:
            # external_id existiert in Airtable, aber nicht mehr in BQ → alle Werte leeren
            if any(v is not None for v in (rec.get("f1"), rec.get("f2"), rec.get("f3"), rec.get("f4"), rec.get("f5"), rec.get("f6"))):
                log.info("Clear fields (no longer in BQ) for %s", ext)
                _merge_update(
                    updates_by_id,
                    rec_id,
                    {
                        AT_FIELD1: None,
                        AT_FIELD2: None,
                        AT_FIELD3: None,
                        AT_FIELD4: None,
                        AT_FIELD5: None,
                        AT_FIELD6: None,
                    },
                )
            # >>> Debug-Tracking
            deleted_full_clears.append({
                "company": company,
                "external_id": ext,
                "airtable_record_id": rec_id,
                "reason": "external_id_not_in_bq"
            })
            continue

        # BQ existiert, aber einzelne Felder sind dort NULL → nur diese Felder in AT leeren
        to_clear = {}
        if bq_row.get("f1") is None and rec.get("f1") is not None:
            to_clear[AT_FIELD1] = 0.0
        if bq_row.get("f2") is None and rec.get("f2") is not None:
            to_clear[AT_FIELD2] = 0.0
        if bq_row.get("f3") is None and rec.get("f3") is not None:
            to_clear[AT_FIELD3] = 0.0
        if bq_row.get("f4") is None and rec.get("f4") is not None:
            to_clear[AT_FIELD4] = None
        if bq_row.get("f5") is None and rec.get("f5") is not None:
            to_clear[AT_FIELD5] = 0.0
        if bq_row.get("f6") is None and rec.get("f6") is not None:
            to_clear[AT_FIELD6] = 0.0

        if to_clear:
            log.info("Clear partial fields for %s: %s", ext, ", ".join(to_clear.keys()))
            _merge_update(updates_by_id, rec_id, to_clear)
            # >>> Debug-Tracking
            deleted_partial_clears.append({
                "company": company,
                "external_id": ext,
                "airtable_record_id": rec_id,
                "cleared_fields": list(to_clear.keys()),
                "reason": "fields_null_in_bq"
            })

    updates = list(updates_by_id.values())

    # Debug-JSON schreiben (lokal in CWD, in Cloud Functions nach /tmp)
    try:
        out_path = f"airtable_delete_updates_{company}.json" if os.getenv("K_SERVICE") else f"airtable_delete_updates{company}.json"
        debug_payload = {
            "company": company,
            "timestamp": int(time.time()),
            "full_clears": deleted_full_clears,
            "partial_clears": deleted_partial_clears,
            "counts": {
                "full_clears": len(deleted_full_clears),
                "partial_clears": len(deleted_partial_clears),
            },
        }
        with open(out_path, "w", encoding="utf-8") as f:
            import json
            json.dump(debug_payload, f, ensure_ascii=False, indent=2)
        log.info("Wrote delete-debug JSON to %s (full=%d, partial=%d)",
                 out_path, len(deleted_full_clears), len(deleted_partial_clears))
    except Exception:
        log.exception("Failed to write delete-debug JSON")

    # 4) Write changes
    # write_creates(company, api_key, creates)  # enable if you want to create new records too
    write_updates(company, api_key, updates)

    return {"created": len(creates), "updated": len(updates), "skipped": skipped}


# ------------------------------
# Cloud Function HTTP entrypoint
# ------------------------------

def my_function(request):
    """HTTP entrypoint for Cloud Functions (2nd gen)."""
    log.info("🚀 Sync started.")
    results = {}
    status = 200
    for company in ("Snocks", "OA", "Leslunes"):
        try:
            results[company] = run_sync(company=company)
        except GatewayTimeout as e:
            results[company] = {"error": str(e)}
            status = max(status, 504)
        except BadGateway as e:
            results[company] = {"error": str(e)}
            status = max(status, 502)
        except Exception:
            log.exception("Unhandled error in %s", company)
            results[company] = {"error": "internal"}
            status = max(status, 500)
    log.info("✅ Done: %s", results)
    return ({"result": results}, status)


# ------------------------------
# Optional local run
# ------------------------------
if __name__ == "__main__":
    print(run_sync(company="OA"))
    print(run_sync(company="Snocks"))
    print(run_sync(company="Leslunes"))