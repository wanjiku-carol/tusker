# Enterprise Marketing Data Platform — System Architecture

---

## 1. The Brief (Original Prompt)

> I am part of a large advertising and marketing consultancy. The organisation deals with data from large companies and their subsidiary organisations. They have mainly been working using Excel sheets for data cleaning and tools such as PowerBI for analysis. They have also been storing their data in SQL Servers, but they only work with SQL queries, and have deeply nested tables.
>
> The task is to automate the ETL pipeline by use of integration, APIs and deployment, and migrate the nested tables to a cloud service. The Cloud Storages they are willing to settle for are **GCP, Azure or AWS**. Since the organisation uses a number of tools like **DV360** for their programmatic advertising platforms, they want to plug that into the pipeline and retrieve the data collected.
>
> They want to use a **Data Lake** to hold unprocessed data. Thereafter they provide a series of ways to clean, parse, transform and prepare for delivery to different locations — e.g. BI presentations for their clients, competitive analysis, and integration through APIs to other interested parties.
>
> **Scale:** 50 million rows of data per day.
> **Data types:** Structured and unstructured.
> **Priorities (in order):** Storage → Availability → Accuracy → Reliability.

---

## 2. Executive Decision: Why GCP

Before drawing a single box, we have to settle the cloud question. Normally this is a coin-flip between the three hyperscalers, but here there is a **hard technical constraint** that makes the decision for us.

### The deciding factor: DV360 is a Google product

| Capability                          | GCP                                              | AWS                                 | Azure                               |
|-------------------------------------|--------------------------------------------------|-------------------------------------|-------------------------------------|
| DV360 ingestion                     | **Native** — BigQuery Data Transfer Service      | Custom OAuth polling against DBM API | Custom OAuth polling against DBM API |
| Campaign Manager 360                | **Native** — BQ Data Transfer                    | Custom build                        | Custom build                        |
| Google Ads / SA360 / YouTube        | **Native** — BQ Data Transfer                    | Custom build                        | Custom build                        |
| Deeply nested table support         | **Native** — `STRUCT` / `ARRAY<STRUCT>` columns  | Athena (partial) / Redshift SUPER   | Synapse (flatten on load)           |
| PowerBI connectivity                | Native BigQuery connector                        | Native                              | Native                              |

On AWS or Azure we would spend the first three months of the project building and hardening an API poller for DV360 (OAuth refresh, pagination, rate-limit backoff, schema-drift handling). On GCP this is a **config form** — you link the DV360 advertiser ID, pick a schedule, and reporting data lands in BigQuery daily with zero code.

The second reason is subtler but equally important: the team currently lives in **deeply nested SQL tables**. BigQuery is the only cloud warehouse where nested and repeated records are first-class citizens. We can migrate a parent-with-five-children table into a single BigQuery table with `ARRAY<STRUCT>` columns and query it with `UNNEST()` — no flattening, no 30-way joins, no loss of semantic structure.

**Decision: Google Cloud Platform.**

---

## 3. High-Level Architecture

We use a **Medallion (Bronze → Silver → Gold)** pattern layered on a **Lakehouse** (Cloud Storage for the lake, BigQuery for the warehouse). Every byte enters through Bronze and is never mutated there — this gives us infinite replay for the Reliability priority.

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                        SOURCE SYSTEMS                                            │
├─────────────────┬─────────────────┬──────────────────┬──────────────────┬────────────────────────┤
│  DV360 / CM360  │  On-Prem SQL    │  Third-Party     │  Client File     │  Streaming Events      │
│  Google Ads     │  Server         │  APIs (Meta,     │  Drops           │  (Pixel/Tag data)      │
│  SA360          │  (nested tbls)  │  TikTok, TTD)    │  (Excel, CSV)    │                        │
└────────┬────────┴────────┬────────┴────────┬─────────┴────────┬─────────┴───────────┬────────────┘
         │                 │                 │                  │                     │
         ▼                 ▼                 ▼                  ▼                     ▼
┌─────────────────┐┌─────────────────┐┌─────────────────┐┌─────────────────┐┌─────────────────────┐
│  BQ Data        ││  Datastream     ││  Cloud          ││  GCS Landing    ││  Pub/Sub            │
│  Transfer Svc   ││  (CDC)          ││  Functions      ││  Bucket +       ││  (at-least-once,    │
│  (zero-code)    ││  → GCS → BQ     ││  → Pub/Sub      ││  Event Trigger  ││  7-day retention)   │
└────────┬────────┘└────────┬────────┘└────────┬────────┘└────────┬────────┘└──────────┬──────────┘
         │                  │                  │                  │                    │
         └──────────────────┴──────────────────┴──────────────────┴────────────────────┘
                                               │
                                               ▼
         ┌─────────────────────────────────────────────────────────────────────────────┐
         │                      BRONZE — Raw Data Lake (GCS)                           │
         │  gs://org-datalake-bronze-{region}/                                         │
         │       ├── dv360/         yyyy/mm/dd/  (Parquet, from BQ export)             │
         │       ├── sqlserver/     yyyy/mm/dd/  (Avro, CDC changelog)                 │
         │       ├── api/{vendor}/  yyyy/mm/dd/  (raw JSON, untouched)                 │
         │       ├── files/{client}/yyyy/mm/dd/  (original Excel/CSV bytes)            │
         │       └── quarantine/    yyyy/mm/dd/  (schema-rejected payloads)            │
         │                                                                             │
         │  Multi-region | Versioned | Immutable (retention lock) | Lifecycle → Archive│
         └─────────────────────────────────────┬───────────────────────────────────────┘
                                               │
                     ┌─────────────────────────┴──────────────────────────┐
                     │              Cloud Composer (Airflow)              │
                     │  Orchestrates daily DAGs. Idempotent. Backfill-    │
                     │  safe. Stores watermarks in BQ metadata table.     │
                     └─────────────────────────┬──────────────────────────┘
                                               │ triggers
                                               ▼
                     ┌────────────────────────────────────────────────────┐
                     │              Dataflow (Apache Beam)                │
                     │  • Parse unstructured → structured                 │
                     │  • Deduplicate (windowed, on natural key + hash)   │
                     │  • Type-cast & null-handle                         │
                     │  • Schema-validate → reject to quarantine          │
                     │  • Flatten OR preserve nesting (per target)        │
                     │  Autoscales 1 → 200 workers. Exactly-once to BQ.   │
                     └─────────────────────────┬──────────────────────────┘
                                               │
                                               ▼
         ┌─────────────────────────────────────────────────────────────────────────────┐
         │                    SILVER — Cleaned Warehouse (BigQuery)                    │
         │  Dataset: org_silver                                                        │
         │       ├── fact_impressions        (partitioned by event_date, clustered     │
         │       │                            by advertiser_id, campaign_id)           │
         │       ├── fact_conversions                                                  │
         │       ├── dim_advertiser          (SCD Type 2 — full history)               │
         │       ├── dim_campaign            (nested: ARRAY<STRUCT<line_item>>)        │
         │       ├── dim_creative                                                      │
         │       └── _metadata_watermarks    (load state, row counts, checksums)       │
         │                                                                             │
         │  Deduplicated | Strongly typed | Referential integrity enforced             │
         └─────────────────────────────────────┬───────────────────────────────────────┘
                                               │
                                    ┌──────────┴──────────┐
                                    │   dbt (in Composer) │
                                    │   SQL transforms    │
                                    │   + data tests      │
                                    └──────────┬──────────┘
                                               │
                                               ▼
         ┌─────────────────────────────────────────────────────────────────────────────┐
         │                     GOLD — Curated / Serving (BigQuery)                     │
         │  Dataset: org_gold                                                          │
         │       ├── client_{name}_daily_performance   (denormalised star, BI-ready)   │
         │       ├── competitive_share_of_voice        (cross-client aggregates)       │
         │       ├── api_campaign_summary              (pre-aggregated for API speed)  │
         │       └── exec_weekly_snapshot              (materialised, small, fast)     │
         │                                                                             │
         │  Authorized Views (row-level security per client) | Materialised where hot  │
         └──────┬──────────────────────┬──────────────────────┬────────────────────────┘
                │                      │                      │
                ▼                      ▼                      ▼
       ┌─────────────────┐   ┌─────────────────┐   ┌──────────────────────────┐
       │  PowerBI        │   │  Cloud Run      │   │  Scheduled Exports       │
       │  (native BQ     │   │  REST API       │   │  → Client GCS buckets    │
       │  connector —    │   │  (serves Gold   │   │  → SFTP push             │
       │  team keeps     │   │  views to       │   │  → Reverse-ETL to CRMs   │
       │  existing tool) │   │  third parties) │   │                          │
       └─────────────────┘   └─────────────────┘   └──────────────────────────┘
```

---

## 4. Layer-by-Layer Design

### 4.1 Ingestion Layer

The ingestion layer has **one job**: get bytes into Bronze as fast as possible, with the minimum possible transformation. We do not clean here. We do not validate here (beyond "is this valid JSON/Avro"). We land and we move on. Cleaning in the ingestion layer is how pipelines become unrepayable.

#### 4.1.1 DV360 and Google Marketing Platform — BigQuery Data Transfer Service

This is the zero-code path. Configured once in the console (or Terraform):

| Source              | Transfer Config                                    | Cadence     | Lands In                          |
|---------------------|----------------------------------------------------|-------------|-----------------------------------|
| DV360               | Link Partner ID + Advertiser IDs                   | Daily 04:00 | `org_bronze.dv360_*` tables       |
| Campaign Manager 360| Link CM360 Account + Floodlight Config             | Daily 04:00 | `org_bronze.cm360_*` tables       |
| Google Ads          | Link Customer ID                                   | Daily 04:00 | `org_bronze.gads_*` tables        |
| Search Ads 360      | Link Agency + Advertiser                           | Daily 04:00 | `org_bronze.sa360_*` tables       |

After each transfer completes, a Composer sensor detects the `_PARTITIONTIME` update and exports a Parquet snapshot to `gs://org-datalake-bronze/dv360/yyyy/mm/dd/` so the lake always has a file-based copy independent of BigQuery. This matters for the **Storage** priority — if we ever need to leave BigQuery, the lake stands alone.

#### 4.1.2 SQL Server Migration — Datastream (CDC)

The on-prem SQL Server holds the deeply nested tables. We do **not** do a one-shot lift-and-shift — we run **Change Data Capture** so the legacy system and the cloud stay in sync during the (long) migration window. The consultancy cannot stop operations while we cut over.

```
SQL Server (on-prem)
   │  [enable CDC on source tables]
   ▼
Datastream private connectivity (Cloud VPN / Interconnect)
   │  reads transaction log, not tables — zero load on prod DB
   ▼
GCS staging  →  Dataflow template  →  BigQuery
   (Avro changelog:                   (MERGE on PK,
    insert/update/delete              handles nesting)
    with op-type + ts)
```

**Nested-table strategy.** Say the legacy schema is:

```
Clients ──< Campaigns ──< LineItems ──< Creatives
```

In SQL Server that's four tables and three joins every query. In BigQuery Silver that becomes **one table**:

```sql
CREATE TABLE org_silver.dim_campaign (
  client_id         STRING,
  client_name       STRING,
  campaign_id       STRING,
  campaign_name     STRING,
  campaign_budget   NUMERIC,
  line_items        ARRAY<STRUCT<
                      line_item_id   STRING,
                      line_item_name STRING,
                      bid_strategy   STRING,
                      creatives      ARRAY<STRUCT<
                                       creative_id   STRING,
                                       creative_type STRING,
                                       asset_url     STRING
                                     >>
                    >>,
  _loaded_at        TIMESTAMP,
  _source_lsn       STRING      -- CDC log sequence number for lineage
)
PARTITION BY DATE(_loaded_at)
CLUSTER BY client_id, campaign_id;
```

Analysts query it with `UNNEST()` when they need the children, or ignore the array when they don't. No joins. This is the single biggest quality-of-life improvement the team will feel.

#### 4.1.3 Third-Party APIs — Cloud Functions (Gen 2)

Meta Ads, TikTok Ads, The Trade Desk, etc. These don't have native transfers, so we poll.

**Pattern:** Cloud Scheduler → Cloud Function → writes raw JSON to Pub/Sub → Pub/Sub subscription → GCS Bronze.

We go through Pub/Sub (rather than Function → GCS direct) because Pub/Sub gives us **7-day message retention** and a **dead-letter topic**. If GCS has a blip, or the payload is malformed, we don't lose the data — it sits in the DLQ and we replay it. Reliability priority.

```python
# cloud_functions/ingest_meta_ads/main.py
# Triggered by Cloud Scheduler every hour.
# Job: pull one hour of Meta Ads insights, push RAW to Pub/Sub. No cleaning.

import json
import os
from datetime import datetime, timedelta, timezone

import functions_framework
from google.cloud import pubsub_v1, secretmanager
import requests

PROJECT_ID   = os.environ["GCP_PROJECT"]
TOPIC_ID     = os.environ["PUBSUB_TOPIC"]          # e.g. "ingest-meta-raw"
SECRET_NAME  = os.environ["META_TOKEN_SECRET"]     # Secret Manager resource name
AD_ACCOUNTS  = os.environ["META_AD_ACCOUNTS"].split(",")

_publisher = pubsub_v1.PublisherClient()
_topic     = _publisher.topic_path(PROJECT_ID, TOPIC_ID)
_secrets   = secretmanager.SecretManagerServiceClient()


def _token() -> str:
    resp = _secrets.access_secret_version(name=SECRET_NAME)
    return resp.payload.data.decode("utf-8")


@functions_framework.http
def ingest(request):
    # Watermark: pull the previous complete hour. Idempotent —
    # re-running the same hour produces identical messages
    # (dedup happens downstream in Dataflow on content hash).
    now       = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    hour_end  = now
    hour_start = now - timedelta(hours=1)

    tok = _token()
    published = 0

    for account in AD_ACCOUNTS:
        url = f"https://graph.facebook.com/v19.0/{account}/insights"
        params = {
            "level": "ad",
            "time_range": json.dumps({
                "since": hour_start.strftime("%Y-%m-%d"),
                "until": hour_end.strftime("%Y-%m-%d"),
            }),
            "time_increment": 1,
            "fields": "ad_id,campaign_id,impressions,clicks,spend,actions",
            "limit": 500,
        }
        headers = {"Authorization": f"Bearer {tok}"}

        while url:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            r.raise_for_status()
            body = r.json()

            for row in body.get("data", []):
                # Wrap — never mutate — the source payload.
                # Envelope carries lineage, payload stays byte-identical.
                envelope = {
                    "source":         "meta_ads",
                    "source_account": account,
                    "ingested_at":    datetime.now(timezone.utc).isoformat(),
                    "watermark_hour": hour_start.isoformat(),
                    "payload":        row,   # UNTOUCHED
                }
                _publisher.publish(
                    _topic,
                    json.dumps(envelope).encode("utf-8"),
                    source="meta_ads",
                    account=account,
                )
                published += 1

            # Cursor pagination — params only on first request.
            url = body.get("paging", {}).get("next")
            params = None

    return {"published": published, "hour": hour_start.isoformat()}, 200
```

Key properties of this function:
- **Stateless** — watermark is derived from wall-clock, not stored state. Re-running is safe.
- **No transformation** — the `payload` field is the API response verbatim. If Meta adds a field tomorrow, it's already in Bronze.
- **Secrets in Secret Manager** — never in env vars, never in code.
- **Timeout set** — a hung HTTP call won't wedge the function.

#### 4.1.4 Client File Drops — GCS Event Triggers

Clients email Excel files. Today an analyst opens them and copy-pastes into a master sheet. Tomorrow the analyst drags the file into a GCS bucket and walks away.

```
gs://org-client-dropzone/{client_id}/inbox/
   │
   │  [Object Finalize event]
   ▼
Cloud Function (validates extension, computes SHA-256,
                copies to Bronze with content-addressed name)
   │
   ▼
gs://org-datalake-bronze/files/{client_id}/yyyy/mm/dd/{sha256}.xlsx
   │
   │  [publishes "file-landed" message to Pub/Sub]
   ▼
Dataflow parses Excel → rows → Silver staging
```

The SHA-256 naming means the same file uploaded twice lands once. **Accuracy** priority — no silent duplicates from a nervous account manager hitting upload three times.

---

### 4.2 Storage Layer — Bronze (Data Lake)

**This is where the Storage priority is won or lost.** The design principle is: *Bronze is forever. Bronze is immutable. Bronze is the source of truth.*

#### Bucket layout

```
gs://org-datalake-bronze-eu/               ← Multi-region: EU (eur4)
├── dv360/           2026/03/04/ part-*.parquet
├── cm360/           2026/03/04/ part-*.parquet
├── sqlserver/
│   ├── clients/     2026/03/04/ cdc-*.avro
│   ├── campaigns/   2026/03/04/ cdc-*.avro
│   └── ...
├── api/
│   ├── meta/        2026/03/04/ hour=13/ *.json
│   ├── tiktok/      2026/03/04/ hour=13/ *.json
│   └── ttd/         2026/03/04/ *.json
├── files/
│   └── {client_id}/ 2026/03/04/ {sha256}.xlsx
└── quarantine/      2026/03/04/ {source}/ *.json   ← schema-rejected rows
```

#### Storage configuration

| Setting                  | Value                              | Why                                                         |
|--------------------------|------------------------------------|-------------------------------------------------------------|
| Location type            | **Multi-region (EU)**              | 99.95% availability SLA. Survives single-region loss.       |
| Storage class (default)  | Standard                           | Hot for first 30 days while pipelines re-read.              |
| Versioning               | **Enabled**                        | Accidental overwrite protection.                            |
| Retention policy         | **Locked, 7 years**                | Regulatory hold. Objects *cannot* be deleted, even by admins. Storage priority = data cannot be lost. |
| Uniform bucket-level IAM | Enabled                            | No ACL drift.                                               |
| CMEK                     | Enabled (Cloud KMS)                | Client data → customer-managed keys.                        |

#### Lifecycle policy — cost without risk

```json
{
  "lifecycle": {
    "rule": [
      {
        "action": { "type": "SetStorageClass", "storageClass": "NEARLINE" },
        "condition": { "age": 30, "matchesStorageClass": ["STANDARD"] }
      },
      {
        "action": { "type": "SetStorageClass", "storageClass": "COLDLINE" },
        "condition": { "age": 90, "matchesStorageClass": ["NEARLINE"] }
      },
      {
        "action": { "type": "SetStorageClass", "storageClass": "ARCHIVE" },
        "condition": { "age": 365, "matchesStorageClass": ["COLDLINE"] }
      }
    ]
  }
}
```

Note what is **absent**: there is no `Delete` rule. Data gets colder and cheaper but never disappears. At 50M rows/day ≈ 50GB/day ≈ 18TB/year, Archive storage costs roughly **$22/TB/year** — trivial compared to the cost of losing a client's historical campaign data.

---

### 4.3 Processing Layer — Bronze → Silver

#### 4.3.1 Orchestration — Cloud Composer (Airflow)

One DAG per source. Each DAG is **idempotent** — running it twice for the same `execution_date` produces identical Silver output. This is non-negotiable for the Reliability priority.

```
DAG: dv360_daily
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│ wait_for_transfer│──▶│ export_to_bronze │──▶│ dataflow_clean   │──▶│ dq_gate          │
│ (sensor on BQ    │   │ (BQ → Parquet    │   │ (dedupe, type,   │   │ (row count ±2%,  │
│  partition)      │   │  → GCS)          │   │  validate)       │   │  null-rate check)│
└──────────────────┘   └──────────────────┘   └──────────────────┘   └────────┬─────────┘
                                                                              │
                                                              ┌───────────────┴────────────────┐
                                                              │ pass                      fail │
                                                              ▼                               ▼
                                                   ┌──────────────────┐          ┌──────────────────┐
                                                   │ merge_to_silver  │          │ alert_pagerduty  │
                                                   │ (MERGE not       │          │ + halt DAG       │
                                                   │  INSERT)         │          │ (no bad data     │
                                                   └────────┬─────────┘          │  reaches Silver) │
                                                            │                    └──────────────────┘
                                                            ▼
                                                   ┌──────────────────┐
                                                   │ update_watermark │
                                                   └──────────────────┘
```

The **DQ gate before Silver** is the architectural expression of the Accuracy priority. Bad data is allowed into Bronze (we want to see it). Bad data is **never** allowed into Silver. If the gate fails, a human looks at it. We'd rather have stale-but-correct data in PowerBI than fresh-but-wrong.

#### 4.3.2 Transformation — Dataflow (Apache Beam)

Dataflow is the workhorse. It reads Bronze, does the heavy lifting, and writes Silver. Autoscaling handles the 50M-row days and the quiet weekends equally.

**What Dataflow does per source:**

1. **Parse** — Excel → rows, JSON → rows, Avro → rows.
2. **Deduplicate** — on `(natural_key, content_hash)` within a sliding window. Catches API retries and double-uploads.
3. **Type-cast** — `"123"` → `INT64`, `"2026-03-04"` → `DATE`, `""` → `NULL`. Strict. Failures go to quarantine, not to Silver.
4. **Conform** — map source-specific names to canonical names (`cmpgn_id` → `campaign_id`).
5. **Enrich** — attach `_source`, `_ingested_at`, `_bronze_path` to every row for lineage.
6. **Validate** — schema contract check. Extra fields are kept (in a `_extras` JSON column). Missing required fields → quarantine.
7. **Write** — `MERGE` into BigQuery Silver, exactly-once.

#### 4.3.3 Silver → Gold — dbt

Silver → Gold is pure SQL, so we use dbt (running inside Composer). dbt gives us:

- **Tests as code** — `unique`, `not_null`, `relationships`, `accepted_values` assertions on every model.
- **Lineage graph** — auto-generated. When a client asks "where did this number come from?" we show them the graph.
- **Incremental models** — only reprocess yesterday's partition, not all of history.

---

### 4.4 Serving Layer — Gold

Three consumers, three patterns.

#### 4.4.1 PowerBI — Direct BigQuery Connector

**We do not rip out PowerBI.** The team knows it, the clients expect it. PowerBI has a native BigQuery connector that supports both Import and DirectQuery mode.

- **Import mode** for exec dashboards (small, fast, refreshed daily at 07:00).
- **DirectQuery mode** for analyst deep-dives (always-fresh, queries hit BQ live).

Row-level security is enforced in BigQuery via **Authorized Views** — a `client_acme` service account can only see `org_gold.client_acme_*` views. PowerBI inherits this. No security logic in the BI layer.

#### 4.4.2 API for Third Parties — Cloud Run

Some partners want to pull data programmatically. Cloud Run serves a thin REST wrapper over Gold views.

```python
# cloud_run/serving_api/main.py
# Thin read-only wrapper over Gold. Auth → query → JSON. Nothing clever.

import os
from flask import Flask, request, jsonify, abort
from google.cloud import bigquery
from google.auth.transport import requests as grequests
from google.oauth2 import id_token

app = Flask(__name__)
_bq = bigquery.Client()

PROJECT  = os.environ["GCP_PROJECT"]
DATASET  = "org_gold"
AUDIENCE = os.environ["EXPECTED_AUDIENCE"]  # this service's own URL

# Whitelist. If it's not here, it can't be queried.
# No string-formatting table names from user input, ever.
ALLOWED_VIEWS = {
    "campaign-summary":  "api_campaign_summary",
    "daily-performance": "api_daily_performance",
}


def _authenticate(req) -> str:
    """Verify the Google-signed ID token. Return the client_id claim."""
    auth = req.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        abort(401)
    tok = auth.removeprefix("Bearer ")
    try:
        claims = id_token.verify_oauth2_token(tok, grequests.Request(), AUDIENCE)
    except ValueError:
        abort(401)
    client_id = claims.get("client_id")
    if not client_id:
        abort(403)
    return client_id


@app.get("/v1/<resource>")
def get_resource(resource: str):
    client_id = _authenticate(request)

    view = ALLOWED_VIEWS.get(resource)
    if view is None:
        abort(404)

    start = request.args.get("start_date")
    end   = request.args.get("end_date")
    if not start or not end:
        abort(400, "start_date and end_date are required")

    # Table name from whitelist. Dates via query parameters.
    # client_id from verified token — caller cannot spoof it.
    sql = f"""
        SELECT *
        FROM `{PROJECT}.{DATASET}.{view}`
        WHERE client_id  = @client_id
          AND event_date BETWEEN @start AND @end
        LIMIT 100000
    """
    job = _bq.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("client_id", "STRING", client_id),
                bigquery.ScalarQueryParameter("start",     "DATE",   start),
                bigquery.ScalarQueryParameter("end",       "DATE",   end),
            ],
            maximum_bytes_billed=10 * 1024**3,  # 10 GiB hard cap per request
        ),
    )

    return jsonify([dict(r) for r in job.result()])
```

Notes on this service:
- **Table names are whitelisted**, never interpolated from input. The f-string contains only values from `ALLOWED_VIEWS`.
- **`client_id` comes from the token**, not the query string. A caller physically cannot request another client's data.
- **`maximum_bytes_billed`** caps the blast radius of a malformed date range.
- Deployed with `min-instances=1` so first request isn't cold.

#### 4.4.3 Scheduled Exports — Reverse ETL

Some clients want files pushed to them. A Composer DAG runs a BigQuery `EXPORT DATA` statement to a client-specific GCS bucket, then a Cloud Function pushes it to their SFTP/S3/Azure Blob. One DAG per client, one delivery config per client, everything in version control.

---

## 5. The Four Priorities — How the Architecture Serves Them

### 5.1 Storage (Priority 1)

| Guarantee                           | Mechanism                                                                                          |
|-------------------------------------|----------------------------------------------------------------------------------------------------|
| Data cannot be lost                 | GCS multi-region: 11 nines durability. A region can burn down and we lose nothing.                 |
| Data cannot be deleted              | Bucket retention lock (7 years). Not even a compromised admin account can delete Bronze.           |
| Data cannot be silently overwritten | GCS Object Versioning. Every write creates a new generation; old ones are retained.                |
| Warehouse has an independent backup | BigQuery snapshots (7-day time-travel built in) + nightly table snapshots to a separate project.   |
| Lake is independent of warehouse    | Every source is exported to GCS Parquet. If BigQuery vanishes tomorrow, the lake is complete.      |

### 5.2 Availability (Priority 2)

| Component       | SLA         | Configuration                                                                |
|-----------------|-------------|------------------------------------------------------------------------------|
| GCS Bronze      | 99.95%      | Multi-region bucket.                                                         |
| BigQuery        | 99.99%      | Multi-region dataset (EU). Read replicas automatic.                          |
| Cloud Run API   | 99.95%      | Regional, min 2 instances, behind Global Load Balancer with Cloud CDN.       |
| Composer        | 99.9%       | Private-IP environment, 3 Airflow schedulers (HA mode).                      |
| Pub/Sub         | 99.95%      | Global service. 7-day message retention — if a subscriber dies, messages wait.|

**Degraded-mode behaviour.** If Dataflow is down, Bronze keeps filling (ingestion is decoupled). If BigQuery is down, PowerBI Import-mode dashboards keep working (data is cached in the .pbix). If Composer is down, no new loads happen but all serving continues. No single component failure takes the whole platform dark.

### 5.3 Accuracy (Priority 3)

| Risk                              | Control                                                                                    |
|-----------------------------------|--------------------------------------------------------------------------------------------|
| Duplicate rows                    | Content-hash dedup in Dataflow. `MERGE` (not `INSERT`) into Silver.                        |
| Schema drift from APIs            | Schema contract at the Silver boundary. Unknown fields → `_extras` JSON. Missing required fields → quarantine + alert. |
| Bad data reaching BI              | DQ gate *between* Dataflow and Silver. Gate fails → DAG halts → human investigates. No auto-retry past a DQ failure. |
| Silent row loss                   | Reconciliation task: `COUNT(*)` Bronze vs Silver vs Gold, per source, per day. Drift > 0.5% → alert. |
| Referential integrity drift       | dbt `relationships` tests on every foreign key, every run.                                 |
| Stale data presented as fresh     | Every Gold table has `_data_fresh_as_of` column. PowerBI surfaces it on every dashboard.   |

### 5.4 Reliability (Priority 4)

| Risk                              | Control                                                                                    |
|-----------------------------------|--------------------------------------------------------------------------------------------|
| API poll fails transiently        | Cloud Functions retry 3× with exponential backoff. Still failing → Pub/Sub DLQ.            |
| Poison message blocks the queue   | Pub/Sub dead-letter topic after 5 delivery attempts. Pipeline continues; poison is isolated.|
| Need to reprocess last Tuesday    | Bronze is immutable. Composer `airflow dags backfill --start-date 2026-03-03`. Idempotent. |
| Partial load leaves half a table  | Dataflow writes to a staging table, then atomic `MERGE`. Never a half-written Silver table.|
| Watermark lost                    | Watermarks in `org_silver._metadata_watermarks` (BigQuery table, not Airflow state). Survives Composer rebuild. |
| Nobody notices it's broken        | Freshness SLOs in Cloud Monitoring: "DV360 Silver partition for yesterday must exist by 08:00 or page the on-call." |

---

## 6. Capacity Model

**Input:** 50,000,000 rows/day.

| Metric                        | Value                             | Notes                                                    |
|-------------------------------|-----------------------------------|----------------------------------------------------------|
| Avg ingest rate               | ~578 rows/sec                     | Evenly spread.                                           |
| Peak ingest rate (assumed 10×)| ~5,800 rows/sec                   | Well under BigQuery streaming limit (1M rows/sec/table). |
| Raw volume (1 KB/row avg)     | ~50 GB/day → ~18 TB/year          | Before Parquet compression.                              |
| Bronze volume (Parquet ~5:1)  | ~10 GB/day → ~3.6 TB/year         | What actually lands in GCS.                              |
| Dataflow workers for daily batch | 20–40 (n1-standard-4)          | Processes 50M rows in ~30 min. Autoscales.               |
| BigQuery slot recommendation  | 500-slot annual commitment        | Covers daily transforms + PowerBI DirectQuery.           |
| BigQuery Silver size (1 year) | ~4 TB (after clustering/compression) | Partition pruning keeps queries cheap.                |

This is a **medium** data platform by cloud standards. Nothing here is pushing any limit. The architecture scales to 500M rows/day before we'd need to rethink anything.

---

## 7. Migration Path from Current State

We don't flip a switch. We run old and new in parallel and earn trust.

### Phase 0 — Foundations
- Land GCP org, projects (`-prod`, `-dev`), VPC, Cloud VPN to on-prem.
- IAM: service accounts per component, least-privilege, no human has write access to Bronze.
- Terraform for everything. No console clicking in prod.

### Phase 1 — Ingest in Shadow
- Turn on BQ Data Transfer for DV360. Data lands in Bronze. **Nobody looks at it yet.**
- Turn on Datastream CDC against SQL Server. Read-only. Legacy SQL queries continue untouched.
- Deploy Cloud Functions for Meta/TikTok. Bronze fills.
- **Success criterion:** 30 consecutive days of Bronze landing with zero gaps.

### Phase 2 — Parallel Run
- Build Silver + Gold for **one client** only.
- Analyst runs their existing Excel process *and* checks the PowerBI-on-BigQuery output. Daily.
- Every discrepancy is a bug. Fix the pipeline, not the spreadsheet.
- **Success criterion:** 14 consecutive days where Excel and BigQuery agree to the penny.

### Phase 3 — Client-by-Client Cutover
- Migrate one client per sprint. Previous clients stay on BigQuery.
- Legacy Excel process is **archived, not deleted** — we can fall back per-client.
- **Success criterion:** all clients on Gold, no Excel opened for 30 days.

### Phase 4 — Decommission
- SQL Server → read-only.
- After one quarter of clean operation, SQL Server → archived snapshot in GCS → powered off.
- **Success criterion:** the old server is off and nobody has noticed.

---

## 8. What We Deliberately Did Not Do

- **No Kubernetes.** Cloud Run and Dataflow are serverless. We do not need to run a cluster to move 50M rows.
- **No Kafka.** Pub/Sub is managed, has the same semantics for this use case, and we don't have to patch Zookeeper at 3am.
- **No real-time streaming to Gold.** The business runs on daily reporting cycles. Streaming to Bronze is enough; Silver and Gold are batch. We can add a streaming lane later if a client demands sub-hour freshness — the architecture supports it (Pub/Sub → Dataflow streaming → BigQuery) but we don't build it speculatively.
- **No custom schema-registry service.** Schema contracts live as JSON files in a GCS bucket, versioned. Boring. Works.
- **No ML platform.** Not in the brief. Gold tables are ML-ready if that becomes a requirement — clean, typed, partitioned.

---

## 9. Project Management — Agile Delivery with Two Teams

The architecture is built by two teams moving in parallel. One is dragging thousands of legacy SQL Server databases into the cloud; the other is wiring up the 50M-rows-a-day firehose. They have different rhythms, different failure modes, and different definitions of "done" — but they share a Silver layer, a Composer environment, and a release train. The project management approach has to make that sharing **explicit and scheduled**, or the two teams will discover their dependencies in production.

### 9.1 Team Topology

| | **Team A — Migration** | **Team B — Daily Ingestion** |
|---|---|---|
| **Mission** | Move historical state out of on-prem SQL Server into BigQuery Silver, without breaking the legacy analysts. | Land 50M fresh rows/day from DV360, third-party APIs, and file drops into the same Silver layer. |
| **Work shape** | Long tail of similar units (thousands of DBs). High repetition, low novelty after the first 50. | Small number of heterogeneous connectors. Low repetition, high novelty per source. |
| **Agile flavour** | **Kanban.** There is no natural sprint boundary in "migrate database #847 of 2,300." Continuous flow, WIP limits, cycle-time tracking. | **Scrum.** "Build the Meta Ads connector" is a sprint-sized story with a clear demo. Two-week sprints, standard ceremonies. |
| **Owns** | Datastream config, CDC Dataflow jobs, nested-table reshaping, `dim_*` tables, cutover runbooks. | BQ Data Transfer, Cloud Functions, Pub/Sub topology, `fact_*` tables, DQ gates, freshness SLOs. |
| **Phase mapping** | Drives Phases 1 → 4 (§7). Owns the decommission. | Drives Phases 0 → 2. Reaches steady state by Phase 3 and shifts to ops. |

Both teams are fed from a **single Product Backlog** owned by one Product Owner. This is the Nexus / LeSS principle: two teams, one backlog, one Definition of Done. Splitting the backlog guarantees the teams optimise locally and drift apart.

### 9.2 Where the Teams Cross Paths

These are the collision points. Each one is a **scheduled ceremony or a shared artefact** — never an ad-hoc Slack thread at 11pm.

```
Sprint timeline (2-week cadence, Team B drives the clock)
──────────────────────────────────────────────────────────────────────────────

 Day 1        Day 3-4          Day 8           Day 10          Day 14
   │             │                │               │               │
   ▼             ▼                ▼               ▼               ▼
┌──────┐   ┌──────────┐    ┌───────────┐   ┌───────────┐   ┌───────────┐
│Joint │   │ Schema   │    │ Mid-sprint│   │ Integration│   │ Joint     │
│Sprint│   │ Contract │    │ Dependency│   │ Env Merge  │   │ Review +  │
│Plan  │   │ Review   │    │ Check     │   │ (staging)  │   │ Retro     │
└──────┘   └──────────┘    └───────────┘   └───────────┘   └───────────┘
   │             │                │               │               │
   │             │                │               │               │
Team A ─────────●────────────────●───────────────●───────────────●─────▶
(Kanban        silver            watermark       composer        cutover
 flow)         schema            table           DAG slot        sign-off

Team B ────────●────────────────●───────────────●───────────────●─────▶
(Scrum         silver            DQ gate         composer        demo
 sprint)       schema            thresholds      DAG slot        fresh data
```

#### Intersection 1 — The Silver Schema Contract (continuous, formalised Day 3-4)

**What it is.** The `org_silver.dim_campaign` table (§4.1.2) is written by Team A's CDC pipeline. The `org_silver.fact_impressions` table is written by Team B's DV360 pipeline. But `fact_impressions.campaign_id` is a foreign key into `dim_campaign`. If Team A renames a column or changes a datatype, Team B's dbt `relationships` test goes red.

**The ceremony.** A **Schema Contract Review** on Day 3-4 of every sprint. Both teams bring their planned Silver DDL changes. Changes are merged into a versioned JSON-schema file in GCS (the "boring schema registry" from §8) *before* any code is written. A schema change without a contract PR is a blocked ticket.

**Why it's early in the sprint.** Discovering a contract break on Day 12 means Team B re-plans mid-sprint. Discovering it on Day 3 means Team B plans around it from the start.

#### Intersection 2 — The Watermark Table (continuous, checked Day 8)

**What it is.** `org_silver._metadata_watermarks` (§4.3.1, §5.4) records how far each pipeline has loaded. Team A's CDC jobs write their LSN high-water mark here. Team B's daily DAGs write their partition-date high-water mark here. The Gold-layer dbt models read this table to decide what is safe to materialise — a Gold fact table will **not** build if the dimension it joins to has a stale watermark.

**The ceremony.** A **Mid-Sprint Dependency Check** (15 minutes, async-friendly — can be a dashboard review). Both leads confirm their watermarks are advancing and there is no widening gap between dimension freshness and fact freshness. A widening gap means one team is blocked and the other hasn't noticed.

**The failure mode this prevents.** Team B ships a connector that loads 50M impression rows referencing `campaign_id = 'C-9912'`. Team A hasn't migrated that client's campaign dimension yet. The join produces NULLs, the dbt test fails, the DAG halts, PagerDuty fires, and both teams lose a day arguing about whose fault it is. The dependency check catches this **before** the DAG runs.

#### Intersection 3 — Composer DAG Namespace and Schedule (Day 10)

**What it is.** Both teams deploy Airflow DAGs into the **same Cloud Composer environment** (§4.3.1). Team A's `sqlserver_cdc_merge` DAGs and Team B's `dv360_daily` DAGs share the scheduler, the worker pool, and — critically — the 04:00–07:00 load window before analysts arrive.

**The ceremony.** An **Integration Environment Merge** on Day 10. Both teams deploy their sprint's DAG changes to a shared staging Composer. A full overnight cycle runs. DAG-name collisions, pool exhaustion, and schedule overlaps surface here, not in prod.

**Why Day 10 and not Day 14.** Four days of buffer to fix what breaks. Merging on the last day of the sprint means broken staging becomes broken prod.

#### Intersection 4 — Data Quality Gate Thresholds (negotiated, revisited at Retro)

**What it is.** The DQ gate (§4.3.1, §5.3) halts a DAG if row counts drift >2% or null-rates spike. Team A's historical backfills legitimately produce 40M-row days followed by 200-row days — that's not an anomaly, that's a migration finishing a database. Team B's daily loads should be boringly consistent — a 40% row-count drop **is** an anomaly.

**The artefact.** Per-source DQ thresholds live in a shared config file. Team A owns the `sqlserver/*` thresholds, Team B owns everything else. But the **escalation policy is joint** — a DQ halt pages one on-call rota, not two. Whoever is on-call needs enough context from both teams to triage.

**The ceremony.** DQ false-positive rate is a standing agenda item in the **Joint Retro**. If Team A's backfills triggered five false alarms this sprint, that's a threshold to loosen. If Team B's connector let a bad batch through, that's a threshold to tighten.

#### Intersection 5 — Client Cutover Sequencing (per client, Phase 3)

**What it is.** §7 Phase 3 migrates one client per sprint. A client is "cut over" when (a) Team A has their historical dimensions in Silver *and* (b) Team B has their daily facts flowing *and* (c) the two join cleanly in Gold *and* (d) an analyst has signed off the parallel run.

**The ceremony.** Cutover is a **cross-team User Story** with acceptance criteria owned by both teams. It sits at the top of the shared backlog for that sprint. Neither team can close it alone. This is the single strongest forcing function for collaboration — you cannot ship your half and walk away.

**The artefact.** A **Cutover Readiness Checklist** per client, visible on the shared board:

```
Client: ACME Corp — Target cutover: Sprint 14
─────────────────────────────────────────────────────────────
[Team A]  Historical dims loaded          ██████████  100%
[Team A]  CDC steady-state (7 days clean) ████████░░   80%
[Team B]  Daily facts landing             ██████████  100%
[Team B]  DQ gate tuned for client volume ██████░░░░   60%
[Joint]   Gold join produces 0 orphan FKs ░░░░░░░░░░    0%  ← BLOCKED
[Analyst] Parallel-run sign-off           ░░░░░░░░░░    —
```

#### Intersection 6 — Infrastructure-as-Code Monorepo (continuous)

**What it is.** Terraform for GCS buckets, BigQuery datasets, IAM, Pub/Sub, Composer. One repo. Both teams commit to it.

**The mechanism.** Not a ceremony — a **branch protection rule**. Any PR touching `terraform/shared/` (the Silver dataset, the Composer env, the Bronze bucket IAM) requires one approver from each team. GitHub CODEOWNERS enforces it. The teams cross paths in code review whether they planned to or not.

### 9.3 Scrum-of-Scrums Cadence

The intersections above plug into a lightweight scaling layer:

| Ceremony | Cadence | Attendees | Duration | Output |
|---|---|---|---|---|
| Joint Sprint Planning | Day 1 of sprint | Both full teams + PO | 90 min | Sprint goal phrased as a **platform** outcome, not two team outcomes. |
| Scrum-of-Scrums | 3×/week (Mon/Wed/Fri) | One delegate per team | 15 min | Dependency flags raised. Escalation to PO if unresolved in 24h. |
| Schema Contract Review | Day 3-4 | Tech leads + data architect | 30 min | Approved schema diff for the sprint. |
| Mid-Sprint Dependency Check | Day 8 | Tech leads | 15 min (async) | Watermark dashboard green, or a ticket raised. |
| Integration Env Merge | Day 10 | Both teams (hands-on) | Half-day | Staging Composer running both teams' DAGs end-to-end. |
| Joint Sprint Review | Day 14 | Both teams + PO + 1 analyst | 60 min | Demo runs Bronze → Gold → PowerBI using **one team's history and the other team's fresh data in the same query**. |
| Joint Retro | Day 14 | Both full teams | 45 min | Cross-team friction surfaced. DQ false-positive rate reviewed. |

Team A runs its internal Kanban standup daily and independently — the migration factory doesn't stop for Team B's sprint boundaries. Team B runs its internal Scrum standup daily. The ceremonies above are the **only** mandatory joint sessions; everything else is pull-based.

### 9.4 Recommended Tooling

The tool has to do three things well: (1) render a Scrum board and a Kanban board from the **same backlog**, (2) model cross-team dependencies as first-class objects, and (3) sit close to the code so a schema-contract PR and the ticket that demanded it are one click apart.

| Tool | Scrum + Kanban from one backlog | Cross-team dependency links | Git / PR integration | Scaling framework support | Verdict |
|---|---|---|---|---|---|
| **Jira Software** (+ Advanced Roadmaps) | Native — one Project, two Boards, shared filter | Native — "is blocked by" links surface on the roadmap as red lines between team swimlanes | Deep (Bitbucket native, GitHub/GitLab via app) — ticket ↔ branch ↔ PR ↔ deploy | Advanced Roadmaps is built for exactly this: two-team capacity planning, dependency visualisation, shared releases | **Primary recommendation.** The dependency-line view is the killer feature — the PO sees intersections 1–5 as literal lines on a Gantt-style timeline. |
| **Azure DevOps Boards** | Native — Boards per Team under one Project, shared Area Path | Native — Predecessor/Successor links, Delivery Plans for cross-team view | Deep (Azure Repos native, GitHub via extension) | Delivery Plans overlays both teams' iterations and flags dependency conflicts | **Strong alternative**, especially if the org is already on Azure AD / M365 (likely, given the PowerBI estate). Delivery Plans ≈ Advanced Roadmaps. |
| **Linear** | Good — Projects + Cycles for Scrum, Projects without Cycles for Kanban, shared Roadmap | Adequate — "blocks / blocked by" relations, but no dedicated cross-team dependency canvas | Excellent — the GitHub sync is the tightest on the market; PR status drives ticket status automatically | Weaker — designed for single-product teams, scaling is DIY | Use if both teams are small (≤5 each) and engineering-led. The speed and Git sync outweigh the missing roadmap view at that size. |
| **GitHub Projects** | Adequate — custom fields fake a sprint, but no native velocity/burndown | Weak — issue links exist, no dependency visualisation | Perfect — it *is* the repo | None | Fine as a **supplement** for the IaC-CODEOWNERS intersection (#6). Not enough on its own for intersections #1–#5. |
| **Monday.com / Asana** | Good boards | Weak code integration | Shallow | Decent | Not recommended here — these are generalist PM tools. The distance from the codebase hurts when a schema PR needs to block a ticket. |

**Recommendation: Jira + Advanced Roadmaps, with GitHub as the repo and Confluence for the Cutover Readiness Checklists.**

Concrete setup:
- **One Jira Project**, two Boards (`Migration — Kanban`, `Ingestion — Scrum`), one Backlog.
- **Components** (`silver-schema`, `composer-dags`, `watermarks`, `dq-gates`, `cutover`) tag every ticket with the intersection it touches. A saved filter on each component is the agenda for that intersection's ceremony.
- **Advanced Roadmaps Plan** spanning both teams. Dependency links are added at Joint Sprint Planning; the Plan view is the Scrum-of-Scrums dashboard.
- **Jira ↔ GitHub app** so the schema-contract PR auto-transitions the Jira ticket and the CODEOWNERS approval is visible on the ticket timeline.
- **Confluence page per client** holding the Cutover Readiness Checklist, embedded live from a Jira filter. The analyst signs off on the Confluence page; the sign-off macro closes the Jira epic.

The tooling is deliberately boring. The value is in the six intersections being **named, scheduled, and owned** — the tool just makes them visible.

### 9.5 Metrics — What "On Track" Actually Looks Like

The two teams run different Agile flavours, so their **process** metrics are different. But they build one platform, so the **outcome** metrics are shared — and those shared metrics are the ones the PO takes to the steering committee. A team can have perfect velocity and still be failing if the shared numbers aren't moving.

We track three tiers: per-team flow, cross-team integration health, and platform outcome. Each tier answers a different question.

```
                 ┌─────────────────────────────────────┐
   Tier 3        │   PLATFORM OUTCOME (shared)         │   "Is the business
   (monthly)     │   % clients cut over, % Excel       │    better off?"
                 │   retired, analyst hours saved      │
                 └──────────────────▲──────────────────┘
                                    │ only moves if Tier 2 is green
                 ┌──────────────────┴──────────────────┐
   Tier 2        │   INTEGRATION HEALTH (shared)       │   "Do the two halves
   (per sprint)  │   Orphan-FK rate, contract-break    │    actually join?"
                 │   count, joint-story completion %   │
                 └────────▲──────────────────▲─────────┘
                          │                  │ only moves if Tier 1 is healthy
              ┌───────────┴──────┐  ┌────────┴───────────┐
   Tier 1     │  TEAM A FLOW     │  │  TEAM B FLOW       │   "Is each team
   (weekly)   │  (Kanban)        │  │  (Scrum)           │    shipping?"
              │  Cycle time,     │  │  Velocity,         │
              │  throughput, WIP │  │  burndown, churn   │
              └──────────────────┘  └────────────────────┘
```

#### Tier 1 — Per-Team Flow Metrics (different per team, reviewed weekly)

These measure whether each team's engine is running smoothly. They are **not** comparable across teams — comparing Kanban cycle time to Scrum velocity is meaningless. Each team owns its own Tier 1 dashboard and brings it to its own standup.

**Team A — Migration (Kanban metrics)**

| Metric | Definition | Target trajectory | Why it matters here |
|---|---|---|---|
| **Cycle time per DB** | Wall-clock from "DB picked up" → "CDC steady-state for 7 days" | ↓ sharply over first 10 sprints, then plateau | The first 50 databases are learning. After that, the process should be a factory — a rising cycle time means the easy DBs are done and the gnarly ones remain. That's a signal to re-triage the backlog, not to panic. |
| **Throughput** | DBs reaching steady-state per week | ↑ until plateau, then flat | This is the number that feeds the decommission forecast. At 2,300 DBs, throughput of 40/week = ~14 months. The PO uses this to set stakeholder expectations. |
| **WIP** | DBs currently between "started" and "steady-state" | Flat, at the WIP limit | If WIP creeps above the limit, DBs are starting but not finishing — usually a sign that CDC validation is bottlenecked on a shared resource (often a single person who knows the legacy schema). |
| **Rework rate** | % of DBs that re-enter the pipeline after being marked steady-state | ↓ toward <5% | A DB bouncing back means the "7 days clean" bar was passed but something broke later. High rework = the Definition of Done is too loose. |
| **Schema-variant count** | Distinct nested-table shapes encountered so far | ↑ fast then flatten (logarithmic) | 2,300 databases will not have 2,300 schemas — probably 30–50 variants. Once the curve flattens, every new DB is a known shape and cycle time should drop. If it keeps climbing linearly, the legacy estate is messier than scoped. |
| **Blocked-column age** | Avg days a DB ticket sits in "Blocked" | ↓ | Migration blocks are almost always external: legacy DBA unavailable, VPN flapping, client hasn't approved the maintenance window. This metric surfaces organisational friction, not engineering friction. |

**Team B — Daily Ingestion (Scrum metrics)**

| Metric | Definition | Target trajectory | Why it matters here |
|---|---|---|---|
| **Velocity** | Story points completed per sprint | Stabilise by Sprint 4, then flat | Used for forecasting only. Never compared to Team A. Never used as a performance target (Goodhart's law applies instantly). |
| **Sprint goal hit rate** | % of sprints where the stated goal was met | ↑ toward >80% | The goal is phrased as a platform outcome (§9.3). Missing it repeatedly means either planning is over-optimistic or Team A dependencies are landing late. |
| **Scope churn** | % of stories added/removed after Day 1 | ↓ toward <15% | High churn often traces back to Intersection #1 — a schema change discovered mid-sprint forces re-planning. If churn stays high, the Schema Contract Review isn't working. |
| **Escaped defect rate** | Bugs found in prod that should have been caught by the DQ gate | ↓ toward zero | Directly measures whether §5.3 controls are real. One escaped defect = one DQ threshold to tighten at the next Retro. |
| **Connector MTTR** | Mean time from "freshness SLO breached" → "data flowing again" | ↓ | This is the Reliability priority (§5.4) expressed as a team metric. A rising MTTR on a mature connector means tribal knowledge is concentrated in one engineer. |
| **DQ false-positive rate** | % of DQ-gate halts that were not real data problems | ↓ toward <10% | The other side of escaped defects. Too many false alarms → on-call fatigue → real alarms get ignored. Reviewed jointly at Retro (Intersection #4). |

#### Tier 2 — Integration Health Metrics (shared, reviewed at Scrum-of-Scrums and Joint Review)

These only exist **because there are two teams**. A single team building the whole platform would not need them. They measure the seams — intersections #1 through #6 — and they are the leading indicators for Tier 3. If Tier 2 goes red, Tier 3 will go red two sprints later.

| Metric | Definition | Target | Which intersection it watches | What a bad number tells you |
|---|---|---|---|---|
| **Orphan-FK rate** | % of `fact_*` rows where the FK join to a `dim_*` row returns NULL | <0.1% per client before cutover | #2, #5 | Team B is landing facts for entities Team A hasn't migrated yet. Either the migration sequence is wrong or the watermark check was skipped. |
| **Schema-contract break count** | PRs merged to Silver DDL without an approved contract-review ticket | 0 per sprint | #1 | The Day 3-4 ceremony is being bypassed. Usually happens under deadline pressure. One break is a conversation; two is a process failure. |
| **Staging integration pass rate** | % of Day-10 staging runs where both teams' DAGs complete end-to-end first try | ↑ toward >90% | #3 | Low = teams are developing in isolation and discovering conflicts late. The Day-10 merge is too late; consider moving to Day 7. |
| **Cross-team PR review latency** | Hours from "PR opened on `terraform/shared/`" → "approval from the other team" | <8 working hours | #6 | If this climbs, CODEOWNERS has become a bottleneck rather than a safety net. Usually means the designated reviewers are overloaded. |
| **Joint-story completion rate** | % of cross-team cutover stories (Intersection #5) closed within their target sprint | ↑ toward >75% | #5 | The single most honest integration metric. Both teams can individually hit every Tier 1 target and still fail this — which is exactly the scenario Tier 2 exists to catch. |
| **Dependency-caused slippage** | Story points Team B deferred because a Team A deliverable wasn't ready (and vice versa) | ↓ toward <10% of velocity | #2 | Makes the cost of poor sequencing visible in the unit each team already tracks. |
| **Watermark gap** | Max(fact freshness) − Min(dim freshness), in hours, measured daily | <24h steady-state | #2 | A widening gap means Gold is building on stale dimensions. The dbt gate will eventually halt the build — this metric sees it coming. |
| **Escalation count** | Issues raised at Scrum-of-Scrums that required PO intervention | ↓ over time | all | Early sprints: high is fine, the teams are learning each other's edges. Late sprints: high means the ceremonies aren't resolving friction and it's flowing uphill. |

#### Tier 3 — Platform Outcome Metrics (shared, reviewed monthly with stakeholders)

These are the only numbers anyone outside the two teams should ever see. They map directly to §7's migration phases and to the four priorities in §5. Tier 1 and Tier 2 are **how**; Tier 3 is **whether**.

| Metric | Definition | Target | Maps to |
|---|---|---|---|
| **% legacy DBs in CDC steady-state** | Databases with 7+ consecutive clean days, as fraction of total estate | S-curve: slow start, steep middle, long tail | §7 Phase 1. Primarily Team A, but gated by shared Composer capacity. |
| **% clients cut over** | Clients where the analyst has signed the parallel-run sheet and Excel is archived | Linear once Phase 3 starts: ~1 per sprint | §7 Phase 3. The headline number. Only moves when a joint story closes — **neither team can move this alone.** |
| **% daily volume landing through the new pipeline** | Rows/day arriving via Cloud Functions + BQ Transfer + Datastream, vs. total 50M target | → 100% by end of Phase 2 | §7 Phase 2. Primarily Team B, but includes CDC volume from Team A. |
| **Parallel-run discrepancy rate** | % of daily reconciliation checks where Excel ≠ BigQuery by more than £0.01 | → 0% for 14 consecutive days per client before cutover | §7 Phase 2, §5.3 Accuracy. The trust metric. A client does not cut over until this is zero. Both teams' correctness feeds it. |
| **End-to-end freshness** | Age of youngest row visible in a PowerBI Gold dashboard at 08:00 | <4 hours | §5.2 Availability, §5.4 Reliability. Bronze (Team B) → Silver merge (both) → Gold dbt (shared) → PowerBI refresh. A miss can originate in either team; the metric doesn't care whose. |
| **Analyst hours on manual ETL** | Self-reported hours/week the analytics team spends in Excel on data prep (not analysis) | ↓ toward zero | The business case. If this isn't falling, the platform is a cost centre regardless of what Tiers 1–2 say. |
| **Unplanned downtime** | Hours/month where Gold was stale past SLO and no planned maintenance was running | <2 hrs/month | §5.2, §5.4. Jointly owned because an outage in either team's DAGs stalls Gold equally. |

#### Reading the three tiers together

The tiers form a diagnostic chain. Read top-down to report, bottom-up to debug:

- **Tier 3 green, Tier 2 green, Tier 1 green** — steady state. Publish the Tier 3 dashboard, leave the teams alone.
- **Tier 3 green, Tier 2 red** — trouble incoming. Integration is degrading but outcomes haven't caught up yet. Fix Tier 2 now; you have roughly two sprints before Tier 3 notices.
- **Tier 3 red, Tier 2 green, Tier 1 green** — the teams are executing flawlessly on the wrong plan. This is a backlog-sequencing problem, not a delivery problem. The PO re-orders the backlog; the teams keep shipping.
- **Tier 1 red on one team, Tier 2 green** — local problem, local fix. The other team does not need to slow down; the joint ceremonies absorb the slack.
- **Tier 1 green on both, Tier 2 red** — the most dangerous pattern. Both teams feel productive, standup boards are all green, and yet the seams are splitting. This is what Tier 2 exists to catch, and it is why the Joint Review demo (§9.3) must always run **one query that joins Team A's dimensions to Team B's facts** — it is the only moment in the sprint where the seam is tested in front of everyone.
