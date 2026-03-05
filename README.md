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
