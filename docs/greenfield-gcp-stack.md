# Greenfield GCP data stack (2026) — design reference

Recommended tooling for a greenfield GCP data platform: one line per tool (what it is / why), with
acronyms expanded. Companion to [`../airflow/architecture.md`](../airflow/architecture.md).

## Ingestion
- **Datastream** — serverless **CDC** (Change Data Capture) that streams row-level DB changes into BigQuery in near-real-time.
- **BigQuery Data Transfer Service (DTS)** — scheduled managed loads from SaaS/Google sources (e.g. Google Ads) into BigQuery.
- **Fivetran** — managed connectors for SaaS sources (ads, AppsFlyer, etc.) → BigQuery.
- **Pub/Sub** — GCP's managed pub/sub message bus; the native streaming ingress into GCP.
- **Dataflow** — managed Apache Beam runner for stateful/streaming transforms (windowing, exactly-once).
- **DLQ** (Dead-Letter Queue) — a side topic/table for messages that fail processing, so the pipeline doesn't block or lose data.
- **Storage Write API** — BigQuery's high-throughput streaming-insert API (replaces legacy streaming inserts).

## Transform
- **dbt Core** — SQL transformation framework (staging → marts) run on BigQuery; tests, contracts, docs.
- **Incremental + `insert_overwrite`** — only rebuild touched date **partitions** each run → idempotent + cheap.
- **Contracts** — enforce a model's column names/types so breaking changes fail fast.
- **dbt Fusion / dbt Core v2** — Rust rewrite (Apache 2.0, alpha mid-2026): real-time SQL validation without a warehouse run, ~30x faster parsing, state-aware runs. Great for dev; BigQuery adapter still **beta**.

## Orchestration
- **Cloud Composer** — GCP-managed **Apache Airflow**; mature, sensors, huge ecosystem.
- **Dagster** — asset-centric orchestrator; each dbt model is an **asset** with native lineage; best dbt-native fit.
- **Cloud Run Jobs** — serverless container jobs (scale-to-zero, ≤24h); run the dbt image without a cluster.
- **KubernetesPodOperator (KPO)** — Airflow operator that runs a container (your dbt image) as a pod.
- *(Temporal = durable **application** workflow engine, not analytics ELT — wrong layer for dbt→BQ.)*

## Governance & data security (≈ AWS Lake Formation)
- **Dataplex** — GCP's data governance/catalog plane (taxonomies, quality, lineage, classification).
- **Policy tags** — labels in a Dataplex taxonomy applied to columns → **column-level access control** (BigQuery column security).
- **Dynamic data masking** — role-based masking (nullify/hash/last-4/UDF) at query time via a data policy on a tag; no data copy.
- **Row access policies** — row-level filtering by principal (≈ Lake Formation row filters).
- **DLP / Sensitive Data Protection** — auto-discovers & classifies PII so you can tag/mask it.
- **Fine-Grained Reader vs Masked Reader** — IAM roles on a tag: see raw vs see masked.

## Lineage
- **Dataplex Data Lineage** — auto-captured BigQuery **table- and column-level** lineage from query jobs (lags ~30 min–24 h; gaps for load jobs/routines/external tables).
- **dbt docs / DAG + exposures** — model-level lineage from `ref()`/`source()` and downstream BI consumers.
- **OpenLineage** — open lineage standard (Airflow/dbt emit events) for cross-tool lineage; **Atlan/Monte Carlo** for enterprise catalogs.

## Observability & monitoring
- **Cloud Logging** — central logs; dbt run as a container emits `--log-format json` to stdout → auto-ingested.
- **Log-based metrics** — counters/distributions derived from logs (error counts, model runtimes).
- **Cloud Monitoring** — dashboards + **alerting policies** (→ Slack via Pub/Sub/webhook).
- **dbt `run_results.json`** — per-model `execution_time` + `rows_affected`; ship to logs or a BQ audit table.
- **Elementary** — dbt-native observability: run/test results, volume/freshness/dimension **anomaly detection**, Slack alerts.
- **BigQuery ML `ARIMA_PLUS`** — time-series model for anomaly detection on row-counts/volume metrics.

## Network & platform security
- **VPC** (Virtual Private Cloud) — your private software-defined network.
- **VPC-SC** (VPC **Service Controls**) — a security **perimeter** around BigQuery/GCS etc. that blocks data **exfiltration** even with leaked credentials. *(SC = Service Controls.)*
- **Private Google Access / PSC** (Private Service Connect) — reach Google APIs/BigQuery over Google's backbone, no public IPs.
- **Cloud NAT** (Network Address Translation) — controlled **egress-only** internet access for private workloads.
- **Workload Identity / WIF** (Workload Identity **Federation**) — **keyless** auth: short-lived tokens instead of service-account JSON keys.
- **OIDC** (OpenID Connect) — the token standard GitHub uses to prove identity to GCP for keyless CI/CD.
- **CMEK** (Customer-Managed Encryption Keys) — encrypt BQ/GCS with keys you control in Cloud KMS.
- **Secret Manager** — managed store for DB creds/API keys (never in code).

## IaC & CI/CD
- **Terraform** — declarative infrastructure; **per-component state** in a GCS backend with locking (small blast radius, no clobbering).
- **WIF-based CI** — GitHub Actions authenticates keyless via OIDC→WIF, impersonates a least-privilege per-env Terraform SA.
- **CI gates** — ruff (Python), SQLFluff (SQL), DAG-integrity (DagBag import), `dbt build` on DuckDB, Artifact Registry image scanning + Trivy.

## AI / ML enablement
- **BigQuery ML (BQML)** — train + predict with **SQL inside BigQuery** (regression, boosted trees, k-means, **ARIMA_PLUS** forecasting/anomaly) — no data movement.
- **Gemini in SQL** — `AI.GENERATE` / `AI.GENERATE_TABLE` (GA) call a Gemini model from a query to summarise, classify, extract, or analyse text/image/audio/video/PDF (default `gemini-2.5-flash`, supports Gemini 3).
- **`ML.GENERATE_EMBEDDING`** — turn text/images into vector **embeddings** in BigQuery for semantic use.
- **`AI.SIMILARITY` / `VECTOR_SEARCH`** — semantic search: one-step similarity, or `VECTOR_SEARCH` + a vector index to scale to billions of rows (the basis for **RAG**).
- **RAG** (Retrieval-Augmented Generation) — ground an LLM on *your* governed data (embeddings + vector search) so answers cite real facts instead of hallucinating.
- **Vertex AI Feature Store** (BigQuery-powered) — serve dbt-built feature tables (and embeddings) online with low latency; keeps **training/serving consistent**.
- **Vertex AI** *(rebranded "Gemini Enterprise Agent Platform", Cloud Next 2026)* — the ML platform: **Model Garden** (Gemini/Claude/Llama/open models), custom + **AutoML** training, **Model Registry**, **Endpoints** (online/batch), **Pipelines** (repeatable training), **Model Monitoring** (drift/skew), **Model Evaluation**.
- **Vertex AI Vector Search** — managed low-latency **ANN** (Approximate Nearest Neighbour) index for production RAG/recommendations beyond in-BQ search.
- **Agent Builder + ADK** (Agent Development Kit) — build/deploy/govern production **AI agents** grounded on your data.
- **Object Tables + multimodal** — BigQuery tables over **unstructured** GCS files (images/PDF/audio) so `AI.GENERATE` / Document AI can analyse them in SQL.
- **Notebooks** — Colab Enterprise / Vertex Workbench / BigQuery Studio, natively wired to BigQuery for data + AI on one surface.
- **AI-ready data (the data engineer's job)** — clean, **contracted**, documented dbt marts + a **semantic layer** (consistent metric definitions) + **Dataplex** governance/lineage + PII masking *before* embedding → the trustworthy foundation models and agents consume.

---

## Cross-cloud ingestion from AWS

**CDC from AWS RDS / Aurora → BigQuery: use Datastream.** It supports **RDS & Aurora PostgreSQL and MySQL**:
- **PostgreSQL** — `pglogical`/logical replication: set replica identity, create a publication + replication slot.
- **MySQL** — row-based binlog replication with adequate binlog retention.
- It writes change events **straight into BigQuery** (handles upserts) — no Dataflow needed.

**Best security when ingesting from AWS (prefer private, encrypted, least-privilege):**
- **HA VPN** (highly-available IPsec tunnel) GCP↔AWS, or **Dedicated/Partner Interconnect** — keep CDC traffic off the public internet; Datastream uses **Private Connectivity** (VPC peering) and the RDS **internal** IP.
- If public path is unavoidable: **TLS/SSL** (server + client certs) + **IP allowlist** to Datastream's regional IPs + tight AWS security groups.
- **Least-privilege replication user** (replication + SELECT on needed tables only); creds in **Secret Manager**; **VPC-SC** around BigQuery on the GCP side.

**Messages / events from AWS:**
- **Kinesis → Dataflow** — Beam **`KinesisIO`** reads Kinesis directly (enhanced fan-out); native path into BigQuery.
- **Kafka / MSK → Dataflow** — Beam **`KafkaIO`** if you run Kafka on AWS.
- **EventBridge → Pub/Sub** — Dataflow has **no native EventBridge source**, so bridge it to Pub/Sub:
  - EventBridge **API Destinations** (HTTPS + OAuth) → a Cloud Run/Functions endpoint that publishes to Pub/Sub, **or**
  - EventBridge → **Lambda** → Pub/Sub (client publish), **or**
  - EventBridge → **Kinesis** → Dataflow `KinesisIO`.
  - Pub/Sub is the GCP-native ingress, so bridging EventBridge→Pub/Sub is the cleanest; then Pub/Sub → Dataflow/BigQuery.
