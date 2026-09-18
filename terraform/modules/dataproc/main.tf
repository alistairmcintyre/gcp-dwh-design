terraform {
  required_version = ">= 1.5"
  required_providers {
    google = { source = "hashicorp/google", version = ">= 6.0" }
  }
}

# ------------------------------------------------------------------------------------------------
# Infrastructure for the Dataproc Serverless ETL framework (../../spark).
#
# Deliberately small: Serverless means there is no cluster, no autoscaling policy and no node pool
# to manage. What is left is the things a batch needs to exist and to be allowed to do its job --
# an identity, a staging bucket, and the least privilege that identity can get away with.
# ------------------------------------------------------------------------------------------------

variable "project_id" { type = string }
variable "region" { type = string }

variable "staging_bucket_name" {
  description = "Bucket for batch dependencies, the framework zip, and job specs."
  type        = string
}

variable "readable_datasets" {
  description = "BigQuery datasets the ETL identity may read."
  type        = list(string)
  default     = []
}

variable "writable_datasets" {
  description = "BigQuery datasets the ETL identity may write. Kept separate from readable on purpose: most jobs read far more than they write, and a single 'dataEditor everywhere' grant is how a batch job ends up able to truncate the warehouse."
  type        = list(string)
  default     = []
}

variable "labels" {
  type    = map(string)
  default = {}
}

resource "google_service_account" "etl" {
  project      = var.project_id
  account_id   = "dataproc-etl"
  display_name = "Dataproc Serverless ETL framework"
  description  = "Runs config-driven Spark batches. Read/write scoped per dataset, not per project."
}

resource "google_storage_bucket" "staging" {
  project  = var.project_id
  name     = var.staging_bucket_name
  location = var.region
  labels   = var.labels

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Dataproc leaves per-batch dependency artefacts behind. Without a lifecycle rule this bucket
  # grows forever and nobody notices until it is a line item.
  lifecycle_rule {
    condition { age = 30 }
    action { type = "Delete" }
  }

  versioning { enabled = false }
}

resource "google_storage_bucket_iam_member" "etl_staging" {
  bucket = google_storage_bucket.staging.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.etl.email}"
}

# Serverless batches need this at project level: it covers creating the batch, writing its logs and
# reporting metrics. Data access is granted per dataset below rather than here.
resource "google_project_iam_member" "etl_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.etl.email}"
}

resource "google_project_iam_member" "etl_bq_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.etl.email}"
}

resource "google_bigquery_dataset_iam_member" "etl_read" {
  for_each = toset(var.readable_datasets)

  project    = var.project_id
  dataset_id = each.value
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.etl.email}"
}

resource "google_bigquery_dataset_iam_member" "etl_write" {
  for_each = toset(var.writable_datasets)

  project    = var.project_id
  dataset_id = each.value
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.etl.email}"
}

output "service_account_email" { value = google_service_account.etl.email }
output "staging_bucket" { value = google_storage_bucket.staging.name }
