terraform {
  required_version = ">= 1.5"
  required_providers {
    google = { source = "hashicorp/google", version = ">= 6.0" }
  }
}

# ------------------------------------------------------------------------------------------------
# Vertex AI Feature Store: the handoff point between data engineering and ML.
#
# WHY A DATA ENGINEER OWNS THIS
# Training/serving skew is the classic silent ML failure: a model trained on one definition of a
# feature and served another. It is a DATA problem, not a modelling one. Feature Store fixes it
# structurally -- the offline path (training) reads the BigQuery table directly, the online path
# (serving) reads a synced copy of THAT SAME TABLE. One definition, maintained in dbt, under the
# same tests and contracts as the rest of the warehouse.
#
# THE THREE OBJECTS, WHICH ARE EASY TO CONFUSE
#   Feature Group        metadata over an existing BigQuery table. Registers WHICH table, which
#                        column is the entity id, and which columns are features. Stores no data.
#   Feature Online Store the low-latency serving layer. Actually holds data.
#   Feature View         the sync: takes a Feature Group and keeps it materialised in an Online
#                        Store on a schedule.
#
# COST WARNING, because this differs from everything else in this repo: an Online Store bills
# CONTINUOUSLY while it exists, not per query. Optimized serving is the cheaper of the two options;
# Bigtable-backed costs more and is only worth it at high, sustained QPS. Destroy it when not in use.
# ------------------------------------------------------------------------------------------------

variable "project_id" { type = string }
variable "region" { type = string }

variable "online_store_name" {
  type    = string
  default = "customer_features"
}

variable "feature_group_name" {
  type    = string
  default = "customer_activity"
}

variable "source_table" {
  description = "BigQuery table backing the feature group, as project.dataset.table. Must expose the entity id column and a `feature_timestamp` column."
  type        = string
}

variable "entity_id_column" {
  type    = string
  default = "user_id"
}

variable "features" {
  description = "Feature column names to register. Listed explicitly rather than inferred, so adding a column to the table is not silently also a change to the serving contract."
  type        = list(string)
}

variable "sync_cron" {
  description = <<-EOT
    How often the online store is refreshed from BigQuery. This is the freshness SLO of the serving
    layer, and it should be derived from what the consuming model actually needs -- not set to
    hourly out of habit. Frequent syncs of a large feature table are one of the easier ways to spend
    money on this service without anyone noticing.
  EOT
  type        = string
  default     = "0 8 * * *"
}

resource "google_vertex_ai_feature_online_store" "this" {
  provider = google-beta

  project = var.project_id
  region  = var.region
  name    = var.online_store_name

  # Optimized (serverless) rather than Bigtable: no nodes to size, scales down, and it is the right
  # default until sustained QPS proves otherwise. Bigtable-backed serving wins at high, steady load
  # and costs materially more at rest.
  optimized {}

  # Demo convenience. In a real environment this stays false -- an online store deleted from under a
  # live serving path is an outage, not a tidy-up.
  force_destroy = true
}

# Registers the BigQuery table as a feature source. No data is copied by this resource.
resource "google_vertex_ai_feature_group" "this" {
  provider = google-beta

  project = var.project_id
  region  = var.region
  name    = var.feature_group_name

  big_query {
    big_query_source {
      input_uri = "bq://${var.source_table}"
    }
    entity_id_columns = [var.entity_id_column]
  }
}

resource "google_vertex_ai_feature_group_feature" "this" {
  provider = google-beta
  for_each = toset(var.features)

  project       = var.project_id
  region        = var.region
  feature_group = google_vertex_ai_feature_group.this.name
  name          = each.value
}

# The sync. Materialises the feature group into the online store on a schedule.
resource "google_vertex_ai_feature_online_store_featureview" "this" {
  provider = google-beta

  project              = var.project_id
  region               = var.region
  name                 = var.feature_group_name
  feature_online_store = google_vertex_ai_feature_online_store.this.name

  sync_config {
    cron = var.sync_cron
  }

  feature_registry_source {
    feature_groups {
      feature_group_id = google_vertex_ai_feature_group.this.name
      feature_ids      = [for f in google_vertex_ai_feature_group_feature.this : f.name]
    }
  }

  depends_on = [google_vertex_ai_feature_group_feature.this]
}

output "online_store_name" { value = google_vertex_ai_feature_online_store.this.name }
output "feature_view_name" { value = google_vertex_ai_feature_online_store_featureview.this.name }
output "feature_group_name" { value = google_vertex_ai_feature_group.this.name }
output "registered_features" { value = [for f in google_vertex_ai_feature_group_feature.this : f.name] }
