terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0"
    }
  }
}

# ------------------------------------------------------------------------------------------------
# The Medallion datasets. One dataset per layer rather than one per domain, because the *first*
# access decision at this shape of organisation is "which layer of trust are you allowed near":
#
#   bronze (raw)      landed, unmodelled, as-received  -- Data Engineering only
#   silver (staging)  cleaned, conformed, still fine-grained -- engineers + modellers
#   gold   (marts)    contracted, documented, governed -- the self-serve surface
#
# Dataset-level IAM is the coarse control. Row access policies and column policy tags are only
# reached for inside Gold, where one physical table has to serve several audiences. See
# docs/governance.md for the full ladder.
# ------------------------------------------------------------------------------------------------

variable "project_id" { type = string }

variable "location" {
  description = "BigQuery location. Must match the governance taxonomy region."
  type        = string
}

variable "datasets" {
  description = "Map of dataset id -> {description, layer}."
  type = map(object({
    description = string
    layer       = string
  }))
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "default_table_expiration_ms" {
  description = <<-EOT
    Default table expiration. Leave null in any project with billing enabled -- an expiration on a
    warehouse dataset is a data-loss footgun.

    Set it only for a project in BigQuery **sandbox** mode (no billing account). Sandbox forces a
    60-day default expiration on every dataset and rejects an update that tries to unset it, so
    Terraform must declare the same value or every plan shows permanent drift.
  EOT
  type        = number
  default     = null
}

variable "default_partition_expiration_ms" {
  description = "As above, for partitioned tables. Sandbox forces this too."
  type        = number
  default     = null
}

variable "delete_contents_on_destroy" {
  description = "True only in throwaway/demo projects. Never true for an environment holding real data."
  type        = bool
  default     = false
}

resource "google_bigquery_dataset" "this" {
  for_each = var.datasets

  project                         = var.project_id
  dataset_id                      = each.key
  location                        = var.location
  description                     = each.value.description
  delete_contents_on_destroy      = var.delete_contents_on_destroy
  default_table_expiration_ms     = var.default_table_expiration_ms
  default_partition_expiration_ms = var.default_partition_expiration_ms

  labels = merge(var.labels, {
    layer = each.value.layer
  })
}

output "dataset_ids" {
  value = { for k, v in google_bigquery_dataset.this : k => v.dataset_id }
}

output "dataset_self_links" {
  value = { for k, v in google_bigquery_dataset.this : k => v.id }
}
