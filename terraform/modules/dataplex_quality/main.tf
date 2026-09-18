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
# Dataplex auto data quality scans.
#
# WHY THIS EXISTS ALONGSIDE DBT TESTS -- the two are not redundant, and the difference is worth
# being able to state crisply, because it is the obvious interview question:
#
#   dbt tests                             Dataplex data quality scans
#   -------------------------------       --------------------------------------------------
#   run INSIDE the pipeline               run OUT OF BAND, on a schedule
#   BLOCK the build (dbt build stops      OBSERVE: they score and alert, they do not stop a
#     downstream models on failure)         pipeline that already ran
#   cover dbt-built models only           cover ANY BigQuery table -- including the Bronze
#                                           landing tables no dbt model has touched yet, and
#                                           tables owned by teams that do not use dbt
#   results live in run_results.json      results land in the catalog, attached to the asset,
#     and the orchestrator's logs           with history and a per-dimension score
#   author: the analytics engineer        author: the data owner / steward, often not an engineer
#
# The practical split used here: dbt tests are the CONTRACT (must pass or nothing downstream
# builds); Dataplex scans are the MONITOR (trend quality over time, catch drift in sources nobody
# models, and give non-engineers a place to see quality without reading CI logs). A rule that must
# never ship bad data belongs in dbt. A rule that describes the health of an asset belongs here.
#
# Scans are also where you attach quality to the SLA/SLO conversation: each rule carries a
# dimension (COMPLETENESS / VALIDITY / UNIQUENESS / FRESHNESS), and the per-dimension score over
# time is what you actually report to a data owner.
# ------------------------------------------------------------------------------------------------

variable "project_id" { type = string }

variable "region" {
  description = "Must match the BigQuery location of the scanned tables."
  type        = string
}

variable "scans" {
  description = <<-EOT
    Data quality scans, keyed by scan id.

      dataset / table    the BigQuery table to scan
      cron               schedule; null runs the scan on demand only
      sampling_percent   100 for correctness-critical tables; lower it only for cost on huge tables
      row_filter         restrict the scan (e.g. to recent partitions) -- a full scan of a large
                         partitioned table every hour is an expensive way to learn nothing new
      non_null_columns   COMPLETENESS: the column must never be null
      unique_columns     UNIQUENESS: values must be distinct
      set_rules          VALIDITY: the column's values must come from a fixed set
      range_rules        VALIDITY: numeric bounds
      row_conditions     VALIDITY: an arbitrary per-row boolean SQL expression
      freshness          FRESHNESS: max age of the newest value in a date/timestamp column
  EOT
  type = map(object({
    dataset          = string
    table            = string
    cron             = optional(string)
    description      = optional(string, "")
    sampling_percent = optional(number, 100)
    row_filter       = optional(string, "")
    non_null_columns = optional(list(string), [])
    unique_columns   = optional(list(string), [])
    set_rules = optional(list(object({
      column = string
      values = list(string)
    })), [])
    range_rules = optional(list(object({
      column     = string
      min_value  = optional(string)
      max_value  = optional(string)
      strict_min = optional(bool, false)
      strict_max = optional(bool, false)
    })), [])
    row_conditions = optional(list(object({
      column      = optional(string)
      expression  = string
      threshold   = optional(number, 1.0)
      description = optional(string, "")
    })), [])
    freshness = optional(object({
      column        = string
      max_age_hours = number
    }))
  }))
}

resource "google_dataplex_datascan" "quality" {
  for_each = var.scans

  project      = var.project_id
  location     = var.region
  data_scan_id = each.key
  description  = each.value.description

  data {
    resource = "//bigquery.googleapis.com/projects/${var.project_id}/datasets/${each.value.dataset}/tables/${each.value.table}"
  }

  execution_spec {
    dynamic "trigger" {
      for_each = [1]
      content {
        dynamic "schedule" {
          for_each = each.value.cron == null ? [] : [each.value.cron]
          content {
            cron = schedule.value
          }
        }
        dynamic "on_demand" {
          for_each = each.value.cron == null ? [1] : []
          content {}
        }
      }
    }
  }

  data_quality_spec {
    sampling_percent = each.value.sampling_percent
    row_filter       = each.value.row_filter

    # COMPLETENESS -- the column is populated.
    dynamic "rules" {
      for_each = each.value.non_null_columns
      content {
        column      = rules.value
        dimension   = "COMPLETENESS"
        threshold   = 1.0
        description = "${rules.value} is always populated"
        non_null_expectation {}
      }
    }

    # UNIQUENESS -- no duplicate keys. Catches a broken merge or a replayed source partition.
    dynamic "rules" {
      for_each = each.value.unique_columns
      content {
        column      = rules.value
        dimension   = "UNIQUENESS"
        description = "${rules.value} is unique"
        uniqueness_expectation {}
      }
    }

    # VALIDITY -- values come from the agreed set. Catches a new enum value appearing upstream
    # without anyone telling the warehouse, which is the single most common silent breakage.
    dynamic "rules" {
      for_each = each.value.set_rules
      content {
        column      = rules.value.column
        dimension   = "VALIDITY"
        threshold   = 1.0
        description = "${rules.value.column} is one of the accepted values"
        set_expectation {
          values = rules.value.values
        }
      }
    }

    # VALIDITY -- numeric bounds.
    dynamic "rules" {
      for_each = each.value.range_rules
      content {
        column      = rules.value.column
        dimension   = "VALIDITY"
        threshold   = 1.0
        description = "${rules.value.column} is within its expected range"
        range_expectation {
          min_value          = rules.value.min_value
          max_value          = rules.value.max_value
          strict_min_enabled = rules.value.strict_min
          strict_max_enabled = rules.value.strict_max
        }
      }
    }

    # VALIDITY -- arbitrary business rules, expressed per row.
    dynamic "rules" {
      for_each = each.value.row_conditions
      content {
        column      = rules.value.column
        dimension   = "VALIDITY"
        threshold   = rules.value.threshold
        description = rules.value.description
        row_condition_expectation {
          sql_expression = rules.value.expression
        }
      }
    }

    # FRESHNESS -- is the data recent enough to be worth reading? Expressed as a row condition
    # rather than a separate rule type so it participates in the same per-dimension score.
    dynamic "rules" {
      for_each = each.value.freshness == null ? [] : [each.value.freshness]
      content {
        column      = rules.value.column
        dimension   = "FRESHNESS"
        threshold   = 1.0
        description = "data is no more than ${rules.value.max_age_hours}h old"
        row_condition_expectation {
          sql_expression = "${rules.value.column} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL ${rules.value.max_age_hours} HOUR)"
        }
      }
    }
  }
}

output "scan_names" {
  value = { for k, v in google_dataplex_datascan.quality : k => v.name }
}

output "scan_ids" {
  value = { for k, v in google_dataplex_datascan.quality : k => v.data_scan_id }
}
