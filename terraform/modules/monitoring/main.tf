terraform {
  required_version = ">= 1.5"
  required_providers {
    google = { source = "hashicorp/google", version = ">= 6.0" }
  }
}

# ------------------------------------------------------------------------------------------------
# Pipeline observability: log-based metrics, alert policies, and freshness SLO monitoring.
#
# The principle behind every choice here: ALERT ON SYMPTOMS THE CONSUMER FEELS, not on causes.
# "The 03:00 dbt run failed" is a cause -- it may be irrelevant if the retry succeeded at 03:15.
# "The Gold table has no data for yesterday at 07:00" is a symptom, and it is what a consumer will
# notice at 09:00 whether or not anyone was paged. Cause-based alerting is how teams end up with
# fifty noisy alerts and still miss the outage.
#
# Everything is Terraform because an alert someone created by hand in the console is an alert nobody
# can review, reproduce in another environment, or explain the history of.
# ------------------------------------------------------------------------------------------------

variable "project_id" { type = string }

variable "notification_channels" {
  description = "Existing Cloud Monitoring notification channel ids to alert. Empty means policies are created but notify nothing -- useful for a demo, useless in production."
  type        = list(string)
  default     = []
}

variable "freshness_slo_hours" {
  description = "Alert when the Gold marts have had no successful build for this long. Derived from the data contracts' freshness SLO, not picked arbitrarily: contracts promise data for D by 07:00 on D+1, so 31 hours is the promise plus a small grace."
  type        = number
  default     = 31
}

variable "dbt_failure_threshold" {
  description = "Number of dbt errors in the alignment period before alerting. 1 is right for a nightly build -- there is no such thing as an acceptable rate of silent failure on a daily job."
  type        = number
  default     = 1
}

# ---- Log-based metrics --------------------------------------------------------------------------
# dbt runs in a container and logs JSON to stdout, which Cloud Logging parses into structured
# fields. That is the whole reason these metrics are possible without shipping metrics separately.

resource "google_logging_metric" "dbt_errors" {
  project     = var.project_id
  name        = "dbt/model_errors"
  description = "Count of dbt model or test failures, extracted from the run's structured logs."

  filter = <<-EOT
    resource.type="k8s_container"
    jsonPayload.info.level="error"
    jsonPayload.info.name=~"NodeFinished|MainEncounteredError"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
    labels {
      key         = "model"
      value_type  = "STRING"
      description = "The dbt node that failed"
    }
  }

  # Labelled by model so the alert says WHICH model broke. An alert that only says "dbt failed"
  # costs the responder the first ten minutes of every incident.
  label_extractors = {
    "model" = "EXTRACT(jsonPayload.data.node_info.node_name)"
  }
}

resource "google_logging_metric" "spark_job_failures" {
  project     = var.project_id
  name        = "spark/job_failures"
  description = "Dataproc Serverless batches that failed, from the framework's structured job_failed event."

  filter = <<-EOT
    resource.type="cloud_dataproc_batch"
    jsonPayload.event="job_failed"
  EOT

  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "INT64"
    unit        = "1"
    labels {
      key        = "job"
      value_type = "STRING"
    }
  }

  label_extractors = {
    "job" = "EXTRACT(jsonPayload.job)"
  }
}

resource "google_logging_metric" "spark_quality_violations" {
  project     = var.project_id
  name        = "spark/quality_violations"
  description = "Rows failing an in-pipeline quality check, including warn-level ones."

  filter = <<-EOT
    resource.type="cloud_dataproc_batch"
    jsonPayload.event="quality_check"
    jsonPayload.passed="false"
  EOT

  # A DISTRIBUTION, not a counter: the interesting question is not "did a check fail" but "by how
  # much, and is it getting worse". A warn-level check drifting from 3 rows to 3,000 is a real
  # signal that a plain counter would flatten into "it failed again".
  metric_descriptor {
    metric_kind = "DELTA"
    value_type  = "DISTRIBUTION"
    unit        = "1"
    labels {
      key        = "check"
      value_type = "STRING"
    }
  }

  value_extractor = "EXTRACT(jsonPayload.violations)"
  label_extractors = {
    "check" = "EXTRACT(jsonPayload.check)"
  }

  bucket_options {
    exponential_buckets {
      num_finite_buckets = 16
      growth_factor      = 4
      scale              = 1
    }
  }
}

# ---- Alert policies -----------------------------------------------------------------------------

resource "google_monitoring_alert_policy" "dbt_failures" {
  project      = var.project_id
  display_name = "dbt model or test failure"
  combiner     = "OR"
  severity     = "ERROR"

  documentation {
    content   = <<-EOT
      A dbt model or test failed. Because the project runs `dbt build`, a failed test BLOCKS every
      downstream model -- so this is not "a test is red", it is "the rest of the warehouse did not
      build".

      1. `dbt/target/run_results.json` from the run names the failing node.
      2. If it is a test, the compiled SQL in `target/compiled/` returns the offending rows.
      3. Check whether the SOURCE changed before assuming the model did -- most test failures are
         upstream contract breaches, not modelling bugs.
    EOT
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "dbt errors in the last hour"
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.dbt_errors.name}\" AND resource.type=\"k8s_container\""
      comparison      = "COMPARISON_GT"
      threshold_value = var.dbt_failure_threshold - 1
      duration        = "0s"
      aggregations {
        alignment_period     = "3600s"
        per_series_aligner   = "ALIGN_SUM"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["metric.label.model"]
      }
    }
  }

  notification_channels = var.notification_channels
  alert_strategy {
    auto_close = "86400s"
  }
}

resource "google_monitoring_alert_policy" "gold_staleness" {
  project      = var.project_id
  display_name = "Gold marts are stale (freshness SLO breach)"
  combiner     = "OR"
  severity     = "CRITICAL"

  documentation {
    content   = <<-EOT
      No successful Gold build for ${var.freshness_slo_hours} hours, which breaches the freshness
      SLO published in `contracts/`. Consumers named in those contracts -- BI, Finance, the Firestore
      serving job -- are reading stale data right now.

      This is the SYMPTOM alert. It fires whether the cause was a failed run, a run that never
      started, a stuck scheduler or an upstream source that never arrived, which is precisely why it
      is the one that pages: those causes have separate alerts that may all be silent while the
      consumer is still wrong.
    EOT
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "no successful dbt run recently"
    condition_absent {
      filter   = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.dbt_errors.name}\" AND resource.type=\"k8s_container\""
      duration = "${var.freshness_slo_hours * 3600}s"
      aggregations {
        alignment_period   = "3600s"
        per_series_aligner = "ALIGN_COUNT"
      }
    }
  }

  notification_channels = var.notification_channels
}

resource "google_monitoring_alert_policy" "spark_failures" {
  project      = var.project_id
  display_name = "Dataproc batch failure"
  combiner     = "OR"
  severity     = "ERROR"

  documentation {
    content   = <<-EOT
      A Dataproc Serverless batch failed. The framework's structured logs carry the job name and the
      failing step; a `QualityGateFailed` error means the job stopped BEFORE writing, so the sink is
      untouched and the fix is upstream, not a cleanup.
    EOT
    mime_type = "text/markdown"
  }

  conditions {
    display_name = "spark job failures"
    condition_threshold {
      filter          = "metric.type=\"logging.googleapis.com/user/${google_logging_metric.spark_job_failures.name}\" AND resource.type=\"cloud_dataproc_batch\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"
      aggregations {
        alignment_period     = "3600s"
        per_series_aligner   = "ALIGN_SUM"
        cross_series_reducer = "REDUCE_SUM"
        group_by_fields      = ["metric.label.job"]
      }
    }
  }

  notification_channels = var.notification_channels
  alert_strategy {
    auto_close = "86400s"
  }
}

output "metric_names" {
  value = [
    google_logging_metric.dbt_errors.name,
    google_logging_metric.spark_job_failures.name,
    google_logging_metric.spark_quality_violations.name,
  ]
}
