variable "project_id" {
  description = "Target GCP project id."
  type        = string
}

variable "region" {
  description = <<-EOT
    Single region used for the BigQuery datasets, the policy-tag taxonomy, the data policies and the
    Dataplex scans. These four MUST agree: a policy tag cannot be applied to a column in a table in a
    different location, and a Dataplex data scan cannot read a table outside its own region. Using
    one region rather than the `EU` multi-region keeps all four aligned and keeps data resident in
    the UK, which is the constraint that usually drives the choice in the first place.
  EOT
  type        = string
  default     = "europe-west2"
}

variable "operator_member" {
  description = <<-EOT
    The human running this stack, as an IAM member string (e.g. "user:alice@example.com").
    Needed for two reasons that are easy to miss:
      1. Once ANY row access policy exists on a table, principals not named in one see ZERO rows --
         there is no owner bypass. Without an explicit all-regions grant, the platform owner's own
         queries silently return nothing.
      2. Impersonating the persona service accounts (to test the controls) requires Token Creator.
  EOT
  type        = string
}

variable "labels" {
  type = map(string)
  default = {
    managed_by = "terraform"
    component  = "data-platform"
  }
}

variable "build_identity_member" {
  description = <<-EOT
    The principal that runs dbt, as an IAM member string. It needs Fine-Grained Reader on every PII
    class, because building the masked derivative table requires reading the raw columns to write
    masked ones -- and BigQuery gives project owners no exemption from column-level security.

    In production this is a dedicated service account (Composer's, or the Cloud Run job's), never a
    person: it is the one identity in the project that can read all PII, so its use should be
    attributable to a pipeline run rather than to someone's console session. Defaults to the
    operator here only because a single-operator demo has nowhere else to put it.
  EOT
  type        = string
}
