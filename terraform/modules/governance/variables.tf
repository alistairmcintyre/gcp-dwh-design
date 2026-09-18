variable "project_id" {
  description = "GCP project that owns the taxonomy, data policies and test principals."
  type        = string
}

variable "region" {
  description = <<-EOT
    Region for the taxonomy and the data policies. This MUST match the BigQuery dataset location:
    a policy tag can only be applied to a column in a table in the same region as its taxonomy.
    Multi-region datasets ("EU", "US") take the lowercase multi-region name here ("eu", "us").
  EOT
  type        = string
}

variable "taxonomy_name" {
  description = "Display name of the Dataplex/Data Catalog taxonomy."
  type        = string
  default     = "pii-classification"
}

variable "pii_classes" {
  description = <<-EOT
    The data classification taxonomy. One entry per class of sensitive data, keyed by the same
    string models use in `meta.pii_class`, so a column's declaration in dbt and its masking rule
    here are joined by a value a human can read.

    `masking_rule` is a BigQuery predefined masking expression:
      SHA256                  deterministic hash -- preserves joins and distinct counts
      ALWAYS_NULL             returns NULL
      DEFAULT_MASKING_VALUE   type-appropriate default ("" for STRING, 0 for numerics, 1970-01-01 for DATE)
      LAST_FOUR_CHARACTERS    keeps a suffix, e.g. for account references
      FIRST_FOUR_CHARACTERS   keeps a prefix
      EMAIL_MASK              keeps the domain, masks the local part (xxxxx@example.com)
      DATE_YEAR_MASK          truncates a DATE/DATETIME to its year
  EOT
  type = map(object({
    display_name = string
    description  = string
    masking_rule = string
  }))
}

variable "fine_grained_readers" {
  description = <<-EOT
    Principals granted `roles/datacatalog.categoryFineGrainedReader` per pii_class -- they read the
    RAW value. Keyed by pii_class; the value is a list of IAM member strings. Deliberately per-class
    rather than blanket, so "Compliance can see names and DOB" and "Marketing can see email" are
    separable grants rather than one all-or-nothing PII role.
  EOT
  type        = map(list(string))
  default     = {}
}

variable "masked_readers" {
  description = <<-EOT
    Principals granted `roles/bigquerydatapolicy.maskedReader` per pii_class -- they read the MASKED
    value. A principal with neither this nor Fine-Grained Reader cannot query the column at all: the
    query fails rather than silently dropping the column, which is the behaviour you want.
  EOT
  type        = map(list(string))
  default     = {}
}

variable "enable_data_masking" {
  description = <<-EOT
    Create BigQuery dynamic-data-masking data policies and grant Masked Reader.

    **This requires the project to belong to a Google Cloud organization.** In a standalone project
    the API refuses with:

        Error 400: The project <n> needs to belong to an organization to manage DataPolicies.

    The taxonomy and the policy tags themselves work fine without an organization, so column-level
    security still enforces -- a principal without `categoryFineGrainedReader` on a tag simply has
    the query REJECTED on that column instead of receiving a masked value. Deny is the stricter
    behaviour; masking is what you add on top so that analysts who only need aggregates are not
    blocked outright.

    Set true in any org-owned environment (i.e. every real one).
  EOT
  type        = bool
  default     = true
}
