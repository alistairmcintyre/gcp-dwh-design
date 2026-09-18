# ------------------------------------------------------------------------------------------------
# Column-level security for BigQuery: a Dataplex/Data Catalog taxonomy of policy tags, plus one
# dynamic-data-masking data policy per tag.
#
# How the pieces fit together (this trips people up, so it is worth stating plainly):
#
#   taxonomy ──contains──▶ policy tag ──attached to──▶ a BigQuery column   (dbt does this attachment)
#                              │
#                              └──has──▶ data policy ──defines──▶ the masking rule
#
#   IAM on the POLICY TAG   (roles/datacatalog.categoryFineGrainedReader) ▶ principal sees RAW
#   IAM on the DATA POLICY  (roles/bigquerydatapolicy.maskedReader)       ▶ principal sees MASKED
#   Neither                                                              ▶ the query is REJECTED
#
# The last line is the important one: BigQuery fails the query with "Access Denied on column" rather
# than quietly returning the row without that column. An analyst therefore cannot mistake a masked
# or missing value for real data, and an unauthorised export cannot happen silently.
#
# Everything here is slow-moving, org-wide and security-reviewed, so it belongs in Terraform. Which
# *columns* carry which tag moves at the speed of the data model, so that lives in dbt. See
# docs/governance.md.
# ------------------------------------------------------------------------------------------------

resource "google_data_catalog_taxonomy" "pii" {
  project      = var.project_id
  region       = var.region
  display_name = var.taxonomy_name
  description  = "Data classification for ${var.project_id}. Tags are attached to columns by dbt; masking rules are defined here."

  # Without this the taxonomy is a catalog-only label with no enforcement behind it.
  activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]
}

resource "google_data_catalog_policy_tag" "class" {
  for_each = var.pii_classes

  taxonomy     = google_data_catalog_taxonomy.pii.id
  display_name = each.value.display_name
  description  = each.value.description
}

# One masking policy per class. A policy tag can carry several data policies with different
# principals on each -- e.g. Marketing gets SHA256 email (still joinable to the CDP) while Support
# gets EMAIL_MASK (can see the domain for troubleshooting). Kept to one per class here so the
# demonstration has one unambiguous answer per identity; docs/governance.md covers the fan-out.
resource "google_bigquery_datapolicy_data_policy" "mask" {
  for_each = var.enable_data_masking ? var.pii_classes : {}

  project          = var.project_id
  location         = var.region
  data_policy_id   = replace("mask_${each.key}", "-", "_")
  policy_tag       = google_data_catalog_policy_tag.class[each.key].name
  data_policy_type = "DATA_MASKING_POLICY"

  data_masking_policy {
    predefined_expression = each.value.masking_rule
  }
}

# ---- Access to RAW values -----------------------------------------------------------------------
locals {
  # Flatten map(class -> [members]) into one binding per (class, member) pair so a member can be
  # added or removed without churning the others.
  fine_grained_bindings = merge([
    for class_key, members in var.fine_grained_readers : {
      for member in members : "${class_key}|${member}" => {
        class_key = class_key
        member    = member
      }
    }
  ]...)

  masked_bindings = merge([
    for class_key, members in var.masked_readers : {
      for member in members : "${class_key}|${member}" => {
        class_key = class_key
        member    = member
      }
    }
  ]...)
}

resource "google_data_catalog_policy_tag_iam_member" "fine_grained_reader" {
  for_each = local.fine_grained_bindings

  policy_tag = google_data_catalog_policy_tag.class[each.value.class_key].name
  role       = "roles/datacatalog.categoryFineGrainedReader"
  member     = each.value.member
}

# ---- Access to MASKED values --------------------------------------------------------------------
resource "google_bigquery_datapolicy_data_policy_iam_member" "masked_reader" {
  for_each = var.enable_data_masking ? local.masked_bindings : {}

  project        = var.project_id
  location       = var.region
  data_policy_id = google_bigquery_datapolicy_data_policy.mask[each.value.class_key].data_policy_id
  role           = "roles/bigquerydatapolicy.maskedReader"
  member         = each.value.member
}
