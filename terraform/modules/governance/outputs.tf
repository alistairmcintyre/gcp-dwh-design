output "taxonomy_id" {
  description = "Full resource id of the taxonomy."
  value       = google_data_catalog_taxonomy.pii.id
}

output "policy_tag_ids" {
  description = <<-EOT
    Map of pii_class -> policy tag resource name. Fed straight into dbt's `policy_tag_ids` var by
    scripts/governance_vars.py, which is what keeps the dbt models free of environment-specific ids.
  EOT
  value       = { for k, v in google_data_catalog_policy_tag.class : k => v.name }
}

output "data_policy_ids" {
  description = "Map of pii_class -> BigQuery data policy id (the masking rule holder). Empty when masking is disabled."
  value       = { for k, v in google_bigquery_datapolicy_data_policy.mask : k => v.data_policy_id }
}

output "data_masking_enabled" {
  value = var.enable_data_masking
}
