output "project_id" {
  value = var.project_id
}

output "region" {
  value = var.region
}

output "dataset_ids" {
  description = "Medallion dataset ids by layer key."
  value       = module.bigquery.dataset_ids
}

output "policy_tag_ids" {
  description = <<-EOT
    pii_class -> policy tag resource name. This is the handoff from Terraform to dbt: the models
    declare `meta.pii_class`, this map says what that class *is* in this project, and
    scripts/governance_vars.py joins the two into dbt's `policy_tag_ids` var. No environment-specific
    id ever appears in a model file.
  EOT
  value       = module.governance.policy_tag_ids
}

output "data_policy_ids" {
  value = module.governance.data_policy_ids
}

output "data_masking_enabled" {
  description = <<-EOT
    Whether dynamic data masking is active. Read by scripts/validate_governance.py to decide what
    the correct behaviour IS for an ungranted principal: masked values when true, a rejected query
    when false. Without this the validation silently asserts the wrong thing.
  EOT
  value       = module.governance.data_masking_enabled
}

output "persona_emails" {
  description = "Persona key -> service account email, for impersonation during validation."
  value       = module.personas.emails
}

output "persona_members" {
  description = "Persona key -> IAM member string."
  value       = module.personas.members
}

output "row_access_policies" {
  description = <<-EOT
    Row-level security, expressed here but APPLIED BY DBT (see dbt/macros/row_access_policies.sql).

    Terraform is the wrong owner for these: `create or replace table` drops every row access policy
    on a table, so dbt full-refreshing a model would leave it unprotected until the next apply, and
    Terraform would report drift on every build. Whoever recreates the table must reapply the policy,
    so the definition lives here (reviewable alongside the IAM it complements) and is handed to dbt.

    Note the operator's own all-regions grant. BigQuery has no owner bypass for row-level security:
    the moment one policy exists on a table, an identity named in no policy sees zero rows.
  EOT
  value = {
    dim_client = [
      {
        name     = "rls_uk_desk"
        grantees = [module.personas.members["uk_desk"]]
        filter   = "trading_region = 'UK'"
      },
      {
        name = "rls_all_regions"
        grantees = [
          module.personas.members["marketing"],
          module.personas.members["compliance"],
          # Quants needs rows in order for the COLUMN behaviour to be observable at all -- a
          # principal filtered to zero rows tells you nothing about column-level security.
          module.personas.members["quants"],
          var.operator_member,
        ]
        filter = "TRUE"
      },
    ]
  }
}

output "data_quality_scan_ids" {
  description = "Dataplex data quality scan ids, read by scripts/dataplex_report.py."
  value       = module.data_quality.scan_ids
}
