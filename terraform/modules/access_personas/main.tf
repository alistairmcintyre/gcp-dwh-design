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
# Test principals, one per access persona.
#
# In production these are Google Groups -- `group:uk-desk-analysts@example.com` -- never individual users
# and never service accounts. Bindings go to groups so that joiners/leavers are an identity-team
# operation rather than a Terraform pull request, and so an access review has one object to read per
# persona instead of a list of people.
#
# Service accounts are used here for one reason: they can be *impersonated*, which makes the access
# model testable. `scripts/validate_governance.py` runs the same query as each persona and asserts
# that masking and row filtering actually applied. An access control you have never executed as the
# restricted principal is an access control you are guessing about -- and the cost of guessing wrong
# is a regulatory incident, not a bug.
# ------------------------------------------------------------------------------------------------

variable "project_id" { type = string }

variable "personas" {
  description = <<-EOT
    Map of persona key -> definition.
      account_id    service account id (also the persona's identity in every IAM binding)
      display_name  human-readable name
      description   what this persona may see, and why
      project_roles project-level roles (keep minimal: usually just jobUser)
  EOT
  type = map(object({
    account_id    = string
    display_name  = string
    description   = string
    project_roles = list(string)
  }))
}

variable "dataset_readers" {
  description = "Map of persona key -> list of dataset ids the persona may read (BigQuery Data Viewer)."
  type        = map(list(string))
  default     = {}
}

variable "impersonators" {
  description = <<-EOT
    Principals allowed to mint tokens for these service accounts, i.e. to run the validation as each
    persona. In production this is a break-glass group with logged, time-bound access -- not a
    standing grant to an engineer.
  EOT
  type        = list(string)
  default     = []
}

resource "google_service_account" "persona" {
  for_each = var.personas

  project      = var.project_id
  account_id   = each.value.account_id
  display_name = each.value.display_name
  description  = each.value.description
}

locals {
  project_role_bindings = merge([
    for persona_key, persona in var.personas : {
      for role in persona.project_roles : "${persona_key}|${role}" => {
        persona_key = persona_key
        role        = role
      }
    }
  ]...)

  dataset_bindings = merge([
    for persona_key, datasets in var.dataset_readers : {
      for dataset_id in datasets : "${persona_key}|${dataset_id}" => {
        persona_key = persona_key
        dataset_id  = dataset_id
      }
    }
  ]...)

  impersonation_bindings = merge([
    for persona_key, persona in var.personas : {
      for principal in var.impersonators : "${persona_key}|${principal}" => {
        persona_key = persona_key
        principal   = principal
      }
    }
  ]...)
}

resource "google_project_iam_member" "persona" {
  for_each = local.project_role_bindings

  project = var.project_id
  role    = each.value.role
  member  = "serviceAccount:${google_service_account.persona[each.value.persona_key].email}"
}

# Dataset-level, not project-level: a persona that may read Gold must not thereby read Bronze.
resource "google_bigquery_dataset_iam_member" "persona" {
  for_each = local.dataset_bindings

  project    = var.project_id
  dataset_id = each.value.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.persona[each.value.persona_key].email}"
}

resource "google_service_account_iam_member" "impersonation" {
  for_each = local.impersonation_bindings

  service_account_id = google_service_account.persona[each.value.persona_key].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value.principal
}

output "emails" {
  description = "Map of persona key -> service account email."
  value       = { for k, v in google_service_account.persona : k => v.email }
}

output "members" {
  description = "Map of persona key -> IAM member string, ready to paste into a policy binding."
  value       = { for k, v in google_service_account.persona : k => "serviceAccount:${v.email}" }
}
