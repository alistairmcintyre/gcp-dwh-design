terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0"
    }
  }
}

variable "project_id" { type = string }

variable "services" {
  description = "APIs to enable on the project."
  type        = list(string)
}

variable "disable_on_destroy" {
  description = <<-EOT
    Leave false. Disabling an API on `terraform destroy` is almost always wrong in a shared project:
    it takes down anything else in the project that depends on that API, and the blast radius is
    invisible from this state file.
  EOT
  type        = bool
  default     = false
}

resource "google_project_service" "this" {
  for_each = toset(var.services)

  project                    = var.project_id
  service                    = each.value
  disable_on_destroy         = var.disable_on_destroy
  disable_dependent_services = false
}

output "enabled" {
  value = [for s in google_project_service.this : s.service]
}
