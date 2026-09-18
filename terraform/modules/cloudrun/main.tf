terraform {
  required_version = ">= 1.5"
  required_providers {
    google = { source = "hashicorp/google", version = ">= 6.0" }
    # API Gateway resources are beta-only. Declared explicitly rather than relying on implicit
    # provider inheritance, so the module is usable standalone.
    google-beta = { source = "hashicorp/google-beta", version = ">= 6.0" }
  }
}

# ------------------------------------------------------------------------------------------------
# Cloud Run service + API Gateway front door for the data contract registry (../../services).
#
# The shape here is the one worth defending: Cloud Run is deployed with NO public invoker, and every
# external caller arrives through API Gateway. That separation is what lets the platform expose an
# API to other squads without each squad's client having to hold a Google identity -- the gateway
# handles the auth scheme the consumer can actually use (API key, JWT), then mints the service
# identity Cloud Run requires. Making the service itself public would mean either no auth at all or
# every consumer needing GCP credentials.
# ------------------------------------------------------------------------------------------------

variable "project_id" { type = string }
variable "region" { type = string }
variable "service_name" {
  type    = string
  default = "contract-api"
}
variable "image" {
  description = "Fully-qualified Artifact Registry image, pinned by DIGEST not tag. A tag is mutable: 'latest' means a redeploy can silently ship different code than the one that was reviewed."
  type        = string
}
variable "enable_api_gateway" {
  description = "Front the service with API Gateway. Off for internal-only deployments where callers already hold Google identities."
  type        = bool
  default     = true
}
variable "invokers" {
  description = "Principals allowed to invoke the service directly (in addition to the gateway)."
  type        = list(string)
  default     = []
}
variable "min_instances" {
  description = "Keep-warm instances. 0 is right for an internal API called by CI -- cold starts of a second or two are irrelevant there, and scale-to-zero is most of the cost saving."
  type        = number
  default     = 0
}

resource "google_service_account" "runtime" {
  project      = var.project_id
  account_id   = "${var.service_name}-run"
  display_name = "Runtime identity for ${var.service_name}"
  description  = "Cloud Run service identity. Holds no data-plane roles: the registry serves files baked into the image and needs nothing from BigQuery."
}

resource "google_cloud_run_v2_service" "this" {
  project  = var.project_id
  name     = var.service_name
  location = var.region

  # Internal + load balancer only: the service is not reachable from the internet except through
  # the gateway. The single most valuable line in this file.
  ingress = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"

  template {
    service_account = google_service_account.runtime.email

    scaling {
      min_instance_count = var.min_instances
      max_instance_count = 10
    }

    containers {
      image = var.image

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
        # Only bill for CPU while a request is in flight. Correct for a request/response API;
        # wrong if the service did background work between requests.
        cpu_idle = true
      }

      # Cloud Run injects PORT; the container honours it rather than hard-coding 8080.
      startup_probe {
        http_get { path = "/health" }
        initial_delay_seconds = 2
        period_seconds        = 3
        failure_threshold     = 10
      }

      liveness_probe {
        http_get { path = "/health" }
        period_seconds = 30
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }
}

# No allUsers binding anywhere in this file, deliberately.
resource "google_cloud_run_v2_service_iam_member" "invokers" {
  for_each = toset(concat(var.invokers, var.enable_api_gateway ? ["serviceAccount:${google_service_account.gateway[0].email}"] : []))

  project  = var.project_id
  location = google_cloud_run_v2_service.this.location
  name     = google_cloud_run_v2_service.this.name
  role     = "roles/run.invoker"
  member   = each.value
}

# ---- API Gateway ---------------------------------------------------------------------------------
resource "google_service_account" "gateway" {
  count = var.enable_api_gateway ? 1 : 0

  project      = var.project_id
  account_id   = "${var.service_name}-gw"
  display_name = "API Gateway identity for ${var.service_name}"
  description  = "The gateway invokes Cloud Run as this identity, so the service can stay non-public."
}

resource "google_api_gateway_api" "this" {
  count = var.enable_api_gateway ? 1 : 0

  provider = google-beta
  project  = var.project_id
  api_id   = var.service_name
}

resource "google_api_gateway_api_config" "this" {
  count = var.enable_api_gateway ? 1 : 0

  provider      = google-beta
  project       = var.project_id
  api           = google_api_gateway_api.this[0].api_id
  api_config_id = "v1"

  openapi_documents {
    document {
      path = "openapi.yaml"
      contents = base64encode(templatefile("${path.module}/openapi.yaml.tftpl", {
        service_name = var.service_name
        backend_url  = google_cloud_run_v2_service.this.uri
      }))
    }
  }

  gateway_config {
    backend_config {
      google_service_account = google_service_account.gateway[0].email
    }
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "google_api_gateway_gateway" "this" {
  count = var.enable_api_gateway ? 1 : 0

  provider   = google-beta
  project    = var.project_id
  region     = var.region
  api_config = google_api_gateway_api_config.this[0].id
  gateway_id = var.service_name
}

output "service_url" { value = google_cloud_run_v2_service.this.uri }
output "gateway_url" { value = try("https://${google_api_gateway_gateway.this[0].default_hostname}", null) }
output "runtime_service_account" { value = google_service_account.runtime.email }
