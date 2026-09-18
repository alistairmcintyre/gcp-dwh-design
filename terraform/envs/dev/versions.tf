terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
    # Only for google_project_service_identity, which has no GA equivalent.
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 6.0"
    }
    # For the deliberate wait on service-agent IAM propagation. See main.tf.
    time = {
      source  = "hashicorp/time"
      version = "~> 0.12"
    }
  }

  # Local state is fine for a single-operator demo environment. Every real environment uses a GCS
  # backend, one prefix PER COMPONENT (governance / bigquery / composer / dataproc), never a single
  # monolithic state: smaller blast radius, plans that finish, and concurrent applies across
  # components that cannot clobber each other. Object versioning on the bucket is the rollback path.
  #
  # backend "gcs" {
  #   bucket = "dwh-tfstate-dev"
  #   prefix = "governance"
  # }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}
