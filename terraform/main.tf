terraform {
  required_version = ">= 1.0.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.34.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# Google Cloud Storage Bucket for storing cycling data
resource "google_storage_bucket" "cycling_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  storage_class = var.gcs_storage_class
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }
}

# BigQuery Dataset for cycling analytics
resource "google_bigquery_dataset" "cycling_dataset" {
  dataset_id = var.bq_dataset_name
  project    = var.project
  location   = var.region
}
