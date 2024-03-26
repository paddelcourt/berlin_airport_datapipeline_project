terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.14.0"
    }
  }
}

provider "google" {
  # Configuration options
  credentials = var.credentials
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "ber-data-project-zoomcamp-bucket" {
  #creating a bucket for the berlin airport data zoomcamp project
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "berlin_airport_dataset" {
  dataset_id                 = var.bq_dataset_name
  description                = "This is a the berlin airport data set project"
  location                   = var.location
  delete_contents_on_destroy = true

}