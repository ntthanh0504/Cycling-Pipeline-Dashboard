variable "credentials" {
  description = "Path to the Google Cloud credentials file."
  default     = "/root/.google/credentials/cycling-pipeline-dashboard-33b9b532c7a9.json"
}

variable "project" {
  description = "The Google Cloud project ID where resources will be provisioned."
  default     = "cycling-pipeline-dashboard"
}

variable "region" {
  description = "The Google Cloud region for resource deployment."
  default     = "us-central1"
}

variable "location" {
  description = "Geographic location for the project resources."
  default     = "US"
}

variable "bq_dataset_name" {
  description = "Name for the BigQuery dataset."
  default     = "cycling_dataset"
}

variable "gcs_bucket_name" {
  description = "Unique name for the Google Cloud Storage bucket."
  default     = "cycling_pipeline_bucket"
}

variable "gcs_storage_class" {
  description = "Storage class for the Google Cloud Storage bucket."
  default     = "STANDARD"
}