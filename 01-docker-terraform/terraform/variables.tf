# Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
// variable "credentials" { 
//   description = "My Credentials"
//   default     = "<Path to your Service Account json file>"
//   #ex: if you have a directory where this file is called keys with your service account json file
//   #saved there as my-creds.json you could use default = "./keys/my-creds.json"
// }

locals {
  data_lake_bucket = "data_lake_bucket"
}

variable "project" {
  description = "Project"
  default     = "zeta-serenity-412422" # "<Your GCP Project ID>"" If this has not default value, when running any terraform command it will ask you (recommended in some cases)
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  #Update the below to your desired region
  default     = "southamerica-west1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "southamerica-west1"
}

variable "gcs_bucket_name" {
  description = "The name of the Google Cloud Storage bucket. Must be globally unique." # See that in main use a function to assure this
  #Update the below to a unique bucket name
  default     = "<bucket_name>"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to."
  #Update the below to what you want your dataset to be called
  default     = "ny_trips_data"
}

variable "gcs_storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default     = "STANDARD"
}