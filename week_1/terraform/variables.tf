variable "credentials" {
  description = "My Credentials"
  default     = "C:/Users/X270/Downloads/dtc-de-410504-26e1c562211f.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "dtc-de-410504"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "australia-southeast2"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "australia-southeast2"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "terraform-dtc-de-410504-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
