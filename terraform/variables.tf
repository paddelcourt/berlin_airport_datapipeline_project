

variable "credentials"{
    description = "GCS Credentials"
    default = ""
}

variable "project"{
    description = "Project"
    default = ""
}

variable "location"{
    description = "Project GCS Location"
    default = "EU"

}

variable "region"{
    description = "Project GCS Location"
    default = "europe-west1"

}



variable "bq_dataset_name"{
    description = "Berlin Airport Data Project Big Query Dataset"
    default = "berlin_airport_dataset"

}

variable "gcs_bucket_name"{
    description = "My storage bucket name"
    default = ""
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"

}