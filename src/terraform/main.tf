provider "google" {
  credentials = file(".credentials/service-account.json")
  project     = var.PROJECT_ID
  region      = "us-central1"
  zone        = "us-central1-a"
}