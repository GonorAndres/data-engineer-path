# =============================================================================
# BigQuery Module Outputs
# =============================================================================

output "dataset_ids" {
  description = "Map of logical dataset name to fully-qualified dataset ID"
  value = {
    for name, dataset in google_bigquery_dataset.datasets :
    name => dataset.dataset_id
  }
}

output "dataset_self_links" {
  description = "Map of logical dataset name to self_link"
  value = {
    for name, dataset in google_bigquery_dataset.datasets :
    name => dataset.self_link
  }
}
