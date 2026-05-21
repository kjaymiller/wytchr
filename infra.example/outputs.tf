output "database_url" {
  description = "libpq-style connection URI for the wytchr database."
  value       = aiven_pg.wytchr.service_uri
  sensitive   = true
}
