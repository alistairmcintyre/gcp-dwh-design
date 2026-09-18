# Dev environment settings. Put your own values in local.auto.tfvars, which git ignores and
# terraform picks up automatically.
#
# The project must sit inside a Google Cloud organization for dynamic data masking: the Data Policy
# API refuses to create data policies outside one. Without an org everything else still works, and
# an ungranted principal is denied on a tagged column rather than shown a masked value.
project_id = "your-project-id"
region     = "europe-west2"

# The person running the stack. Needs an explicit all-regions row access grant, because BigQuery has
# no owner bypass for row-level security, plus Token Creator to impersonate the personas.
operator_member = "user:you@example.com"

# The identity dbt runs as. Holds Fine-Grained Reader on every PII class, since building a masked
# derivative means reading the raw columns. In production this is a dedicated service account whose
# runs are logged, not a person.
build_identity_member = "user:you@example.com"
