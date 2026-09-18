#!/usr/bin/env bash
# Submit a job spec as a Dataproc Serverless batch.
#
# Serverless rather than a Dataproc cluster, deliberately:
#   * no cluster to size, patch, autoscale or forget to delete -- the usual source of "why is
#     Dataproc costing us four figures a month" is an idle cluster nobody owns
#   * per-batch runtime version, so upgrading Spark is a job-level decision, not a fleet migration
#   * billed per batch, scale-to-zero between runs, which suits a warehouse's spiky batch profile
# A long-lived cluster still wins for interactive/notebook work and for very high job frequency,
# where per-batch start-up latency (roughly a minute) starts to dominate.
#
#   ./submit.sh jobs/silver_customer_activity.yaml
#   ./submit.sh jobs/silver_customer_activity.yaml --validate-only
set -euo pipefail

JOB_SPEC="${1:?usage: submit.sh <job-spec.yaml> [--validate-only]}"
shift || true

PROJECT="${GCP_PROJECT:?set GCP_PROJECT}"
REGION="${GCP_REGION:-europe-west2}"
STAGING_BUCKET="${DATAPROC_STAGING_BUCKET:?set DATAPROC_STAGING_BUCKET (no gs:// prefix)}"
SERVICE_ACCOUNT="${DATAPROC_SERVICE_ACCOUNT:-dataproc-etl@${PROJECT}.iam.gserviceaccount.com}"
SUBNET="${DATAPROC_SUBNET:-default}"
RUNTIME_VERSION="${DATAPROC_RUNTIME_VERSION:-2.2}"

# Validate locally first. A malformed spec costs nothing to catch here and costs a batch submission,
# a minute of start-up and an opaque executor traceback to catch there.
python -m framework.main --config "${JOB_SPEC}" --validate-only

if [[ "${1:-}" == "--validate-only" ]]; then
  exit 0
fi

JOB_NAME="$(basename "${JOB_SPEC}" .yaml | tr '_' '-')"
BATCH_ID="${JOB_NAME}-$(date -u +%Y%m%d-%H%M%S)"

# The framework itself ships as a zip of modules; only main.py is the entrypoint. Keeping the
# framework versioned separately from the job specs is the whole point: specs live in GCS and change
# often, the framework changes rarely and is released deliberately.
FRAMEWORK_ZIP="$(mktemp -d)/framework.zip"
python -m zipfile -c "${FRAMEWORK_ZIP}" framework/

# Upload the spec so the batch reads it from GCS rather than baking it into the submission.
SPEC_URI="gs://${STAGING_BUCKET}/jobs/$(basename "${JOB_SPEC}")"
gsutil -q cp "${JOB_SPEC}" "${SPEC_URI}"

gcloud dataproc batches submit pyspark framework/main.py \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --batch="${BATCH_ID}" \
  --version="${RUNTIME_VERSION}" \
  --deps-bucket="gs://${STAGING_BUCKET}" \
  --py-files="${FRAMEWORK_ZIP}" \
  --service-account="${SERVICE_ACCOUNT}" \
  --subnet="${SUBNET}" \
  --labels="framework=dataproc-etl,job=${JOB_NAME}" \
  --properties="\
spark.executor.instances=2,\
spark.driver.cores=4,\
spark.executor.cores=4,\
spark.dynamicAllocation.enabled=true,\
spark.dynamicAllocation.minExecutors=2,\
spark.dynamicAllocation.maxExecutors=20" \
  -- --config "${SPEC_URI}"

echo "submitted batch ${BATCH_ID}"
echo "  logs: gcloud dataproc batches describe ${BATCH_ID} --region=${REGION} --project=${PROJECT}"
