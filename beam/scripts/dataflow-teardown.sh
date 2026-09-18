#!/usr/bin/env bash
# Removes everything dataflow-setup.sh and DataflowLoadTest created, except the BigQuery tables.
#
#   GCP_PROJECT=your-project-id ./scripts/dataflow-teardown.sh
#   DROP_TABLES=1 GCP_PROJECT=your-project-id ./scripts/dataflow-teardown.sh   # tables too
set -uo pipefail

PROJECT="${GCP_PROJECT:?set GCP_PROJECT}"
REGION="${REGION:-europe-west2}"
DATASET="${BQ_DATASET:-scratch}"
BUCKET="${BUCKET:-${PROJECT}-dataflow}"
SA="dataflow-loadtest@${PROJECT}.iam.gserviceaccount.com"

echo "== running load test jobs (should be none)"
for job in $(gcloud dataflow jobs list --project "$PROJECT" --region "$REGION" --status active \
               --filter "name~^schema-autoupdate" --format 'value(id)'); do
  gcloud dataflow jobs cancel "$job" --project "$PROJECT" --region "$REGION"
done

echo "== Pub/Sub"
gcloud pubsub subscriptions delete loadtest-dataflow-sub --project "$PROJECT" --quiet 2>/dev/null
gcloud pubsub topics delete loadtest-dataflow --project "$PROJECT" --quiet 2>/dev/null
gcloud pubsub schemas delete loadtest-trade --project "$PROJECT" --quiet 2>/dev/null

echo "== roles and service account"
acl="$(mktemp)"
bq show --format=prettyjson "${PROJECT}:${DATASET}" > "$acl" && python3 - "$acl" "$SA" <<'PY' && bq update --source "$acl" "${PROJECT}:${DATASET}" >/dev/null
import json, sys
path, sa = sys.argv[1], sys.argv[2]
ds = json.load(open(path))
json.dump({"access": [a for a in ds["access"] if a.get("userByEmail") != sa]}, open(path, "w"))
PY
rm -f "$acl"
for role in roles/dataflow.worker roles/pubsub.subscriber roles/pubsub.viewer; do
  gcloud projects remove-iam-policy-binding "$PROJECT" --member "serviceAccount:$SA" --role "$role" \
    --condition=None --quiet >/dev/null 2>&1
done
gcloud iam service-accounts delete "$SA" --project "$PROJECT" --quiet 2>/dev/null

echo "== bucket"
gcloud storage rm --recursive "gs://$BUCKET" --quiet 2>/dev/null

if [[ "${DROP_TABLES:-0}" == "1" ]]; then
  echo "== tables"
  for t in loadtest_dataflow_raw loadtest_dataflow_parsed loadtest_dataflow_failed; do
    bq rm -f -t "${PROJECT}:${DATASET}.${t}" 2>/dev/null
  done
fi
echo "done (Dataflow API left enabled; it costs nothing idle)"
