#!/usr/bin/env bash
# One-off setup for the Dataflow load test (DataflowLoadTest). Safe to re-run.
#
#   GCP_PROJECT=your-project-id ./scripts/dataflow-setup.sh
#
# Creates, all outside Terraform, all removed by dataflow-teardown.sh:
#   - the Dataflow API
#   - gs://<project>-dataflow for staged jars and temp files, emptied after 7 days
#   - a worker service account with only what the job touches. The default Compute Engine account
#     is not used: the organisation blocks its automatic Editor grant, so it can do nothing anyway.
set -euo pipefail

PROJECT="${GCP_PROJECT:?set GCP_PROJECT}"
REGION="${REGION:-europe-west2}"
DATASET="${BQ_DATASET:-scratch}"
BUCKET="${BUCKET:-${PROJECT}-dataflow}"
SA_NAME="dataflow-loadtest"
SA="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"

echo "== Dataflow API"
gcloud services enable dataflow.googleapis.com --project "$PROJECT"

echo "== bucket gs://$BUCKET"
if ! gcloud storage buckets describe "gs://$BUCKET" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://$BUCKET" --project "$PROJECT" --location "$REGION" --uniform-bucket-level-access
fi
lifecycle="$(mktemp)"
echo '{"rule":[{"action":{"type":"Delete"},"condition":{"age":7}}]}' > "$lifecycle"
gcloud storage buckets update "gs://$BUCKET" --lifecycle-file="$lifecycle" >/dev/null
rm -f "$lifecycle"

echo "== service account $SA"
if ! gcloud iam service-accounts describe "$SA" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_NAME" --project "$PROJECT" --display-name "Dataflow load test workers"
  sleep 10  # a new account takes a moment before IAM will accept it as a member
fi

echo "== roles"
# Project level only where there is no narrower place to grant it.
#   dataflow.worker    run as a Dataflow worker
#   pubsub.subscriber  read the subscription (recreated every run, so a subscription-level grant would vanish)
#   pubsub.viewer      read schema revisions to decode messages
for role in roles/dataflow.worker roles/pubsub.subscriber roles/pubsub.viewer; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:$SA" --role "$role" \
    --condition=None --quiet >/dev/null
done
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" --member "serviceAccount:$SA" \
  --role roles/storage.objectAdmin >/dev/null
# Writes only to the scratch dataset. Through the dataset's access list: bq add-iam-policy-binding
# on a dataset needs allowlisting. WRITER on a dataset is roles/bigquery.dataEditor.
acl="$(mktemp)"
bq show --format=prettyjson "${PROJECT}:${DATASET}" > "$acl"
python3 - "$acl" "$SA" <<'PY'
import json, sys
path, sa = sys.argv[1], sys.argv[2]
ds = json.load(open(path))
entry = {"role": "WRITER", "userByEmail": sa}
if entry not in ds["access"]:
    ds["access"].append(entry)
json.dump({"access": ds["access"]}, open(path, "w"))
PY
bq update --source "$acl" "${PROJECT}:${DATASET}" >/dev/null
rm -f "$acl"

echo "ready. run:"
echo "  GCP_PROJECT=$PROJECT mvn -B -q compile exec:java -Dexec.mainClass=com.dwh.beam.DataflowLoadTest"
