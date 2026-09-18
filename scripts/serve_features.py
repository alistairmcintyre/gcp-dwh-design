"""Fetch online features for a customer, the way a serving application would.

This is the other half of terraform/modules/vertex_feature_store: the infrastructure syncs a
BigQuery table into a low-latency online store, and this is what reads it back on the hot path.

    python scripts/serve_features.py usr-00000042
    python scripts/serve_features.py usr-00000042 --compare   # online vs BigQuery, side by side

WHY --compare EXISTS
Training/serving skew is the failure this whole component prevents, so the tool that demonstrates it
should be able to *show* the two paths agreeing. `--compare` reads the same entity from the online
store and from the BigQuery source table and diffs them. A mismatch means the sync is stale or the
feature definition moved -- and that is a data engineering incident, not a data science one, which
is the argument for the DE team owning this component.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time


def terraform_output(tf_dir: str) -> dict:
    result = subprocess.run(
        ["terraform", "output", "-json"], cwd=tf_dir, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        sys.exit(f"terraform output failed in {tf_dir}:\n{result.stderr.strip()}")
    return {k: v["value"] for k, v in json.loads(result.stdout or "{}").items()}


def fetch_online(project: str, region: str, online_store: str, view: str, entity_id: str) -> dict:
    """Read one entity's features from the online store.

    Note the endpoint: the Feature Online Store has its OWN regional endpoint, distinct from the
    Vertex AI API endpoint used for training and prediction. Using the wrong one is the usual reason
    a first attempt at this returns NOT_FOUND for an entity that plainly exists.
    """
    from google.cloud.aiplatform_v1 import (
        FeatureOnlineStoreServiceClient,
        FeatureViewDataKey,
        FetchFeatureValuesRequest,
    )

    client = FeatureOnlineStoreServiceClient(
        client_options={"api_endpoint": f"{region}-aiplatform.googleapis.com"}
    )
    name = (
        f"projects/{project}/locations/{region}"
        f"/featureOnlineStores/{online_store}/featureViews/{view}"
    )

    started = time.perf_counter()
    response = client.fetch_feature_values(
        request=FetchFeatureValuesRequest(
            feature_view=name,
            data_key=FeatureViewDataKey(key=entity_id),
            data_format=FetchFeatureValuesRequest.Format.KEY_VALUE,
        )
    )
    latency_ms = (time.perf_counter() - started) * 1000

    features = {}
    for pair in response.key_values.features:
        value = pair.value
        # The oneof carries whichever type the feature actually is; read the set field.
        field = value.WhichOneof("value")
        features[pair.name] = getattr(value, field) if field else None

    return {"features": features, "latency_ms": round(latency_ms, 1)}


def fetch_bigquery(project: str, table: str, entity_column: str, entity_id: str) -> dict:
    from google.cloud import bigquery

    client = bigquery.Client(project=project)
    query = f"select * from `{table}` where {entity_column} = @entity_id"
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("entity_id", "STRING", entity_id)]
        ),
    )
    rows = list(job.result())
    return dict(rows[0]) if rows else {}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("entity_id", help="customer id, e.g. usr-00000042")
    parser.add_argument("--tf-dir", default="terraform/envs/dev")
    parser.add_argument("--compare", action="store_true", help="diff the online store against BigQuery")
    args = parser.parse_args()

    tf = terraform_output(args.tf_dir)
    project, region = tf["project_id"], tf["region"]
    online_store = tf.get("feature_online_store")
    view = tf.get("feature_view")
    if not online_store or not view:
        sys.exit(
            "no feature store in the Terraform outputs. Enable the vertex_feature_store module in "
            "terraform/envs/dev and apply it first (note: an online store bills continuously)."
        )

    result = fetch_online(project, region, online_store, view, args.entity_id)
    print(f"\n  Online store  ({result['latency_ms']} ms)")
    for name, value in sorted(result["features"].items()):
        print(f"    {name:<24} {value}")

    if not args.compare:
        return

    source = tf.get("feature_source_table", f"{project}.features.feat_client_activity")
    offline = fetch_bigquery(project, source, "client_id", args.entity_id)
    print("\n  BigQuery source")
    for name, value in sorted(offline.items()):
        print(f"    {name:<24} {value}")

    # Compare only the registered features; the source carries extra columns the view does not sync.
    drift = []
    for name, online_value in result["features"].items():
        if name not in offline:
            continue
        if str(offline[name]) != str(online_value):
            drift.append(f"{name}: online={online_value!r} bigquery={offline[name]!r}")

    print()
    if drift:
        print("  SKEW DETECTED -- the online store is stale or the feature definition moved:")
        for line in drift:
            print(f"    {line}")
        sys.exit(1)
    print("  Online and offline agree. No training/serving skew.\n")


if __name__ == "__main__":
    main()
