"""Generate one Bronze offload job spec per Kafka topic from the registry.

This is what makes 200 topics tractable: the registry (topics.yaml) is the only thing a human
writes, and every derived artefact is generated from it. Onboarding a topic is a pull request
against one file, not a new pipeline.

Generated output is written to `spark/jobs/generated/` and is NOT edited by hand -- if a generated
job needs to differ, the difference belongs in the registry so that every future topic inherits it.
Hand-editing generated pipelines is how a config-driven estate quietly becomes 200 bespoke ones.

    python streaming/generate_topic_jobs.py --check      # CI: fail if generated output is stale
    python streaming/generate_topic_jobs.py --write      # regenerate
"""

from __future__ import annotations

import argparse
import pathlib
import sys

import yaml

REPO = pathlib.Path(__file__).resolve().parents[1]
REGISTRY = REPO / "streaming" / "topics.yaml"
OUTPUT_DIR = REPO / "spark" / "jobs" / "generated"

BANNER = "# GENERATED from streaming/topics.yaml by streaming/generate_topic_jobs.py. Do not edit.\n"

# PII classes recognised across the estate. Kept in sync with the Terraform taxonomy and dbt's
# `meta.pii_class`; a topic declaring anything else fails validation rather than silently landing
# unclassified personal data in Bronze.
VALID_PII_CLASSES = {"person_name", "date_of_birth", "contact"}
REQUIRED_KEYS = {"name", "owner", "subject", "bronze_table", "dedupe_key", "order_by", "freshness_slo_minutes"}


class RegistryError(ValueError):
    pass


def load_registry(path: pathlib.Path = REGISTRY) -> list[dict]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    topics = data.get("topics") or []
    if not topics:
        raise RegistryError("registry contains no topics")
    return topics


def validate(topics: list[dict]) -> None:
    """Fail loudly on a malformed registry entry.

    Every check here corresponds to a production failure: a missing dedupe key means duplicate rows
    in Bronze, an unknown PII class means personal data landing untagged, a duplicate Bronze table
    means two topics silently overwriting each other.
    """
    seen_names: set[str] = set()
    seen_tables: set[str] = set()

    for topic in topics:
        name = topic.get("name", "<unnamed>")

        missing = REQUIRED_KEYS - topic.keys()
        if missing:
            raise RegistryError(f"{name}: missing required keys {sorted(missing)}")

        if name in seen_names:
            raise RegistryError(f"duplicate topic name '{name}'")
        seen_names.add(name)

        table = topic["bronze_table"]
        if table in seen_tables:
            raise RegistryError(f"{name}: bronze_table '{table}' is already used by another topic")
        seen_tables.add(table)

        unknown = set((topic.get("pii") or {}).values()) - VALID_PII_CLASSES
        if unknown:
            raise RegistryError(
                f"{name}: unknown pii class(es) {sorted(unknown)}. Known: {sorted(VALID_PII_CLASSES)}"
            )

        if not topic["dedupe_key"]:
            raise RegistryError(f"{name}: dedupe_key must be non-empty -- Kafka is at-least-once")

        slo = topic["freshness_slo_minutes"]
        if not isinstance(slo, int) or slo <= 0:
            raise RegistryError(f"{name}: freshness_slo_minutes must be a positive integer")


def _tombstoned(topic: dict) -> bool:
    """Whether erasure reaches this topic as a null-valued record."""
    return topic.get("erasure") == "tombstone"


def _transforms(topic: dict) -> list[dict]:
    """Dedupe and stamp. Tombstoned topics get a delete marker first.

    A tombstone arrives with the key populated and every value column null, which is why the
    connector runs with behavior.on.null.values=write rather than skipping them: a skipped tombstone
    erases the topic and leaves the warehouse untouched. Bronze keeps the marker rather than applying
    it, because Bronze is a record of what arrived; Silver decides what the current state is.
    """
    stamp = {
        "_ingested_at": "current_timestamp()",
        "event_date": "cast(event_timestamp as date)",
        "_kafka_offset": "kafka_offset",
        "_kafka_partition": "kafka_partition",
    }
    dedupe_on = list(topic["order_by"])

    if _tombstoned(topic):
        stamp["_is_delete"] = f"{topic['dedupe_key'][0]} is null"
        # A tombstone carries no event time, so ordering falls back to when the broker took it.
        # Without this the delete sorts unpredictably against the record it is meant to supersede.
        stamp["_event_time"] = "coalesce(event_timestamp, kafka_timestamp)"
        stamp["event_date"] = "cast(coalesce(event_timestamp, kafka_timestamp) as date)"
        dedupe_on = ["_event_time"]

    transforms = [{"type": "with_columns", "columns": stamp}] if _tombstoned(topic) else []
    transforms.append({
        "type": "deduplicate",
        # Tombstones share the subject key, not the event key, so a compacted topic dedupes on the
        # key the log itself is compacted by.
        "keys": [topic["key"]] if _tombstoned(topic) else list(topic["dedupe_key"]),
        "order_by": dedupe_on,
        "descending": True,
    })
    if not _tombstoned(topic):
        transforms.append({"type": "with_columns", "columns": stamp})
    return transforms


def _quality(topic: dict) -> list[dict]:
    """The gate before the write. Delete markers are exempt from the rules about payload contents."""
    exempt = " or _is_delete" if _tombstoned(topic) else ""
    key_column = topic["key"] if _tombstoned(topic) else topic["dedupe_key"][0]
    return [
        {"name": "key_present", "expression": f"{key_column} is not null", "on_failure": "fail"},
        {
            "name": "event_timestamp_present",
            "expression": f"event_timestamp is not null{exempt}",
            "on_failure": "fail",
        },
        {
            "name": "event_timestamp_not_in_future",
            "expression": f"event_timestamp <= current_timestamp() + interval 1 hour{exempt}",
            "on_failure": "warn",
        },
    ]


def build_job_spec(topic: dict) -> dict:
    """Render one topic into the Spark framework's job-spec shape."""
    return {
        "name": f"bronze_{topic['bronze_table']}",
        "description": (
            f"Bronze offload for Kafka topic {topic['name']} "
            f"(owner: {topic['owner']}, freshness SLO: {topic['freshness_slo_minutes']}m). "
            "Generated from the topic registry."
        ),
        "sources": [
            {
                "name": "topic_offload",
                "format": "gcs",
                "options": {
                    "format": topic.get("format", "avro"),
                    "path": f"gs://${{LANDING_BUCKET}}/kafka/{topic['name']}/dt=${{execution_date}}/*",
                },
            }
        ],
        "transforms": _transforms(topic),
        "quality": _quality(topic),
        "sink": {
            "format": "bigquery",
            "mode": "overwrite",
            "partition_by": [topic.get("partition_field", "event_date")],
            "options": {
                "table": f"${{GCP_PROJECT}}.{topic.get('bronze_dataset', 'raw')}.{topic['bronze_table']}",
                "writeMethod": "direct",
                "partitionType": "DAY",
                **({"clusteredFields": topic["key"]} if topic.get("key") else {}),
            },
        },
        "spark_conf": {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        },
    }


def render(topic: dict) -> str:
    return BANNER + yaml.safe_dump(build_job_spec(topic), sort_keys=False, width=100)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--write", action="store_true", help="regenerate job specs")
    group.add_argument("--check", action="store_true", help="fail if generated output is stale (CI gate)")
    args = parser.parse_args()

    try:
        topics = load_registry()
        validate(topics)
    except RegistryError as exc:
        print(f"invalid topic registry: {exc}", file=sys.stderr)
        return 2

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    expected = {f"bronze_{t['bronze_table']}.yaml": render(t) for t in topics}

    if args.check:
        stale: list[str] = []
        for filename, content in expected.items():
            path = OUTPUT_DIR / filename
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                stale.append(filename)
        orphaned = [p.name for p in OUTPUT_DIR.glob("*.yaml") if p.name not in expected]
        if stale or orphaned:
            print("generated job specs are stale. Run: python streaming/generate_topic_jobs.py --write", file=sys.stderr)
            for name in stale:
                print(f"  out of date: {name}", file=sys.stderr)
            for name in orphaned:
                print(f"  orphaned (topic removed from the registry): {name}", file=sys.stderr)
            return 1
        print(f"OK: {len(expected)} generated job spec(s) match the registry")
        return 0

    for path in OUTPUT_DIR.glob("*.yaml"):
        if path.name not in expected:
            path.unlink()  # a topic removed from the registry must lose its pipeline
    for filename, content in expected.items():
        (OUTPUT_DIR / filename).write_text(content, encoding="utf-8")
    print(f"wrote {len(expected)} job spec(s) to {OUTPUT_DIR.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
