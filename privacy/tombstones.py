"""Tombstones for the keyed topics, planned from the topic registry.

A tombstone is a record with the subject's key and a null value. On a compacted topic it makes the
log forget every earlier record for that key, and it tells every consumer to delete their copy. Both
halves matter: without the second you have erased the log and left the data in six warehouses.

Which topics can be tombstoned is a property of the topic, not of the erasure process, so it is
declared in streaming/topics.yaml next to the rest of the contract. A topic can only be tombstoned
if it is compacted and keyed by the subject. Anything else has to be shredded instead, and the
registry says which.

Timing is the part that catches people out. Compaction only touches closed segments, and by default
waits until half the log is uncompacted, so a low-volume topic can sit on a tombstone for weeks. The
registry pins the segment and lag settings for erasure topics so the delay is bounded and stated
rather than inherited from whatever the cluster default happens to be.
"""

from __future__ import annotations

import dataclasses
import pathlib

import yaml

REGISTRY = pathlib.Path(__file__).resolve().parents[1] / "streaming" / "topics.yaml"


@dataclasses.dataclass(frozen=True)
class Tombstone:
    topic: str
    key: str
    max_delay_hours: int

    def describe(self) -> str:
        return f"{self.topic} key={self.key} (compacted within ~{self.max_delay_hours}h)"


class RegistryError(ValueError):
    """A topic claims an erasure method its configuration cannot deliver."""


def load_topics(path: pathlib.Path = REGISTRY) -> list[dict]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))["topics"]


def check_erasure_config(topics: list[dict], subject_key: str = "client_id") -> list[str]:
    """Problems that would make an erasure request silently incomplete.

    Run in CI. A topic carrying personal data with no erasure method, or claiming tombstones without
    compaction, is the kind of mistake that stays invisible until someone asks you to prove a
    deletion and the answer has to be "we cannot".
    """
    problems = []
    for topic in topics:
        name = topic["name"]
        method = topic.get("erasure")

        if method is None:
            problems.append(f"{name}: no erasure method declared")
            continue
        if method not in {"tombstone", "retain", "none"}:
            problems.append(f"{name}: unknown erasure method {method!r}")
            continue

        # Declared PII has to be encrypted whatever else happens to the topic. A tombstone only
        # reaches the log; the key is what reaches the copies.
        if topic.get("pii") and not topic.get("encrypt_pii"):
            problems.append(f"{name}: declares pii fields but not encrypt_pii")

        if method == "tombstone":
            if topic.get("key") != subject_key:
                problems.append(
                    f"{name}: erasure=tombstone needs the topic keyed by {subject_key}, "
                    f"not {topic.get('key')!r}"
                )
            if topic.get("cleanup_policy") not in {"compact", "compact,delete"}:
                problems.append(
                    f"{name}: erasure=tombstone needs cleanup_policy compact or compact,delete"
                )
            if not topic.get("max_compaction_delay_hours"):
                problems.append(
                    f"{name}: erasure=tombstone needs max_compaction_delay_hours, or the delay is "
                    "whatever the cluster default happens to be"
                )
        elif method == "retain" and not topic.get("lawful_basis"):
            problems.append(f"{name}: erasure=retain must name the lawful basis for keeping it")
        elif method == "none" and topic.get("key") == subject_key:
            problems.append(
                f"{name}: erasure=none but it is keyed by {subject_key}, so it holds subjects"
            )
    return problems


def plan(subject_id: str, topics: list[dict] | None = None) -> list[Tombstone]:
    """The tombstones one erasure request produces."""
    topics = topics if topics is not None else load_topics()
    return [
        Tombstone(
            topic=topic["name"],
            key=subject_id,
            max_delay_hours=topic.get("max_compaction_delay_hours", 0),
        )
        for topic in topics
        if topic.get("erasure") == "tombstone"
    ]


def emit(tombstones: list[Tombstone], bootstrap_servers: str) -> int:
    """Produce the tombstones for real.

    Kept separate from `plan` so the planning is testable without a broker, and so the sweep can run
    in dry-run mode anywhere. Needs a Kafka client library, which this repo does not depend on: the
    deployed job runs inside an image that has one.
    """
    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError as exc:  # pragma: no cover - depends on the deployment image
        raise RuntimeError(
            "no Kafka client available. Install kafka-python in the job image, or run the sweep "
            "with --dry-run and hand the plan to the streaming platform."
        ) from exc

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    try:
        for tombstone in tombstones:
            # value=None is what makes it a tombstone. A zero-length value is not the same thing and
            # will sit in the log forever.
            producer.send(tombstone.topic, key=tombstone.key.encode("utf-8"), value=None)
        producer.flush()
    finally:
        producer.close()
    return len(tombstones)
