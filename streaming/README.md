# Kafka ingestion

The topic registry. Each Kafka topic is a row of configuration rather than its own pipeline:
`topics.yaml` holds the schema subject, dedupe key, ordering, owner, freshness target, personal-data
fields and how an erasure request reaches the topic. A Bronze load job for each topic is generated
from it.

```bash
python streaming/generate_topic_jobs.py --write   # regenerate after changing the registry
python streaming/generate_topic_jobs.py --check   # CI: fails if the generated jobs are stale
```

`topics.yaml` → `spark/jobs/generated/*.yaml` → Dataproc Serverless batches.

There's no Kafka cluster, Connect worker or schema registry here. This is the warehouse side of the
contract; the brokers belong to whoever runs the platform, and `topics.yaml` is the interface
between the two.

Which ingestion path to use, where Avro should be decoded, and the delivery decisions:
[decision guide, section 6](../docs/decision-guide.md#6-kafka-ingestion). How erasure reaches a
topic: [section 1](../docs/decision-guide.md#1-erasure-requests).
