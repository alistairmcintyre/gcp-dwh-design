"""Entrypoint for a Dataproc Serverless batch.

One entrypoint, one image, N pipelines: the batch is always
`main.py --config gs://.../jobs/<name>.yaml`, so adding a pipeline never touches this file and never
requires a new build. That is the whole argument for the config-driven shape -- the alternative,
a PySpark file per pipeline, means every new source is a code review, an image build and a deploy,
and the fiftieth one looks nothing like the first.

    python -m framework.main --config gs://bucket/jobs/bronze_users.yaml
    python -m framework.main --config jobs/bronze_users.yaml --validate-only
"""

from __future__ import annotations

import argparse
import logging
import sys

from framework.config import ConfigError, JobSpec


def load_config_text(path: str) -> str:
    """Read the job spec from GCS or the local filesystem.

    Configs live in GCS in production so a pipeline change is an object upload gated by CI, not an
    image rebuild -- which is what makes "add a source" a five-minute change.
    """
    if path.startswith("gs://"):
        from google.cloud import storage

        bucket_name, _, blob_name = path[len("gs://"):].partition("/")
        client = storage.Client()
        return client.bucket(bucket_name).blob(blob_name).download_as_text()
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Job spec YAML: a gs:// URI or a local path")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Parse and validate the spec, then exit. Used as a CI gate so a malformed config "
             "never reaches a paid Dataproc batch.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)

    try:
        job = JobSpec.from_yaml(load_config_text(args.config))
    except ConfigError as exc:
        print(f"invalid job spec: {exc}", file=sys.stderr)
        return 2

    if args.validate_only:
        print(f"OK: '{job.name}' -- {len(job.sources)} source(s), {len(job.transforms)} transform(s), "
              f"{len(job.quality)} quality check(s), sink={job.sink.format}/{job.sink.mode}")
        return 0

    from framework.runner import run

    run(job)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
