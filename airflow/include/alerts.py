"""Slack failure alerting for Airflow DAGs, via a Slack incoming webhook.

Wired as ``on_failure_callback`` on the DAGs. Set ``SLACK_WEBHOOK_URL`` (a Composer env var, or local
``.env``) to enable; unset, it is a no-op. Uses stdlib ``urllib`` rather than
``apache-airflow-providers-slack`` so the DAGs still import under the CI DagBag-integrity check, which
installs a minimal Airflow.
"""

from __future__ import annotations

import json
import os
import urllib.request


def slack_failure_callback(context: dict) -> None:
    """Post a short failure message to SLACK_WEBHOOK_URL (no-op if unset)."""
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        return

    dag = context.get("dag")
    task_instance = context.get("task_instance")
    dag_id = getattr(dag, "dag_id", "?")
    task_id = getattr(task_instance, "task_id", "?")
    when = context.get("logical_date") or context.get("execution_date")
    log_url = getattr(task_instance, "log_url", "")

    text = (
        ":red_circle: *Airflow task failed*\n"
        f"DAG `{dag_id}` · task `{task_id}`\n"
        f"When: {when}\n"
        f"{log_url}"
    )
    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        webhook, data=payload, headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=10)  # noqa: S310 (trusted, user-supplied webhook)
    except Exception:  # noqa: BLE001 — alerting must never fail the callback
        pass
