"""Run-failure alerting, including dbt test failures.

A dbt test with ``severity: error`` that fails makes ``dbt build`` exit non-zero, which fails the asset
step and the run, firing the sensors below. The failed test also shows as a red asset check on the model
it guards. ``severity: warn`` records a WARN check without failing the run (matching dbt's semantics); to
alert on warns, use an asset-check sensor or a Dagster+ alert policy.

Three sinks, all env-gated so the demo runs with none configured:

* ``slack_webhook_on_run_failure`` — always registered; posts to ``SLACK_WEBHOOK_URL`` via stdlib
  ``urllib`` (no extra dependency).
* Slack bot token sensor (``dagster-slack``) — added when ``DAGSTER_SLACK_BOT_TOKEN`` is set.
* Email sensor — added when the SMTP env vars are set.
"""

import json
import os
import urllib.request
from typing import List

from dagster import (
    DefaultSensorStatus,
    RunFailureSensorContext,
    SensorDefinition,
    run_failure_sensor,
)


def _failure_text(context: RunFailureSensorContext) -> str:
    run = context.dagster_run
    error = (context.failure_event.message or "").strip() or "unknown error"
    return (
        f":red_circle: *Dagster run failed* — job `{run.job_name}`\n"
        f"> {error}\n"
        f"Run ID: `{run.run_id}`"
    )


@run_failure_sensor(
    name="slack_webhook_on_run_failure",
    default_status=DefaultSensorStatus.RUNNING,
    description="Post to a Slack incoming webhook (SLACK_WEBHOOK_URL) on any run failure, incl. dbt tests.",
)
def slack_webhook_on_run_failure(context: RunFailureSensorContext) -> None:
    webhook = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook:
        context.log.info("SLACK_WEBHOOK_URL not set — skipping Slack alert.")
        return
    payload = json.dumps({"text": _failure_text(context)}).encode("utf-8")
    req = urllib.request.Request(
        webhook, data=payload, headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=10)  # noqa: S310 (trusted, user-supplied webhook)
        context.log.info("Posted failure alert to Slack webhook.")
    except Exception as exc:  # noqa: BLE001 — alerting must never crash the sensor
        context.log.error(f"Failed to post Slack alert: {exc}")


def build_alert_sensors() -> List[SensorDefinition]:
    """Assemble the failure sensors, adding the optional token/email sinks when configured."""
    sensors: List[SensorDefinition] = [slack_webhook_on_run_failure]

    slack_token = os.getenv("DAGSTER_SLACK_BOT_TOKEN")
    if slack_token:
        from dagster_slack import make_slack_on_run_failure_sensor

        sensors.append(
            make_slack_on_run_failure_sensor(
                channel=os.getenv("DAGSTER_SLACK_CHANNEL", "#data-alerts"),
                slack_token=slack_token,
                default_status=DefaultSensorStatus.RUNNING,
            )
        )

    smtp_host = os.getenv("DAGSTER_SMTP_HOST")
    email_from = os.getenv("DAGSTER_ALERT_EMAIL_FROM")
    email_password = os.getenv("DAGSTER_ALERT_EMAIL_PASSWORD")
    email_to = os.getenv("DAGSTER_ALERT_EMAIL_TO")
    if smtp_host and email_from and email_password and email_to:
        from dagster import make_email_on_run_failure_sensor

        sensors.append(
            make_email_on_run_failure_sensor(
                email_from=email_from,
                email_password=email_password,
                email_to=[addr.strip() for addr in email_to.split(",") if addr.strip()],
                smtp_host=smtp_host,
                smtp_port=int(os.getenv("DAGSTER_SMTP_PORT", "587")),
                smtp_type=os.getenv("DAGSTER_SMTP_TYPE", "STARTTLS"),
                default_status=DefaultSensorStatus.RUNNING,
            )
        )

    return sensors
