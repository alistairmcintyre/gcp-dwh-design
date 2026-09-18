"""DORA-style engineering metrics from git history.

The JD asks for "DORA-style engineering metrics" reported to the Head of Data Engineering and the
CDO. This computes the four from git alone -- no external tooling, no instrumentation to install,
which is what makes it something a team will actually keep running.

    python scripts/dora_metrics.py --days 90
    python scripts/dora_metrics.py --days 90 --format json

WHAT DORA MEANS FOR A DATA TEAM
The four metrics were defined for application delivery and need honest adaptation, because a data
platform's failures do not look like an application's:

  Deployment frequency   -- unchanged: merges to main that reach production.
  Lead time for changes  -- unchanged: first commit on a branch to its merge.
  Change failure rate    -- ADAPTED. An app change fails by breaking the app. A data change fails by
                            producing WRONG NUMBERS, which nobody notices for a week. So the honest
                            denominator includes data incidents (a failed contract, a breached
                            freshness SLO, a mart that had to be rebuilt), not just rollbacks.
  Time to restore        -- ADAPTED. For data, "restored" is not "the pipeline runs again", it is
                            "the numbers are right again and consumers have been told". Backfill and
                            notification time count.

The metric this repo cannot compute from git is the one that matters most -- change failure rate
needs an incident source. `--incidents` takes a CSV so the number is real rather than flattering;
without it the script reports what it can and says so, which is better than a confident fiction.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
import subprocess
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone


def git(*args: str) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout.strip()


@dataclass
class Deployment:
    sha: str
    merged_at: datetime
    first_commit_at: datetime
    subject: str

    @property
    def lead_time_hours(self) -> float:
        return (self.merged_at - self.first_commit_at).total_seconds() / 3600


@dataclass
class Metrics:
    window_days: int
    deployments: list[Deployment] = field(default_factory=list)
    incidents: list[dict] = field(default_factory=list)

    @property
    def deployment_frequency_per_week(self) -> float:
        return len(self.deployments) / (self.window_days / 7) if self.window_days else 0.0

    @property
    def lead_time_median_hours(self) -> float | None:
        times = [d.lead_time_hours for d in self.deployments]
        return statistics.median(times) if times else None

    @property
    def lead_time_p90_hours(self) -> float | None:
        times = sorted(d.lead_time_hours for d in self.deployments)
        if not times:
            return None
        # p90 matters more than the median here: the median hides the change that sat in review for
        # a fortnight, and that change is the one that hurt.
        return times[min(int(len(times) * 0.9), len(times) - 1)]

    @property
    def change_failure_rate(self) -> float | None:
        if not self.deployments or not self.incidents:
            return None
        return len(self.incidents) / len(self.deployments)

    @property
    def median_time_to_restore_hours(self) -> float | None:
        durations = [i["restore_hours"] for i in self.incidents if i.get("restore_hours") is not None]
        return statistics.median(durations) if durations else None


def collect_deployments(days: int, main_branch: str) -> list[Deployment]:
    """Every merge into main in the window, with the lead time of its branch."""
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    log = git("log", main_branch, f"--since={since}", "--first-parent", "--format=%H%x1f%cI%x1f%s")
    if not log:
        return []

    deployments: list[Deployment] = []
    for line in log.splitlines():
        sha, merged_iso, subject = line.split("\x1f")
        merged_at = datetime.fromisoformat(merged_iso)

        # For a merge commit, the branch's first commit is the oldest one it brought in. For a
        # squashed or direct commit, author date is the closest honest proxy.
        parents = git("rev-list", "--parents", "-n", "1", sha).split()[1:]
        if len(parents) > 1:
            branch_commits = git("log", f"{parents[0]}..{parents[1]}", "--format=%aI").splitlines()
            first_commit_at = (
                datetime.fromisoformat(branch_commits[-1]) if branch_commits else merged_at
            )
        else:
            first_commit_at = datetime.fromisoformat(git("log", "-1", "--format=%aI", sha))

        deployments.append(Deployment(sha[:8], merged_at, first_commit_at, subject))
    return deployments


def load_incidents(path: str | None) -> list[dict]:
    """Incidents from a CSV: detected_at,resolved_at,severity,summary.

    Deliberately an external file. Deriving "was this change a failure?" from git alone produces a
    number that is always flattering and never true.
    """
    if not path:
        return []
    incidents = []
    with open(path, encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            detected = datetime.fromisoformat(row["detected_at"])
            resolved = datetime.fromisoformat(row["resolved_at"]) if row.get("resolved_at") else None
            incidents.append(
                {
                    "summary": row.get("summary", ""),
                    "severity": row.get("severity", ""),
                    "detected_at": detected,
                    "restore_hours": ((resolved - detected).total_seconds() / 3600) if resolved else None,
                }
            )
    return incidents


def render_text(metrics: Metrics) -> str:
    lines = [
        "",
        "=" * 74,
        f"  Engineering metrics -- last {metrics.window_days} days",
        "=" * 74,
        "",
        f"  Deployment frequency     {metrics.deployment_frequency_per_week:.1f} / week "
        f"({len(metrics.deployments)} in window)",
    ]

    median = metrics.lead_time_median_hours
    p90 = metrics.lead_time_p90_hours
    lines.append(
        f"  Lead time for changes    median {median:.1f}h, p90 {p90:.1f}h"
        if median is not None
        else "  Lead time for changes    no deployments in window"
    )

    cfr = metrics.change_failure_rate
    lines.append(
        f"  Change failure rate      {cfr:.1%} ({len(metrics.incidents)} incidents)"
        if cfr is not None
        else "  Change failure rate      NOT MEASURED -- pass --incidents to compute it honestly"
    )

    mttr = metrics.median_time_to_restore_hours
    lines.append(
        f"  Time to restore          median {mttr:.1f}h"
        if mttr is not None
        else "  Time to restore          NOT MEASURED -- needs incident data"
    )

    if metrics.deployments:
        by_week: dict[str, int] = defaultdict(int)
        for deployment in metrics.deployments:
            by_week[deployment.merged_at.strftime("%G-W%V")] += 1
        lines += ["", "  Deployments per week", ""]
        for week in sorted(by_week):
            lines.append(f"    {week}  {'#' * by_week[week]} {by_week[week]}")

    lines += ["", "=" * 74, ""]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--branch", default="main")
    parser.add_argument("--incidents", help="CSV: detected_at,resolved_at,severity,summary")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    args = parser.parse_args()

    metrics = Metrics(
        window_days=args.days,
        deployments=collect_deployments(args.days, args.branch),
        incidents=load_incidents(args.incidents),
    )

    if args.format == "json":
        print(json.dumps({
            "window_days": metrics.window_days,
            "deployment_frequency_per_week": round(metrics.deployment_frequency_per_week, 2),
            "lead_time_median_hours": metrics.lead_time_median_hours,
            "lead_time_p90_hours": metrics.lead_time_p90_hours,
            "change_failure_rate": metrics.change_failure_rate,
            "median_time_to_restore_hours": metrics.median_time_to_restore_hours,
            "deployment_count": len(metrics.deployments),
            "incident_count": len(metrics.incidents),
        }, indent=2))
    else:
        print(render_text(metrics))


if __name__ == "__main__":
    main()
