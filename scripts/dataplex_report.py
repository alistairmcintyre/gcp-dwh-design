"""Print the latest Dataplex data quality results, per rule and per dimension.

The Dataplex console shows this, but a terminal report is what you want in CI and in a morning
check: it exits non-zero when any scan failed, so it can gate a deploy or page someone, and it
prints the per-dimension scores a data owner actually cares about (is COMPLETENESS slipping?)
rather than a wall of individual rules.

    python scripts/dataplex_report.py                 # latest results for every scan
    python scripts/dataplex_report.py --run           # trigger the scans first, then wait
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time

import google.auth
import google.auth.transport.requests
import requests

GREEN, RED, DIM, RESET = "\033[32m", "\033[31m", "\033[2m", "\033[0m"
BASE = "https://dataplex.googleapis.com/v1"


def bearer_token() -> str:
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials.refresh(google.auth.transport.requests.Request())
    return credentials.token


def terraform_output(tf_dir: str) -> dict:
    result = subprocess.run(
        ["terraform", "output", "-json"], cwd=tf_dir, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        sys.exit(f"terraform output failed:\n{result.stderr.strip()}")
    return {k: v["value"] for k, v in json.loads(result.stdout or "{}").items()}


def fetch_scan(project: str, region: str, scan_id: str, token: str) -> dict:
    response = requests.get(
        f"{BASE}/projects/{project}/locations/{region}/dataScans/{scan_id}",
        params={"view": "FULL"},
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def trigger_scan(project: str, region: str, scan_id: str, token: str) -> None:
    requests.post(
        f"{BASE}/projects/{project}/locations/{region}/dataScans/{scan_id}:run",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    ).raise_for_status()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tf-dir", default="terraform/envs/dev")
    parser.add_argument("--run", action="store_true", help="trigger each scan, then poll for results")
    parser.add_argument("--timeout", type=int, default=300, help="seconds to wait when --run is used")
    args = parser.parse_args()

    outputs = terraform_output(args.tf_dir)
    project, region = outputs["project_id"], outputs["region"]
    scan_ids = sorted(outputs.get("data_quality_scan_ids", {}).values())
    if not scan_ids:
        sys.exit("no data quality scans in the Terraform outputs")

    token = bearer_token()

    if args.run:
        for scan_id in scan_ids:
            trigger_scan(project, region, scan_id, token)
            print(f"triggered {scan_id}")
        deadline = time.time() + args.timeout
        while time.time() < deadline:
            time.sleep(15)
            if all(
                fetch_scan(project, region, s, token).get("dataQualityResult") is not None
                for s in scan_ids
            ):
                break

    print(f"\n{'=' * 88}\n  Dataplex data quality -- {project} ({region})\n{'=' * 88}")
    any_failed = False

    for scan_id in scan_ids:
        scan = fetch_scan(project, region, scan_id, token)
        result = scan.get("dataQualityResult")
        table = scan.get("data", {}).get("resource", "").split("/")[-1]
        print(f"\n  {scan_id}  {DIM}({table}){RESET}")

        if not result:
            print(f"    {DIM}no completed run yet -- use --run{RESET}")
            continue

        passed = result.get("passed", False)
        any_failed = any_failed or not passed
        mark = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"    overall: [{mark}]  rows scanned: {result.get('rowCount', '?')}")

        for dimension in result.get("dimensions", []):
            name = dimension.get("dimension", {}).get("name", "?")
            ok = dimension.get("passed", False)
            print(f"      {(GREEN + 'ok  ') if ok else (RED + 'FAIL')}{RESET}  {name}")

        for rule_result in result.get("rules", []):
            rule = rule_result.get("rule", {})
            ok = rule_result.get("passed", False)
            kinds = [k for k in rule if k.endswith("Expectation")]
            ratio = rule_result.get("passRatio")
            ratio_text = f"{ratio:.4f}" if isinstance(ratio, (int, float)) else "-"
            print(
                f"        {(GREEN + 'PASS') if ok else (RED + 'FAIL')}{RESET}  "
                f"{(rule.get('column') or '(table)'):<18} {rule.get('dimension', ''):<13} "
                f"{(kinds[0] if kinds else ''):<26} pass_ratio={ratio_text}"
            )
            if not ok and rule.get("description"):
                print(f"              {DIM}{rule['description']}{RESET}")

    print(f"\n{'=' * 88}")
    if any_failed:
        print(f"  {RED}At least one scan failed.{RESET}\n")
        sys.exit(1)
    print(f"  {GREEN}All scans passed.{RESET}\n")


if __name__ == "__main__":
    main()
