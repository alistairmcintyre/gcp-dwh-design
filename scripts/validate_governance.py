"""Prove the BigQuery access controls actually work, by querying as each persona.

An access control that has only been *applied* is an access control you are guessing about. This
script impersonates each persona service account, runs the same queries a real analyst would, and
asserts the outcome against what the Terraform + dbt configuration claims should happen. It exits
non-zero on any mismatch, so it works as a CI gate after every governance change -- the same way a
dbt test gates a data change.

Three controls are checked, in the order an attacker would probe them:

  1. Dataset IAM (the coarse control)
     Can the persona read the Bronze `raw` dataset, where the same columns sit untagged? If yes,
     every finer control below is theatre. This is the single most common way a masking
     implementation is defeated in practice, so it is checked first.

  2. Row-level security (row access policies on `dim_client`)
     Which `trading_region` rows come back? A UK desk analyst must see UK and nothing else.

  3. Column-level security (Dataplex policy tags + dynamic data masking)
     Selecting a tagged column returns the raw value (Fine-Grained Reader), a masked value (Masked
     Reader), or is REJECTED (neither). All three come from the SAME column in the SAME table --
     only the caller's grants differ, which is the whole point of masking in place. Where a value is
     masked, the returned VALUE is asserted, not merely that the query succeeded: a check that only
     confirms "it worked" passes just as happily against completely unmasked data.

Usage
-----
    python scripts/validate_governance.py                       # read config from terraform output
    python scripts/validate_governance.py --tf-dir path/to/env
    python scripts/validate_governance.py --verbose             # show returned rows
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field

import google.auth
from google.api_core import exceptions as gexc
from google.auth import impersonated_credentials
from google.cloud import bigquery

SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]

DEFAULT_TF_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "terraform", "envs", "dev"
)

GREEN, RED, YELLOW, DIM, RESET = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"


# ---------------------------------------------------------------------------------------------
# What each persona is supposed to be able to do. This is the executable version of the access
# model described in docs/governance.md -- if the doc and this table disagree, this one is right,
# because it is the one that runs.
# ---------------------------------------------------------------------------------------------
@dataclass
class Expectation:
    persona: str
    summary: str
    can_read_bronze: bool
    visible_regions: set[str] | None  # None = all regions
    # The three column outcomes, kept as separate sets because they come from three different
    # grants and must be asserted differently. A class in none of them is a configuration gap, and
    # the run fails rather than quietly skipping it.
    readable_pii: set[str] = field(default_factory=set)  # Fine-Grained Reader -> raw value
    masked_pii: set[str] = field(default_factory=set)    # Masked Reader       -> masked value
    denied_pii: set[str] = field(default_factory=set)    # neither             -> query rejected


PII_COLUMNS = {
    "person_name": ["first_name", "last_name"],
    "date_of_birth": ["date_of_birth"],
    "contact": ["email"],
}

EXPECTATIONS = [
    Expectation(
        persona="uk_desk",
        summary="UK trading desk: no PII, UK rows only",
        can_read_bronze=False,
        visible_regions={"UK"},
        readable_pii=set(),
        masked_pii={"person_name", "date_of_birth", "contact"},
    ),
    Expectation(
        persona="marketing",
        summary="Marketing: email for CDP activation, no names/DOB, all regions",
        can_read_bronze=False,
        visible_regions=None,
        readable_pii={"contact"},
        masked_pii={"person_name", "date_of_birth"},
    ),
    Expectation(
        persona="compliance",
        summary="Compliance/KYC: full PII, all regions",
        can_read_bronze=False,
        visible_regions=None,
        readable_pii={"person_name", "date_of_birth", "contact"},
    ),
    Expectation(
        persona="quants",
        summary="Quants: behavioural columns only, PII denied outright, all regions",
        can_read_bronze=False,
        visible_regions=None,
        denied_pii={"person_name", "date_of_birth", "contact"},
    ),
]


def _looks_like_sha256(value: object) -> bool:
    """True for any of the shapes BigQuery's SHA256 masking rule actually returns.

    Worth being explicit about, because it is not what the documentation leads you to expect and it
    is only visible if you assert on the VALUE rather than on the query succeeding: BigQuery returns
    the digest **base64-encoded** (44 characters, `=` padded), not lowercase hex. Hex and raw BYTES
    are accepted too so the check survives a column typed as BYTES or a future change.
    """
    if isinstance(value, (bytes, bytearray)):
        return len(value) == 32
    if not isinstance(value, str):
        return False
    if len(value) == 64 and all(c in "0123456789abcdef" for c in value.lower()):
        return True  # hex
    if len(value) == 44 and value.endswith("=") and "@" not in value:
        return True  # base64 of a 32-byte digest -- what BigQuery actually returns
    return False


def _assert_masked(pii_class: str, columns: list[str], rows: list) -> tuple[bool, str]:
    """Check returned values actually carry the masking rule configured for this class.

    Asserting only that the query succeeded would pass identically against unmasked data, which is
    the one outcome this whole exercise exists to rule out.

      person_name    DEFAULT_MASKING_VALUE -> '' for STRING
      date_of_birth  DATE_YEAR_MASK        -> 1 January of the birth year
      contact        SHA256                -> 64-char lowercase hex digest
    """
    if not rows:
        return False, "no rows returned, so nothing was proved"

    sample = rows[0]

    if pii_class == "person_name":
        values = [row[column] for row in rows for column in columns]
        blanked = all(value == "" or value is None for value in values)
        return blanked, (
            f"all {len(values)} name values blanked (DEFAULT_MASKING_VALUE)"
            if blanked
            else f"NOT masked -- saw {[v for v in values if v][:3]}"
        )

    if pii_class == "date_of_birth":
        dates = [row["date_of_birth"] for row in rows if row["date_of_birth"] is not None]
        year_only = all(d.month == 1 and d.day == 1 for d in dates)
        return year_only, (
            f"truncated to year (DATE_YEAR_MASK), e.g. {dates[0]}"
            if year_only and dates
            else f"NOT masked -- saw {dates[:3]}"
        )

    if pii_class == "contact":
        values = [row["email"] for row in rows if row["email"] is not None]
        if not values:
            return False, "no email values returned, so nothing was proved"

        # Two things have to hold, and the second is the one that matters to the business:
        #   1. the value is a digest, not an address -- nobody can mail it
        #   2. the mapping is DETERMINISTIC, so the masked value still self-joins and still supports
        #      count(distinct). That is the whole reason SHA256 was chosen over nulling the column;
        #      if distinctness collapsed, marketing analytics would break and the control would get
        #      routed around.
        hashed = all(_looks_like_sha256(v) for v in values)
        no_addresses = not any(isinstance(v, str) and "@" in v for v in values)
        distinct_preserved = len(set(values)) == len(values)

        passed = hashed and no_addresses and distinct_preserved
        sample = values[0] if isinstance(values[0], str) else values[0].hex()
        return passed, (
            f"SHA256 digest, e.g. {sample[:28]}... "
            f"({len(set(values))} distinct of {len(values)} -- joins and count(distinct) preserved)"
            if passed
            else f"NOT masked as expected -- saw {[str(v)[:40] for v in values[:2]]}"
        )

    return False, f"no masking assertion defined for class '{pii_class}'"


def terraform_output(tf_dir: str) -> dict:
    binary = shutil.which("terraform")
    if binary is None:
        sys.exit("terraform not found on PATH")
    result = subprocess.run(
        [binary, "output", "-json"], cwd=tf_dir, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        sys.exit(f"terraform output failed in {tf_dir}:\n{result.stderr.strip()}")
    return {k: v["value"] for k, v in json.loads(result.stdout or "{}").items()}


def client_for(persona_email: str | None, project: str, location: str) -> bigquery.Client:
    """A BigQuery client authenticated as a persona, or as the operator when persona_email is None."""
    source, _ = google.auth.default(scopes=SCOPES)
    if persona_email is None:
        return bigquery.Client(project=project, credentials=source, location=location)
    creds = impersonated_credentials.Credentials(
        source_credentials=source,
        target_principal=persona_email,
        target_scopes=SCOPES,
    )
    return bigquery.Client(project=project, credentials=creds, location=location)


def try_query(client: bigquery.Client, sql: str) -> tuple[bool, object]:
    """Run a query; return (ok, rows) or (False, error message). Access denials are the point here."""
    try:
        return True, list(client.query(sql).result())
    except (gexc.Forbidden, gexc.BadRequest) as exc:
        first_line = str(exc).split("\n")[0]
        return False, first_line


class Report:
    def __init__(self) -> None:
        self.failures: list[str] = []

    def check(self, label: str, passed: bool, detail: str) -> None:
        mark = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"    [{mark}] {label}")
        print(f"           {DIM}{detail}{RESET}")
        if not passed:
            self.failures.append(f"{label}: {detail}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--tf-dir", default=DEFAULT_TF_DIR)
    parser.add_argument("--verbose", action="store_true", help="print the rows each persona sees")
    args = parser.parse_args()

    tf = terraform_output(args.tf_dir)
    project = tf["project_id"]
    location = tf["region"]
    emails = tf["persona_emails"]
    masking_enabled = tf.get("data_masking_enabled", False)

    gold = f"`{project}.{tf['dataset_ids']['marts']}.dim_client`"
    bronze = f"`{project}.{tf['dataset_ids']['raw']}.clients`"

    print(f"\n{'=' * 92}")
    print(f"  Governance validation -- project {project} ({location})")
    print(f"  Dynamic data masking: {'ENABLED' if masking_enabled else 'UNAVAILABLE (project has no org parent)'}")
    print(f"{'=' * 92}")

    report = Report()

    # ---- Baseline: the operator, who is a grantee of the all-regions policy and project owner ----
    operator = client_for(None, project, location)
    ok, rows = try_query(operator, f"select count(*) as n from {gold}")
    total_rows = rows[0]["n"] if ok else 0
    print(f"\n  Baseline (operator, unrestricted): {total_rows} rows in dim_client")

    ok, region_rows = try_query(
        operator, f"select trading_region, count(*) as n from {gold} group by 1 order by 1"
    )
    all_regions = {r["trading_region"] for r in region_rows} if ok else set()
    print(f"  Regions present: {', '.join(sorted(all_regions))}")

    for expectation in EXPECTATIONS:
        persona_email = emails[expectation.persona]
        print(f"\n  {'-' * 88}")
        print(f"  PERSONA: {expectation.persona}  --  {expectation.summary}")
        print(f"  {DIM}{persona_email}{RESET}")
        print(f"  {'-' * 88}")
        client = client_for(persona_email, project, location)

        # --- 1. Dataset IAM: Bronze must be unreachable ---------------------------------------
        ok, result = try_query(client, f"select count(*) as n from {bronze}")
        report.check(
            "Bronze (raw.clients) is not readable",
            ok == expectation.can_read_bronze,
            f"query {'succeeded' if ok else 'denied'} -- expected "
            f"{'success' if expectation.can_read_bronze else 'denial'}."
            + ("" if ok else f" [{str(result)[:90]}]"),
        )

        # --- 2. Row-level security -------------------------------------------------------------
        ok, result = try_query(
            client, f"select trading_region, count(*) as n from {gold} group by 1 order by 1"
        )
        if not ok:
            report.check("Row access policy returns the expected regions", False, f"query denied: {result}")
        else:
            seen = {r["trading_region"] for r in result}
            expected = expectation.visible_regions if expectation.visible_regions is not None else all_regions
            counts = ", ".join(f"{r['trading_region']}={r['n']}" for r in result) or "(no rows)"
            report.check(
                "Row access policy returns the expected regions",
                seen == expected,
                f"saw {sorted(seen) or '[]'}, expected {sorted(expected)}  ({counts})",
            )

        # --- 3. Column-level security ----------------------------------------------------------
        for pii_class, columns in PII_COLUMNS.items():
            column_list = ", ".join(columns)
            # No LIMIT: distinctness of a hashed column cannot be demonstrated on three rows.
            ok, result = try_query(client, f"select {column_list} from {gold}")
            if pii_class in expectation.readable_pii:
                report.check(
                    f"Column-level: can read {pii_class} ({column_list})",
                    ok,
                    "returned raw values" if ok else f"unexpectedly denied: {str(result)[:110]}",
                )
                if ok and args.verbose:
                    for row in result[:3]:
                        print(f"           {DIM}{dict(row)}{RESET}")

            elif pii_class in expectation.masked_pii:
                if not masking_enabled:
                    # No data policies in this project, so Masked Reader cannot be granted and the
                    # correct behaviour degrades to rejection. Assert that, not a masked value.
                    denied = (not ok) and "Access Denied" in str(result)
                    report.check(
                        f"Column-level: {pii_class} ({column_list}) denied (masking unavailable)",
                        denied,
                        f"denied: {str(result)[:110]}" if not ok else "query SUCCEEDED -- PII exposed",
                    )
                elif not ok:
                    report.check(
                        f"Column-level: {pii_class} ({column_list}) is masked",
                        False,
                        f"denied instead of masked: {str(result)[:110]}",
                    )
                else:
                    # Assert the returned VALUE, not merely that the query succeeded -- a check that
                    # only confirms "it worked" passes identically against completely unmasked data.
                    passed, detail = _assert_masked(pii_class, columns, result)
                    report.check(f"Column-level: {pii_class} ({column_list}) is masked", passed, detail)
                    if args.verbose:
                        for row in result[:3]:
                            print(f"           {DIM}{dict(row)}{RESET}")

            elif pii_class in expectation.denied_pii:
                # No grant of either kind. BigQuery rejects the QUERY rather than silently omitting
                # the column -- so an analyst can never mistake a withheld value for a genuine null,
                # and an unauthorised export cannot happen quietly.
                denied = (not ok) and "Access Denied" in str(result)
                report.check(
                    f"Column-level: {pii_class} ({column_list}) is denied outright",
                    denied,
                    f"rejected: {str(result)[:110]}" if not ok else "query SUCCEEDED -- PII exposed",
                )

            else:
                report.check(
                    f"Column-level: {pii_class} unclassified for '{expectation.persona}'",
                    False,
                    "add it to readable_pii, masked_pii or denied_pii -- an unclassified column is "
                    "an untested control",
                )

    print(f"\n{'=' * 92}")
    if report.failures:
        print(f"  {RED}{len(report.failures)} check(s) FAILED{RESET}")
        for failure in report.failures:
            print(f"    - {failure}")
        print(f"{'=' * 92}\n")
        sys.exit(1)
    print(f"  {GREEN}All governance checks passed.{RESET}")
    print(f"{'=' * 92}\n")


if __name__ == "__main__":
    main()
