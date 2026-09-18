"""CI gate: check every contract in `contracts/` against the version registered on the main branch.

Runs the same compatibility rules the Cloud Run service exposes, but as a plain script with no
service, no network and no credentials -- the check has to be cheap enough that nobody is tempted to
skip it, and available on a fork PR that has no cloud access.

    python scripts/validate_contracts.py                 # compare against origin/main
    python scripts/validate_contracts.py --base HEAD~1
"""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys

import yaml

REPO = pathlib.Path(__file__).resolve().parents[1]
CONTRACTS_DIR = REPO / "contracts"
sys.path.insert(0, str(REPO / "services" / "contract-api"))

from app.compatibility import check_compatibility, check_version_bump  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"


def contract_at_revision(path: pathlib.Path, revision: str) -> dict | None:
    """The contract as it exists at `revision`, or None if it did not exist yet."""
    relative = path.relative_to(REPO)
    result = subprocess.run(
        ["git", "show", f"{revision}:{relative}"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return yaml.safe_load(result.stdout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="origin/main", help="revision to compare against")
    args = parser.parse_args()

    paths = sorted(CONTRACTS_DIR.glob("*.yaml"))
    if not paths:
        print("no contracts found", file=sys.stderr)
        return 1

    failures = 0
    for path in paths:
        proposed = yaml.safe_load(path.read_text(encoding="utf-8"))
        current = contract_at_revision(path, args.base)

        if current is None:
            print(f"  {GREEN}NEW{RESET}   {path.name} {DIM}(not on {args.base}; nothing to break){RESET}")
            continue

        violations = check_compatibility(current, proposed)
        version_problems = check_version_bump(current, proposed, violations)

        if not violations and not version_problems:
            print(f"  {GREEN}OK{RESET}    {path.name}")
            continue

        # A breaking change that IS declared with a major bump is allowed -- teams must be able to
        # break contracts, just never silently. Only an undeclared one fails the build.
        if violations and not version_problems:
            print(f"  {YELLOW}BREAK{RESET} {path.name} {DIM}(declared via major version bump){RESET}")
            for violation in violations:
                print(f"          {DIM}{violation}{RESET}")
            continue

        failures += 1
        print(f"  {RED}FAIL{RESET}  {path.name}")
        for problem in version_problems:
            print(f"          {problem}")
        for violation in violations:
            print(f"          {DIM}{violation}{RESET}")

    if failures:
        print(f"\n{RED}{failures} contract(s) changed incompatibly without a major version bump.{RESET}")
        print("Either keep the change backward compatible, or bump the MAJOR version and notify the")
        print("consumers listed in the contract.\n")
        return 1

    print(f"\n{GREEN}All contracts are compatible with {args.base}.{RESET}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
