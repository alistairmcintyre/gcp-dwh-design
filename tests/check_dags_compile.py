"""AST-compile every orchestration source file (no Airflow/Dagster needed) to catch syntax errors locally.

Run via `make dag-test`. For a full import check (needs Airflow installed), use
`tests/test_dag_integrity.py` in CI instead.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY_DIRS = [
    "airflow/include",
    "airflow/dags",
    "dagster/dwh_dagster",
]


def main() -> int:
    files: list[Path] = []
    for d in PY_DIRS:
        files += sorted((ROOT / d).glob("*.py"))

    errors: list[str] = []
    for f in files:
        try:
            ast.parse(f.read_text(), filename=str(f))
            print(f"  ok  {f.relative_to(ROOT)}")
        except SyntaxError as exc:
            errors.append(f"{f.relative_to(ROOT)}: {exc}")

    if errors:
        print("\nSYNTAX ERRORS:")
        for e in errors:
            print("  ", e)
        return 1
    print(f"\n{len(files)} files compiled OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
