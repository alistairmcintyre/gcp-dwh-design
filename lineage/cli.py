"""Column lineage from the terminal.

    python -m lineage.cli trace  marts.dim_client.email          where did it come from?
    python -m lineage.cli impact raw.clients.email               what does it feed?
    python -m lineage.cli pii                                    the checks; non-zero exit on a gap

Needs `dbt compile` (for the SQL) and `dbt docs generate` (for the columns) to have run first.
`make lineage-check` does both.
"""

from __future__ import annotations

import argparse
import sys

from lineage import checks
from lineage.graph import Column, build


def _column(text: str) -> Column:
    schema, table, name = text.lower().rsplit(".", 2)
    return Column(f"{schema}.{table}", name)


def trace(args) -> int:
    graph = build(args.target)
    column = _column(args.column)
    paths = graph.trace(column)
    if paths == [[column]]:
        print(f"{column} has nothing upstream: it's a source column, or not a column dbt builds")
        return 0
    for path in paths:
        print("  " + "  <-  ".join(str(c) for c in path))
    return 0


def impact(args) -> int:
    graph = build(args.target)
    affected = sorted(graph.impact(_column(args.column)))
    for column in affected:
        print(f"  {column}")
    tables = len({c.table for c in affected})
    print(f"\n{len(affected)} column(s) in {tables} table(s) built from it")
    return 0


def pii(args) -> int:
    graph = build(args.target)
    findings = checks.run(graph)

    print("personal data reaches:")
    for table in sorted(findings.reach):
        print(f"  {table:<40} {', '.join(sorted(findings.reach[table]))}")

    problems = 0
    for table in sorted(findings.missing_from_inventory):
        problems += 1
        print(f"\nNOT IN THE ERASURE INVENTORY: {table}")
        print("  an erasure request would leave this table untouched; add it to "
              "privacy/erasure_targets.yaml")
    for column in findings.untagged_served_columns:
        problems += 1
        print(f"\nUNTAGGED PERSONAL DATA IN A SERVED TABLE: {column}")
        print(f"  from: {'  <-  '.join(str(c) for c in graph.trace(column)[0][1:])}")
        print("  no pii_class means no policy tag and no masking; tag it in the model's yml")
    for column in findings.unresolved_near_pii:
        problems += 1
        print(f"\nCOULDN'T TRACE, NEXT TO PERSONAL DATA: {column}")

    if problems:
        print(f"\n{problems} problem(s)")
        return 1
    print("\nevery table personal data reaches is in the erasure inventory, and every served "
          "column carrying it is tagged")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--target", default="dbt/target", help="dbt target directory")
    sub = parser.add_subparsers(dest="command", required=True)
    for name, func, helptext in [("trace", trace, "where a column came from"),
                                 ("impact", impact, "everything built from a column")]:
        p = sub.add_parser(name, help=helptext)
        p.add_argument("column", help="schema.table.column")
        p.set_defaults(func=func)
    sub.add_parser("pii", help="the erasure and tagging checks").set_defaults(func=pii)
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
