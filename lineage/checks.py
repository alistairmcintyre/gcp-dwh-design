"""What the lineage graph is for: three checks that fail CI.

    1. every table personal data reaches is in the erasure inventory
    2. every Gold or feature column carrying personal data is tagged, so it gets a policy tag and
       masking, rather than arriving in a mart as an untagged copy
    3. no column next to personal data is untraceable

The first two are the reason to have column-level lineage at all. Table-level lineage would say
dim_client depends on stg_clients, which is true and useless here: what matters is whether the email
column made it through, and into which columns.
"""

from __future__ import annotations

import dataclasses
import pathlib

import yaml

from lineage.graph import Column, LineageGraph

INVENTORY = pathlib.Path(__file__).resolve().parents[1] / "privacy" / "erasure_targets.yaml"
# Where analysts read from. A personal-data column here without a tag has no policy tag and no
# masking, whatever the governance docs say.
SERVED_SCHEMAS = ("marts", "features")


@dataclasses.dataclass
class Findings:
    reach: dict[str, set[str]]
    missing_from_inventory: set[str]
    untagged_served_columns: list[Column]
    unresolved_near_pii: list[Column]

    @property
    def ok(self) -> bool:
        return not (self.missing_from_inventory or self.untagged_served_columns
                    or self.unresolved_near_pii)


def inventory_tables(path: pathlib.Path = INVENTORY) -> set[str]:
    spec = yaml.safe_load(path.read_text(encoding="utf-8"))
    return {
        t["table"].lower() for t in spec["targets"]
        if t.get("engine", "warehouse") == "warehouse"
    }


def _served(table: str) -> bool:
    schema = table.split(".")[0]
    return any(schema == s or schema.startswith(f"{s}_") for s in SERVED_SCHEMAS)


def run(graph: LineageGraph, inventory: set[str] | None = None) -> Findings:
    inventory = inventory if inventory is not None else inventory_tables()
    reach = graph.pii_reach()

    by_table: dict[str, set[str]] = {}
    for column, classes in reach.items():
        by_table.setdefault(column.table, set()).update(classes)

    untagged = sorted(c for c in reach if _served(c.table) and c not in graph.pii)

    # A column that couldn't be traced, in a model that reads a table carrying personal data, might
    # be carrying it too. Fail closed.
    pii_tables = set(by_table)
    unresolved = sorted(c for c, reads in graph.unresolved.items() if reads & pii_tables)

    return Findings(
        reach=by_table,
        missing_from_inventory=set(by_table) - inventory,
        untagged_served_columns=untagged,
        unresolved_near_pii=unresolved,
    )
