"""Column-level lineage for the dbt project, worked out from the SQL itself.

Nobody draws this graph and nobody maintains it. It comes from two files dbt already writes:

    target/manifest.json   every model's compiled SQL, and which columns are tagged pii_class
    target/catalog.json    every relation's actual columns (from `dbt docs generate`)

For each column of each model, sqlglot parses the compiled SQL and follows the expression back to
the columns it reads. Chaining those one-hop edges across models gives lineage from a Bronze column
to every Gold column built from it. Because it reads compiled SQL, ephemeral models are already
inlined and never appear as tables of their own.

The same graph answers three questions:

    trace    where did this column come from?
    impact   what does this column feed, all the way down?
    reach    which tables does personal data end up in?

A column sqlglot can't trace is recorded, not dropped. Treating it as clean would be the one mistake
a lineage check exists to prevent.
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
from collections import defaultdict, deque

from sqlglot import exp
from sqlglot.lineage import lineage as column_lineage

DIALECTS = {"duckdb": "duckdb", "bigquery": "bigquery"}


@dataclasses.dataclass(frozen=True, order=True)
class Column:
    table: str  # schema.name, lower case; the database/project is left out on purpose
    name: str

    def __str__(self) -> str:
        return f"{self.table}.{self.name}"


@dataclasses.dataclass
class LineageGraph:
    upstream: dict[Column, set[Column]] = dataclasses.field(
        default_factory=lambda: defaultdict(set)
    )
    downstream: dict[Column, set[Column]] = dataclasses.field(
        default_factory=lambda: defaultdict(set)
    )
    # Columns sqlglot couldn't trace, with the tables their model reads from. A check that meets
    # one of these has to assume the worst about it.
    unresolved: dict[Column, set[str]] = dataclasses.field(default_factory=dict)
    pii: dict[Column, str] = dataclasses.field(default_factory=dict)

    def add_edge(self, source: Column, target: Column) -> None:
        self.upstream[target].add(source)
        self.downstream[source].add(target)

    def trace(self, column: Column) -> list[list[Column]]:
        """Every path from this column back to a column with nothing upstream of it."""
        paths: list[list[Column]] = []

        def walk(node: Column, path: list[Column]) -> None:
            parents = self.upstream.get(node)
            if not parents:
                paths.append(path)
                return
            for parent in sorted(parents):
                if parent not in path:
                    walk(parent, path + [parent])

        walk(column, [column])
        return paths

    def impact(self, column: Column) -> set[Column]:
        """Everything built from this column, however many models away."""
        seen: set[Column] = set()
        queue = deque([column])
        while queue:
            for child in self.downstream.get(queue.popleft(), ()):
                if child not in seen:
                    seen.add(child)
                    queue.append(child)
        return seen

    def pii_reach(self) -> dict[Column, set[str]]:
        """Every column personal data flows into, with the classes that reach it."""
        reach: dict[Column, set[str]] = defaultdict(set)
        for column, pii_class in self.pii.items():
            reach[column].add(pii_class)
            for child in self.impact(column):
                reach[child].add(pii_class)
        return dict(reach)


def _relation(name: str, dialect: str) -> str:
    table = exp.to_table(name, dialect=dialect)
    return f"{table.db}.{table.name}".lower()


def build(target_dir: str | pathlib.Path = "dbt/target") -> LineageGraph:
    target = pathlib.Path(target_dir)
    manifest = json.loads((target / "manifest.json").read_text())
    catalog = json.loads((target / "catalog.json").read_text())
    adapter = manifest["metadata"]["adapter_type"]
    dialect = DIALECTS.get(adapter, adapter)

    # sqlglot needs every relation's columns to expand `select *` and qualify bare names.
    schema: dict = {}
    for entry in [*catalog["nodes"].values(), *catalog["sources"].values()]:
        meta = entry["metadata"]
        columns = {name: col["type"] for name, col in entry["columns"].items()}
        tables = schema.setdefault(meta["database"], {}).setdefault(meta["schema"], {})
        tables[meta["name"]] = columns

    graph = LineageGraph()
    nodes = {**manifest["nodes"], **manifest["sources"]}

    for node in nodes.values():
        relation = node.get("relation_name")
        if not relation:
            continue
        for name, col in node.get("columns", {}).items():
            pii_class = (col.get("meta") or {}).get("pii_class")
            if pii_class and pii_class != "none":
                graph.pii[Column(_relation(relation, dialect), name.lower())] = pii_class

    for unique_id, node in manifest["nodes"].items():
        if node["resource_type"] != "model" or node["config"].get("materialized") == "ephemeral":
            continue
        sql = node.get("compiled_code")
        entry = catalog["nodes"].get(unique_id)
        if not sql or not entry:
            continue
        target_table = _relation(node["relation_name"], dialect)
        reads = {
            _relation(nodes[parent]["relation_name"], dialect)
            for parent in node["depends_on"]["nodes"]
            if nodes.get(parent, {}).get("relation_name")
        }
        for name in entry["columns"]:
            output = Column(target_table, name.lower())
            try:
                root = column_lineage(name, sql, schema=schema, dialect=dialect)
            except Exception:  # noqa: BLE001 - recorded, and treated as possibly personal
                graph.unresolved[output] = reads
                continue
            for node_ in root.walk():
                if isinstance(node_.expression, exp.Table):
                    source = Column(
                        f"{node_.expression.db}.{node_.expression.name}".lower(),
                        node_.name.split(".")[-1].lower(),
                    )
                    graph.add_edge(source, output)
    return graph
