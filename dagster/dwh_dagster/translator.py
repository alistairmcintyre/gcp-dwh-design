"""Customise how dbt nodes map onto Dagster assets.

``enable_asset_checks=True`` turns every dbt test into an asset check on the model it guards, so
pass/fail/warn shows up per-asset in the UI and a failing error-severity test fails the run.

Assets are grouped by their dbt layer (``staging`` / ``intermediate`` / ``marts``).

The default key mapping is kept: dbt sources -> ``AssetKey([source, table])`` (e.g. ``raw/users``),
models -> ``AssetKey([model_name])``. The dev ingestion asset in ``raw_data.py`` emits the same
``raw/<table>`` keys, so it becomes the upstream of the dbt sources.
"""

from __future__ import annotations

from typing import Any, Mapping, Optional

from dagster_dbt import DagsterDbtTranslator, DagsterDbtTranslatorSettings

DBT_TRANSLATOR_SETTINGS = DagsterDbtTranslatorSettings(
    enable_asset_checks=True,
)


class DwhDbtTranslator(DagsterDbtTranslator):
    """Group dbt models by their folder layer; everything else uses the defaults."""

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        # fqn = [project_name, <subfolder(s)...>, node_name]; the first subfolder is the layer.
        fqn = dbt_resource_props.get("fqn") or []
        if dbt_resource_props.get("resource_type") == "model" and len(fqn) >= 3:
            return fqn[1]
        return super().get_group_name(dbt_resource_props)
