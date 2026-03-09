"""Dagster dbt assets."""

import logging
from pathlib import Path

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, dbt_assets

logger = logging.getLogger(__name__)

# Path to the compiled dbt manifest relative to the orchestration package
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt" / "energy_analytics"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


class _SourceTranslator(DagsterDbtTranslator):
    """Map dbt source asset keys to the upstream Dagster ingestion asset keys."""

    def get_asset_key(self, dbt_resource_props: dict) -> AssetKey:
        if dbt_resource_props.get("resource_type") == "source":
            # Strip the schema prefix — map to the bare Dagster asset key
            return AssetKey([dbt_resource_props["name"]])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    name="energy_dbt_assets",
    dagster_dbt_translator=_SourceTranslator(),
)
def energy_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Execute all dbt models in dependency order (Bronze → Silver → Gold)."""
    
    context.log.info("Starting dbt run — project_dir=%s", DBT_PROJECT_DIR)
    yield from dbt.cli(["run", "--project-dir", str(DBT_PROJECT_DIR)], context=context).stream()
    context.log.info("dbt run completed successfully")
