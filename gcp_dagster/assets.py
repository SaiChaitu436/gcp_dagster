from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from gcp_dagster.project import buy_box_gcp_1_project


@dbt_assets(manifest=buy_box_gcp_1_project.manifest_path)
def buy_box_gcp_1_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
    