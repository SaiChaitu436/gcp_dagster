from dagster import job, op, Definitions, AssetSelection, define_asset_job
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path
from ..project import buy_box_gcp_1_project

################## Define DBT CLI Resource ############################

dbt_cli_resource = DbtCliResource(
    project_dir=str(buy_box_gcp_1_project.project_dir),  
    profiles_dir=str(Path(buy_box_gcp_1_project.project_dir)),
)

################ Working Code Part ###################################

@op(required_resource_keys={"dbt"}) #Operation based on DBT resource
def run_dbt_seeds(context):
    context.resources.dbt.cli(["run", "--select", "path:seeds"]).wait()
    return "seeds_finished"  

# Define DBT base model operation
@op(required_resource_keys={"dbt"})
def run_dbt_base_model(context, seeds_status: str):
    if seeds_status == "seeds_finished": # As in Airflow we do not have any lineage of executing jobs
        context.resources.dbt.cli(["run", "--select", "src_offers"]).wait()


# Define the Dagster job, This decorator makes the methods we define apepar as a Dagster Job
@job(resource_defs={"dbt": dbt_cli_resource}) # Provide resources for the job 
def dbt_seed_pipeline():
    seed_status = run_dbt_seeds()
    run_dbt_base_model(seed_status)

#######################             ###################################

