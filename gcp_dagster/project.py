# from pathlib import Path

# from dagster_dbt import DbtProject

# buy_box_gcp_1_project = DbtProject(
#     project_dir=Path(__file__).joinpath("..", "..", "..", "buy_box_gcp_1").resolve(),
#     packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
# )
# buy_box_gcp_1_project.prepare_if_dev()

from pathlib import Path
from dagster_dbt import DbtProject

buy_box_gcp_1_project = DbtProject(
    project_dir=(Path(__file__).parent.parent / "buy_box_gcp_1").resolve(),
    packaged_project_dir=(Path(__file__).parent.parent / "dbt-project").resolve(),
)
buy_box_gcp_1_project.prepare_if_dev()
