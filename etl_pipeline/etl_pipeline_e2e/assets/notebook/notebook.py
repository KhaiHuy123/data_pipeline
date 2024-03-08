from dagstermill import define_dagstermill_asset, ConfigurableLocalOutputNotebookIOManager, \
    local_output_notebook_io_manager, define_dagstermill_op
from dagster import file_relative_path, Definitions, job

analysis_notebook = define_dagstermill_asset(
    name="analysis_notebook",
    notebook_path=file_relative_path(__file__, "..\\..\\..\\notebooks\\dagster_notebook.ipynb"),
    io_manager_key="output_notebook_io_manager",
)

defs = Definitions(
    assets=[analysis_notebook],
    resources={"output_notebook_io_manager": local_output_notebook_io_manager}
)

