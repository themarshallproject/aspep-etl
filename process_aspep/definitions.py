from dagster import Definitions
from process_aspep.assets import scrape_data, export_to_csv
from process_aspep.resources import output_paths_resource

# Default paths for outputs
output_paths = output_paths_resource.configured({
    "paths": {
        "csv_output": "year_url_mapping.csv",
        "json_output": "output_data.json",
        "log_output": "logs/output.log",
    }
})

defs = Definitions(
    assets=[scrape_data, export_to_csv],
    resources={"output_paths": output_paths},
)