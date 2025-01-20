from dagster import Definitions
from process_aspep.assets import scrape_and_export_data_urls, combine_years
from process_aspep.resources import output_paths_resource

# Default paths for outputs
output_paths = output_paths_resource.configured({
    "paths": {
        "year_url_mapping": "data/out/year_url_mapping.json",
    }
})

defs = Definitions(
    assets=[scrape_and_export_data_urls, combine_years],
    resources={"output_paths": output_paths},
)