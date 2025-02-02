from dagster import Definitions
from process_aspep.assets import scrape_and_export_aspep_urls, download_aspep_year, combine_years, s3_upload
from process_aspep.resources import output_paths_resource
from dagster_aws.s3 import s3_resource
from .asset_checks import asset_checks


# S3 config
s3 = s3_resource.configured({
    "region_name": "us-east-1",  
})

# Default paths for outputs
output_paths = output_paths_resource.configured({
    "paths": {
        "year_url_mapping": "data/out/year_url_mapping.json",
        "combined_data": "data/out/combined_data.json",
    }
})

defs = Definitions(
    assets=[scrape_and_export_aspep_urls, download_aspep_year, combine_years, s3_upload],
    asset_checks=asset_checks,
    resources={"output_paths": output_paths, "s3": s3},
)