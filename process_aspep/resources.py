import pandas as pd
from dagster import resource
from dagster_aws.s3 import s3_resource

@resource
def output_paths_resource(context):
    # Return a dictionary of output paths
    return context.resource_config.get("paths", {
        "year_url_mapping": "year_url_mapping.json",
    })

@resource
def state_to_census_groups(context) -> dict:
    # Load census region dataset
    census_region_df = pd.read_csv('https://raw.githubusercontent.com/cphalpert/census-regions/master/us%20census%20bureau%20regions%20and%20divisions.csv')
    return census_region_df.set_index('State Code')[['State', 'Region', 'Division']].to_dict(orient="index")


s3 = s3_resource.configured({
    "region_name": "us-east-1",  
})