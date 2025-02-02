from dagster import resource
from dagster_aws.s3 import s3_resource

@resource
def output_paths_resource(context):
    # Return a dictionary of output paths
    return context.resource_config.get("paths", {
        "year_url_mapping": "year_url_mapping.json",
    })

s3 = s3_resource.configured({
    "region_name": "us-east-1",  
})