from dagster import resource

@resource
def output_paths_resource(context):
    # Return a dictionary of output paths
    return context.resource_config.get("paths", {
        "year_url_mapping": "year_url_mapping.json",
    })
