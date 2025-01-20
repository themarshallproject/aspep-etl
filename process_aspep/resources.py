from dagster import resource

@resource
def output_paths_resource(context):
    # Return a dictionary of output paths
    return context.resource_config.get("paths", {
        "csv_output": "default_csv_output.csv",
        "json_output": "default_json_output.json",
        "log_output": "default_log_output.log",
    })
