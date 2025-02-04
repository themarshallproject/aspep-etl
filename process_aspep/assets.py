import gzip
import json
import numpy as np
import os
import pandas as pd
import re
import requests
import unicodedata
from bs4 import BeautifulSoup
from dagster import asset, AssetExecutionContext, Output, MetadataValue
from .constants import ASPEP_DATA_CONFIG, COLUMN_MAP, GOV_FUNCTION_MAP, STATE_MAP

PROCESSED_DIRECTORY = "data/out"
BUCKET_NAME = "tmp-gfx-public-data"
S3_PREFIX = "aspep"  # Target "directory" in S3

START_YEAR = 2003
END_YEAR = 2024

def _get_census_url(year, context):
    """
    Get the URL for a Census ASPEP download page.
    """
    if year == 2017 or year == 2018:
        url = f"https://www.census.gov/data/tables/{year}/econ/apes/annual-apes.html"
    elif year == 2014:
        url = "https://www.census.gov/data/datasets/2014/econ/apes/annual-apes.html"
    else:
        url = f"https://www.census.gov/programs-surveys/apes/data/datasetstables/{year}.html"
    
    context.log.info(f"Getting Census URL: {url} / year: {year}")
    return url


def classify_state_scope(state_code):
    """Classify national level or state level"""
    if state_code == "US":
        return "national"
    return "state"


def slugify(text):
    """Convert text to a slug format, removing non-alphanumeric characters and normalizing Unicode."""
    text = str(text)
    text = re.sub(r'\s+', '_', text.strip())  # Replace spaces with underscores
    text = re.sub(r'[^a-zA-Z0-9_]', '', text)  # Remove non-alphanumeric characters
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')  # Normalize Unicode
    return text.lower()


def extract_column_names(df, config):
    """Extract column names from a given dataframe based on the configured header range."""
    header_start = config.get("header_start", 0)
    header_end = config.get("header_end", header_start + 2)
    
    raw_headers = df.iloc[header_start:header_end + 1].astype(str).replace("nan", "").agg(' '.join, axis=0)
    cleaned_headers = [slugify(re.sub(r'\(.*?\)', '', col).strip()) for col in raw_headers]
    
    # Ensure the first column is always 'state'
    if cleaned_headers:
        cleaned_headers[0] = "state"
        cleaned_headers[1] = "gov_function"
    
    return cleaned_headers


def get_state_census_groups(state_code):
    """Ensure a three-element list is always returned from the mapping of state code to census regions."""
    mapping = state_to_census_groups.get(state_code, None)
    if isinstance(mapping, dict):
        return [mapping.get('State'), mapping.get('Region'), mapping.get('Division')]
    return [state_code, None, None]  # Default for missing mappings


def upload_file_to_s3(context, local_path, s3_key):
    """
    Upload a single file to S3 with public-read access.
    
    Args:
        context (AssetExecutionContext): Dagster execution context.
        local_path (str): The local file path to upload.
        s3_key (str): The S3 key (path) to store the file under.

    Returns:
        str: Public URL of the uploaded file.
    """
    s3_client = context.resources.s3

    try:
        # Check if the file is a text-based format to gzip
        is_text = local_path.endswith((".json", ".csv", ".txt"))
        compressed_path = f"{local_path}.gz" if is_text else local_path

        if is_text:
            context.log.info(f"Gzipping: {local_path} -> {compressed_path}")
            with open(local_path, "rb") as f_in, gzip.open(compressed_path, "wb") as f_out:
                f_out.writelines(f_in)
        
        # Upload file with public-read ACL
        extra_args = {"ACL": "public-read"}
        if is_text:
            extra_args.update({"ContentType": "text/plain", "ContentEncoding": "gzip"})
        
        s3_client.upload_file(compressed_path, BUCKET_NAME, s3_key, ExtraArgs=extra_args)
        context.log.info(f"Uploaded {compressed_path} to s3://{BUCKET_NAME}/{s3_key}")

        # Generate public URL
        public_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{s3_key}"
        return public_url

    except Exception as e:
        context.log.error(f"Failed to upload {local_path} to S3: {e}")
        return None


@asset(
    description="Scrape URLs from Census website and export them to a JSON file.",
    required_resource_keys={"output_paths"},
    group_name="download"
)
def scrape_and_export_aspep_urls(context) -> dict:
    """
    Scrape the Census website to find links for State and Local Government Employment Data
    and export the results to a JSON file.
    """
    year_url_mapping = {}

    for year in range(START_YEAR, END_YEAR + 1):
        url = _get_census_url(year, context)
        response = requests.get(url)

        if response.status_code != 200:
            context.log.warning(f"Failed to fetch {url}, status code: {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all anchor tags and look for the one containing the desired text
        link = None
        for a_tag in soup.find_all('a'):
            if "State Government Employment" in a_tag.get_text(strip=True):
                link = a_tag
                break
        
        if link and link.get('href'):
            full_url = link['href']
            year_url_mapping[year] = {"year": year, "source_url": url, "data_url": full_url}
        else:
            context.log.warning(f"No matching link found for year {year}")

    # Export to JSON
    output_file = context.resources.output_paths["year_url_mapping"]
    with open(output_file, mode="w") as file:
        json.dump({"data": year_url_mapping}, file, indent=4)

    context.log.info(f"JSON exported to {output_file}")
    context.add_output_metadata({
        "total_years": len(year_url_mapping.values()),
        "output_file": MetadataValue.path(output_file),
    })

    return year_url_mapping


@asset(
    description="Download Excel files for a specific year, but skip if cached.",
    group_name="download"
)
def download_aspep_year(context, scrape_and_export_aspep_urls: dict) -> dict:
    """
    Download Excel files for each year if not already cached.
    """
    downloaded_files = {}

    for year, row in scrape_and_export_aspep_urls.items():
        file_extension = ".xlsx" if ".xlsx" in row["data_url"] else ".xls"
        output_file = os.path.join("data/raw", f"aspep_{year}{file_extension}")

        # Check if file already exists
        if os.path.exists(output_file):
            context.log.info(f"File for year {year} already exists at {output_file}, skipping download.")
            downloaded_files[year] = output_file
            continue

        # Try downloading if not cached
        try:
            response = requests.get(row["data_url"])
            response.raise_for_status()

            with open(output_file, "wb") as file:
                file.write(response.content)

            downloaded_files[year] = output_file
            context.log.info(f"Downloaded file for year {year} to {output_file}")

        except requests.exceptions.RequestException as e:
            context.log.warning(f"Failed to fetch {row['data_url']} for year {year}: {str(e)}")

    context.add_output_metadata({"total_downloaded": len(downloaded_files)})

    return downloaded_files


@asset(
    description="Combine all years of data.",
    required_resource_keys={"output_paths", "state_to_census_groups"},
    group_name="process"
)
def combine_years(context, download_aspep_year: dict) -> pd.DataFrame:
    state_to_census_groups = context.resources.state_to_census_groups  # Get from resource

    combined_data = pd.DataFrame()
    bad_files = []

    items = [(year, file_path) for year, file_path in download_aspep_year.items() if year >= START_YEAR and year < END_YEAR]

    for year, file_path in items:
        try:
            engine = "openpyxl" if file_path.endswith(".xlsx") else "xlrd"
            config = ASPEP_DATA_CONFIG.get(year, {})

            raw_df = pd.read_excel(file_path, engine=engine, header=None)
            
            # Extract column names using the header range
            new_columns = extract_column_names(raw_df, config)
            raw_df.columns = new_columns
            
            # Drop the header rows since we extracted column names from them
            raw_df = raw_df.iloc[config["header_end"]:].reset_index(drop=True)
            
            # Drop any completely empty columns and column named ""
            raw_df = raw_df.dropna(axis=1, how='all')
            if "" in raw_df.columns:
                raw_df = raw_df.drop(columns=[""])

            # Map slugified columns to expected common names (if applicable)
            raw_df.rename(columns=COLUMN_MAP, inplace=True)
            
            raw_df["year"] = year
            
            raw_df["gov_function"] = raw_df["gov_function"].str.strip()
            raw_df["gov_function"] = raw_df["gov_function"].str.lower()
            
            raw_df["state"] = raw_df["state"].str.strip()
            raw_df["state"] = raw_df["state"].str.lower()
            raw_df = raw_df.replace({"state": STATE_MAP, "gov_function": GOV_FUNCTION_MAP}).reset_index()
            raw_df["state code"] = raw_df["state"].str.upper()

            state_groups = raw_df['state code'].apply(get_state_census_groups)
            raw_df[['state', 'region', 'division']] = pd.DataFrame(state_groups.tolist(), index=raw_df.index)
            raw_df['state_scope'] = raw_df['state code'].apply(classify_state_scope)
 
            combined_data = pd.concat([combined_data, raw_df], ignore_index=True)

            context.log.info(f"Processed year {year} with columns: {raw_df.columns.tolist()}")
        
        except Exception as e:
            context.log.warning(f"Error processing file for year {year}: {str(e)}")
            context.log.info(f"bad columns {raw_df.columns}")
            bad_files.append({"year": year, "file": file_path, "reason": str(e)})

    if combined_data.index.duplicated().any():
        context.log.warning(f"Non-unique index detected: {combined_data.index[combined_data.index.duplicated()].tolist()}")

    # Ensure 'state' column is present and clean up
    if "state" in combined_data.columns:
        combined_data = combined_data[combined_data["state"].notnull() & (combined_data["state"] != "")]
        combined_data.sort_values(["state", "year", "gov_function"], inplace=True)

    output_path = context.resources.output_paths["combined_data"]
    combined_data.to_json(output_path, orient="records", lines=False, indent=4)
    context.log.info(f"Combined data written to {output_path}")
    context.add_output_metadata({"output_file": output_path})
    
    if bad_files:
        context.log.warning(f"Encountered issues with {len(bad_files)} files.")
        context.add_output_metadata({"bad_files": bad_files})
    
    return combined_data


@asset(
    required_resource_keys={"s3"},
    description="Upload all files from 'data/out' to a public s3 bucket.",
    group_name="process"
)
def s3_upload(context: AssetExecutionContext, combine_years: pd.DataFrame) -> Output[list]:
    uploaded_files = []
    for root, _, files in os.walk(PROCESSED_DIRECTORY):
        for filename in files:
            local_path = os.path.join(root, filename)
            s3_key = os.path.join(S3_PREFIX, os.path.relpath(local_path, PROCESSED_DIRECTORY))
            s3_key = s3_key.replace("\\", "/")  # Ensure consistent path formatting

            public_url = upload_file_to_s3(context, local_path, s3_key)
            if public_url:
                uploaded_files.append({"file": filename, "url": public_url})

    return Output(value=uploaded_files, metadata={
        "uploaded_files": MetadataValue.json(uploaded_files),
    })

import pandas as pd
import numpy as np

@asset(
    description="Derive pay metrics and add nationwide statistics.",
    required_resource_keys={"output_paths"},
    group_name="process",
    deps=[combine_years]
)
def derive_stats(context, combine_years: pd.DataFrame) -> pd.DataFrame:
    derived_data = combine_years.copy()
    
    # Ensure numeric types
    numeric_cols = ["total_pay", "ft_eq_employment", "pt_pay", "pt_hour", "ft_pay", "ft_employment"]
    for col in numeric_cols:
        derived_data[col] = pd.to_numeric(derived_data[col], errors='coerce')
    
    # Compute derived metrics, avoiding division by zero
    derived_data["pay_per_fte"] = derived_data["total_pay"].div(derived_data["ft_eq_employment"].replace(0, np.nan))
    derived_data["pay_per_pt_hour"] = derived_data["pt_pay"].div(derived_data["pt_hour"].replace(0, np.nan))
    derived_data["pay_per_ft"] = derived_data["ft_pay"].div(derived_data["ft_employment"].replace(0, np.nan))
    
    # Handle potential division by zero
    derived_data.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    # Filter out nationwide sum (US state code)
    state_filtered_data = derived_data[derived_data["state code"] != "US"]
    
    # Identify strictly numeric columns for statistics
    exclude_cols = ['index', 'state', 'gov_function', 'state code', 'region', 'division', 'state_scope', 'year']
    stat_columns = [col for col in derived_data.columns if col not in exclude_cols and pd.api.types.is_numeric_dtype(derived_data[col])]
    
    # Compute descriptive statistics grouped by year and gov_function
    grouped_stats = state_filtered_data.groupby(["year", "gov_function"])[stat_columns].agg(['median', 'mean'])
    grouped_stats.columns = ['_'.join(col).strip() for col in grouped_stats.columns.values]
    grouped_stats = grouped_stats.reset_index()
    
    # Add a special state identifier
    grouped_stats.insert(0, "state code", "US-mean")
    grouped_stats.insert(1, "state_scope", "stats")
    
    # Append the stats to the dataset
    derived_data = pd.concat([derived_data, grouped_stats], ignore_index=True)
    
    # Save the derived dataset
    output_path = context.resources.output_paths["derived_pay_metrics"]
    derived_data.to_json(output_path, orient="records", lines=False, indent=4)
    
    context.log.info(f"Derived pay metrics with statistics written to {output_path}")
    context.add_output_metadata({"output_file": output_path})
    
    return derived_data
