import json
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dagster import asset, MetadataValue
from io import BytesIO
from us import states


def _get_census_url(year, context):
    """
    Get the URL for a Census ASPEP download page.
    """
    base_url = "https://www.census.gov/programs-surveys/apes/data/datasetstables/"
    if year == 2017 or year == 2018:
        url = f"https://www.census.gov/data/tables/{year}/econ/apes/annual-apes.html"
        context.log.info(f"URL: {url} (special case for 2017 or 2018)")
    else:
        url = f"{base_url}{year}.html"
        context.log.info(f"URL: {url}")

    return url 

@asset(description="Scrape URLs from Census website and export them to a JSON file.", required_resource_keys={"output_paths"})
def scrape_and_export_data_urls(context) -> dict:
    """
    Scrape the Census website to find links for State and Local Government Employment Data
    and export the results to a JSON file.
    """
    year_url_mapping = {}

    for year in range(2000, 2024):
        url = _get_census_url(year, context)
        response = requests.get(url)

        if response.status_code != 200:
            context.log.warning(f"Failed to fetch {url}, status code: {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all anchor tags and look for the one containing the desired text
        link = None
        for a_tag in soup.find_all('a'):
            if "State and Local Government Employment" in a_tag.get_text(strip=True):
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

@asset(description="Combine all years of data.")
def combine_years(context, scrape_and_export_data_urls: dict):
    data_urls = scrape_and_export_data_urls

    # Initialize an empty DataFrame to accumulate results
    combined_data = pd.DataFrame()

    # List to keep track of bad files
    bad_files = []

    # Helper function to extract headers dynamically
    def extract_headers(df):
        import re

        # Locate the row with "State" in the first column
        state_row_idx = df[df[0].str.strip() == "State"].index[0]

        # Walk back to find the first empty row
        empty_row_idx = None
        for idx in range(state_row_idx, -1, -1):
            if df.iloc[idx].isnull().all():
                empty_row_idx = idx
                break

        if empty_row_idx is None:
            raise ValueError("Could not find an empty row preceding the 'State' row.")

        # Collect headers row by row starting after the empty row and include the 'State' row
        header_rows = []
        for idx in range(empty_row_idx + 1, state_row_idx + 1):
            header_rows.append(df.iloc[idx])

        # Flatten multi-row headers into a single header row
        combined_headers = []
        for col_idx in range(len(header_rows[0])):
            if col_idx == 0:  # Skip processing for the zeroth column
                combined_name = "State"
            else:
                combined_name = " ".join(
                    str(row[col_idx]).strip() for row in header_rows if pd.notnull(row[col_idx])
                )

                # Prefix "Coefficient of variation (%)" columns with their left column name
                if "coefficient of variation (%)" in combined_name.lower() and col_idx > 0:
                    left_column_name = combined_headers[col_idx - 1]
                    combined_name = f"{left_column_name} Coefficient of Variation (%)"

            # Aggressively slugify column names and remove parentheses content
            combined_name = re.sub(r"\([^)]*\)", "", combined_name)  # Remove text in parentheses
            combined_name = re.sub(r"[^a-zA-Z0-9]+", "_", combined_name.strip()).lower()  # Slugify

            combined_headers.append(combined_name)

        context.log.debug(f"Extracted headers: {combined_headers}")
        return combined_headers

    # Process each year and its corresponding URL
    for year, row in data_urls.items():
        response = requests.get(row["data_url"])

        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            context.log.warning(f"Failed to fetch {row['data_url']} for year {year}: {str(e)}")
            bad_files.append({"year": year, "url": row["data_url"], "reason": str(e)})
            continue

        source_dir = os.path.join("data", "source")
        os.makedirs(source_dir, exist_ok=True)
        output_file_path = os.path.join(source_dir, f"{year}_source.xlsx")

        with open(output_file_path, "wb") as f:
            f.write(response.content)

        with BytesIO(response.content) as file:
            try:
                # Determine the file type based on the URL
                if row["data_url"].endswith(".xls"):
                    engine = "xlrd"
                elif row["data_url"].endswith(".xlsx"):
                    engine = "openpyxl"
                else:
                    raise ValueError(f"Unsupported file extension for {row['data_url']}")

                # Read the raw Excel file without skipping rows
                raw_df = pd.read_excel(file, engine=engine, header=None)

                # Extract dynamic headers
                headers = extract_headers(raw_df)

                # Locate the actual data start row (row with "State")
                data_start_idx = raw_df[raw_df[0].str.strip() == "State"].index[0] + 1

                # Read the data portion
                df = pd.read_excel(file, engine=engine, skiprows=data_start_idx, names=headers)

                # Drop entirely blank rows
                df.dropna(how="all", inplace=True)

                # Add the year column
                df["year"] = int(year)

                # Append to the combined DataFrame
                combined_data = pd.concat([combined_data, df], ignore_index=True)

                context.log.info(f"processed {year} w columns {df.columns}")
          
            except Exception as e:
                context.log.warning(f"Error processing file for year {year}: {str(e)}")
                bad_files.append({"year": year, "url": row["data_url"], "reason": str(e)})

    # Ensure "State" column exists before proceeding
    if "state" in combined_data.columns:
        combined_data.rename(columns=lambda x: x.strip(), inplace=True)

        # Drop rows where "State" or other key columns are missing or invalid
        combined_data = combined_data[combined_data["state"].notnull() & (combined_data["state"] != "")]

        # Dictionary replacements for state standardization using a mapping
        state_dict = {"us": "United States"}  # Add more mappings as needed
        combined_data.replace({"state": state_dict}, inplace=True)
    else:
        context.log.error("Missing 'state' column in the combined data. Check headers and data structure.")

    # Sort values if the DataFrame is not empty
    if not combined_data.empty:
        combined_data.sort_values(["state", "year"], inplace=True)

    # Log bad files to the context and add as metadata
    if bad_files:
        context.log.warning(f"Encountered issues with {len(bad_files)} files.")
        context.log.debug(f"Bad files: {bad_files}")
        context.add_output_metadata({"bad_files": bad_files})

    # Write combined data to a JSON file in the "out" directory
    output_path = os.path.join("data/out", "combined_data.json")
    os.makedirs("out", exist_ok=True)  # Ensure the "out" directory exists
    combined_data.to_json(output_path, orient="records", lines=False, indent=4)
    context.log.info(f"Combined data written to {output_path}")
    context.add_output_metadata({"output_file": output_path})

    return combined_data
