import json
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dagster import asset, MetadataValue
from io import BytesIO

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

    # Column definitions for uniform formatting
    column_names = [
        "state", "gov_function", "ft_employment", "ft_pay",
        "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_pay"
    ]

    # Initialize an empty DataFrame to accumulate results
    combined_data = pd.DataFrame()

    # Process each year and its corresponding URL
    for year, row in data_urls.items():
        response = requests.get(row["data_url"])

        try:
            response.raise_for_status()
        except:
            context.log.warn(f"{url} didn't work.")
            continue

        # response.raise_for_status()
        # extension = os.path.splitext(url)[-1].lower()  # Get the file extension
        
        # engine = None
        # if extension in ['.xls']:
        #     engine = 'xlrd'
        # elif extension in ['.xlsx']:
        #     engine = 'openpyxl'
        # else:
        #     raise ValueError(f"Unsupported file extension: {extension}")

        with BytesIO(response.content) as file:
            # Guess row to skip based on known patterns
            skiprows = 4 if int(year) <= 2006 else 14

            # Read Excel file with the appropriate engine
            df = pd.read_excel(file, skiprows=skiprows, engine="openpyxl")

            # Drop unnamed columns and rename for consistency
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            df.columns = column_names[:len(df.columns)]

            # Add the year column
            df["year"] = int(year)

            # Append to the combined DataFrame
            combined_data = pd.concat([combined_data, df], ignore_index=True)

    # Clean up columns
    combined_data["gov_function"] = combined_data["gov_function"].str.strip().str.lower()
    combined_data["state"] = combined_data["state"].str.strip().str.lower()

    # Dictionary replacements for standardization
    state_dict = {
        "us": "united states",
        "al": "alabama",
        "ak": "alaska",
        # Add the rest of the states...
    }

    gov_function_dict = {
        "total": "total - all government employment functions",
        "financial admin": "financial administration",
        # Add other mappings as necessary
    }

    combined_data.replace({"state": state_dict, "gov_function": gov_function_dict}, inplace=True)

    # Sort values
    combined_data.sort_values(["state", "gov_function", "year"], inplace=True)

    return combined_data
