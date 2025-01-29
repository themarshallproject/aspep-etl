import json
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dagster import asset, MetadataValue
from io import BytesIO
from us import states

START_YEAR = 2000
END_YEAR = 2023

data_config = {
    2000: {
        "skiprows": 4,
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_pay"
        ]
    },
    2001: {
        "skiprows": 6,
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_pay"
        ]
    },
    2002: {
        "skiprows": 4,
        "drop_columns": ["Unnamed: 9"],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_pay"
        ]
    },
    2003: {
        "skiprows": 4,
        "drop_columns": ["Unnamed: 9"],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_pay"
        ]
    },
    2004: {
        "skiprows": 4,
        "drop_columns": ["Unnamed: 9"],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_pay"
        ]
    },
    2005: {
        "skiprows": 4,
        "drop_columns": ["Unnamed: 9"],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_pay"
        ]
    },
    2006: {
        "skiprows": 4,
        "drop_columns": ["Unnamed: 9"],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_pay"
        ]
    },
    2007: {
        "skiprows": 12,
        "drop_columns": ["Unnamed: 10"],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2008: {
        "skiprows": 11,
        "drop_columns": ["Unnamed: 10"],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2009: {
        "skiprows": 12,
        "drop_columns": ["Unnamed: 10"],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2010: {
        "skiprows": 13,
        "drop_columns": ["Unnamed: 10"],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2011: {
        "skiprows": 13,
        "drop_columns": ["Unnamed: 10"],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2012: {
        "skiprows": 14,
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2013: {
        "skiprows": 14,
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2014: {
        "skiprows": 14,
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2015: {
        "skiprows": 14,
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2016: {
        "skiprows": 14,
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2017: {
        "skiprows": 14,
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2018: {
        "skiprows": 14,
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2019: {
        "skiprows": 14,
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2020: {
        "skiprows": 14,
        "drop_columns": [
            "Unnamed: 9", "Unnamed: 10", "Unnamed: 11", "Unnamed: 12",
            "Unnamed: 13", "Unnamed: 14", "Unnamed: 15", "Unnamed: 16", "Unnamed: 17"
        ],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2021: {
        "skiprows": 14,
        "drop_columns": [
            "Unnamed: 9", "Unnamed: 10", "Unnamed: 11", "Unnamed: 12",
            "Unnamed: 13", "Unnamed: 14", "Unnamed: 15", "Unnamed: 16", "Unnamed: 17"
        ],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "ft_eq_employment", "total_employment", "total_pay"
        ]
    },
    2022: {
        "skiprows": 14,
        "drop_columns": [
            "Unnamed: 9", "Unnamed: 10", "Unnamed: 11", "Unnamed: 12",
            "Unnamed: 13", "Unnamed: 14", "Unnamed: 15", "Unnamed: 16", "Unnamed: 17"
        ],
        "columns": [
            "state", "gov_function", "ft_employment", "ft_pay",
            "pt_employment", "pt_pay", "ft_eq_employment", "total_employment", "total_pay"
        ]
    }
}

EXPECTED_COLUMNS = [
    "state", "gov_function", "ft_employment", "ft_pay",
    "pt_employment", "pt_pay", "pt_hour", "ft_eq_employment", "total_employment", "total_pay"
]

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
            if "State Government Employment & Payroll" in a_tag.get_text(strip=True):
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

@asset(description="Download Excel files for a specific year.")
def get_data_for_year(context, scrape_and_export_data_urls: dict) -> dict:
    """
    Download Excel files for each year and store them locally.
    """
    downloaded_files = {}

    for year, row in scrape_and_export_data_urls.items():
        response = requests.get(row["data_url"])

        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            context.log.warning(f"Failed to fetch {row['data_url']} for year {year}: {str(e)}")
            continue

        file_extension = ".xlsx" if ".xlsx" in row["data_url"] else ".xls"
        output_file = os.path.join("data/raw", f"data_{year}{file_extension}")
        os.makedirs("data/raw", exist_ok=True)

        with open(output_file, "wb") as file:
            file.write(response.content)

        downloaded_files[year] = output_file
        context.log.info(f"Downloaded file for year {year} to {output_file}")

    context.add_output_metadata({"total_downloaded": len(downloaded_files)})

    return downloaded_files

@asset(description="Combine all years of data.")
def combine_years(context, get_data_for_year: dict):
    combined_data = pd.DataFrame(columns=EXPECTED_COLUMNS)
    bad_files = []

    for year, file_path in get_data_for_year.items():
        try:
            engine = "openpyxl" if file_path.endswith(".xlsx") else "xlrd"
            config = data_config.get(year, {})

            raw_df = pd.read_excel(file_path, engine=engine, skiprows=config.get("skiprows", 0), header=None)

            if "drop_columns" in config:
                raw_df.drop(columns=config["drop_columns"], errors="ignore", inplace=True)

            raw_df.columns = config.get("columns", raw_df.columns)

            raw_df = raw_df.reindex(columns=EXPECTED_COLUMNS)
            raw_df["year"] = year

            combined_data = pd.concat([combined_data, raw_df], ignore_index=True)

            context.log.info(f"Processed year {year} with columns: {raw_df.columns.tolist()}")

        except Exception as e:
            context.log.warning(f"Error processing file for year {year}: {str(e)}")
            bad_files.append({"year": year, "file": file_path, "reason": str(e)})

    if "state" in combined_data.columns:
        combined_data = combined_data[combined_data["state"].notnull() & (combined_data["state"] != "")]
        combined_data.sort_values(["state", "year"], inplace=True)

    if not combined_data.empty:
        output_path = os.path.join("data/out", "combined_data.json")
        os.makedirs("data/out", exist_ok=True)
        combined_data.to_json(output_path, orient="records", lines=False, indent=4)
        context.log.info(f"Combined data written to {output_path}")
        context.add_output_metadata({"output_file": output_path})

    if bad_files:
        context.log.warning(f"Encountered issues with {len(bad_files)} files.")
        context.add_output_metadata({"bad_files": bad_files})

    return combined_data
