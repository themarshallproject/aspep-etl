import csv
import requests
from dagster import asset, MetadataValue
from bs4 import BeautifulSoup


def _get_census_url(year, context):
    """
    Get a census data URL
    """
    base_url = "https://www.census.gov/programs-surveys/apes/data/datasetstables/"
    if year == 2017 or year == 2018:
        url = f"https://www.census.gov/data/tables/{year}/econ/apes/annual-apes.html"
        context.log.info(f"URL: {url} (special case for 2017 or 2018)")
    else:
        url = f"{base_url}{year}.html"
        context.log.info(f"URL: {url}")

    return url 

@asset
def scrape_data(context) -> dict:
    """
    Scrape the Census website to find links for State and Local Government Employment Data
    for years 2000-2023.
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
            # Ensure the URL is absolute
            if not full_url.startswith("http"):
                full_url = f"https://www.census.gov{full_url}"
            year_url_mapping[year] = full_url
        else:
            context.log.warning(f"No matching link found for year {year}")

    context.log.info(f"Scraped data for {len(year_url_mapping)} years.")
    context.add_output_metadata({"total_years": len(year_url_mapping)})
    return year_url_mapping


@asset(required_resource_keys={"output_paths"})
def export_to_csv(context, scrape_data: dict):
    """
    Export the year-to-URL mapping to a CSV file.
    """
    # Get the CSV output path from the resource
    output_file = context.resources.output_paths["csv_output"]

    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Year", "URL"])
        for year, url in scrape_data.items():
            writer.writerow([year, url])

    context.log.info(f"CSV exported to {output_file}")
    context.add_output_metadata({
        "output_file": MetadataValue.path(output_file),
        "total_records": len(scrape_data),
    })