# ASPEP Data Pipeline

This project implements a data pipeline using [Dagster](https://dagster.io/) to process the [Annual Survey of Public Employment & Payroll (ASPEP)](https://www.census.gov/programs-surveys/apes.html) data from the U.S. Census Bureau. The pipeline automates the extraction, transformation, and loading (ETL) of ASPEP data spanning from 2003 to 2024 and potentially future years.

## Overview

The pipeline consists of the following assets:

1. **Scrape and Export ASPEP URLs**: Scrapes the U.S. Census website to retrieve download links for ASPEP data files for each year within the specified range.

2. **Download ASPEP Year**: Downloads the Excel files for each year, skipping any files that have already been cached locally.

3. **Combine Years**: Processes and combines the data from all downloaded files into a single DataFrame, performing necessary data cleaning and transformation.

4. **Derive Stats**: Calculates additional metrics such as pay per full-time equivalent (FTE) and aggregates nationwide statistics.

5. **S3 Upload**: Uploads the processed data files to a specified Amazon S3 bucket for public access.

## Prerequisites

- **Python 3.8+**: Ensure Python is installed on your system.

- **Pipenv**: Install Pipenv for managing dependencies:

  ```bash
  pip install pipenv
  ```

## Setup

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/themarshallproject/aspep-etl.git
   cd aspep-etl
   ```

2. **Install Dependencies**:

   ```bash
   pipenv install --dev
   pipenv run pip install -e .
   ```

   Note: Dagster may require editable mode, so ensure dependencies are correctly installed.

3. **Configure Environment Variables**:

   Amazon authentication tokens and a functional bucket are required to deploy data to s3. 
   
   Create a `.env` file in the project root with the following variables:

   ```
   AWS_ACCESS_KEY_ID=your_access_key_id
   AWS_SECRET_ACCESS_KEY=your_secret_access_key
   BUCKET_NAME=s3_bucket_name
   ```

## Running the Pipeline

1. **Start the Dagster Webserver**:

   ```bash
   pipenv run dagster dev
   ```

   Access the Dagster UI at [http://localhost:3000](http://localhost:3000).

2. **Execute the Pipeline**:

   In the Dagster UI, navigate to the pipeline ([http://localhost:3000/asset-groups](http://localhost:3000/asset-groups)) and materialize the assets you want to work with. Monitor the execution and logs through the interface.

## Running Tests

Data integrity tests use Dagster's [asset checks](https://docs.dagster.io/guides/test/asset-checks). 

Run tests using `pytest`:

```bash
pipenv run pytest process_aspep_tests
```

## Customization

- **Date Range**: Modify the `START_YEAR` and `END_YEAR` constants in the script to adjust the range of years processed.

- **S3 Configuration**: Update the `BUCKET_NAME` and `S3_PREFIX` constants to change the target S3 bucket and directory.

## Error Handling

The pipeline includes logging and error handling to manage issues during execution. Failed downloads or processing steps are logged, and the pipeline generally will continue processing subsequent files.

## Contributing

This project is not actively maintained as a public project. Contributions are welcome but may not always be merged quickly. Please fork the repository and submit a pull request with your improvements or maintain a separate version. 

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

This README provides a comprehensive guide to setting up and running the ASPEP data pipeline using Dagster. For more detailed information on Dagster and its capabilities, refer to the [official documentation](https://docs.dagster.io/).

