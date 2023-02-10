"""ETL load GCS data to BigQuery"""
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3, name="Extract from GCS")
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=".")
    return Path(f"{gcs_path}")

@task(name="Data cleaning")
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example - nothing to transform this time"""
    df = pd.read_parquet(path)
    # print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df['passenger_count'].fillna(0, inplace=True)
    # print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(name="Write to Big Query")
def write_bq(df: pd.DataFrame, color: str) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcs-credentials")

    df.to_gbq(
        destination_table = f"trips_data_all.{color}_trips",
        project_id = "dtc-de-course-375723",
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        chunksize = 500_000,
        if_exists="append"
    )


@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int, color: str) -> int:
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    print(f"Rows processed: {len(df)}")
    write_bq(df, color)
    return len(df)

@flow(log_prints=True)
def etl_parent_flow(
    months: list, year: int = 2019, color: str = "yellow"
):
    """ETL parent flow"""
    if not months:
        months = [2, 3]
    rows_processed = 0
    for month in months:
        rows_processed += etl_gcs_to_bq(year, month, color)
    print(f"Total rows processed: {rows_processed}")

if __name__ == "__main__":
    outer_months = [2, 3]
    outer_year = 2019
    outer_color = "yellow"

    etl_parent_flow(outer_months, outer_year, outer_color)
