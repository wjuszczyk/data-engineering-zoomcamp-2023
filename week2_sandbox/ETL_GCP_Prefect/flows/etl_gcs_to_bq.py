"""ETL flow GCS to BigQuery"""
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3, name="Extract from GCS")
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    local_path = "."
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=local_path)
    return Path(f"{gcs_path}")


@task(name="Data cleaning")
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    data_frame = pd.read_parquet(path)
    print(
        f"pre: missing passenger count: {data_frame['passenger_count'].isna().sum()}")
    data_frame['passenger_count'].fillna(0, inplace=True)
    print(
        f"post: missing passenger count: {data_frame['passenger_count'].isna().sum()}")
    return data_frame


@task(name="Write to Big Query")
def write_bq(data_frame: pd.DataFrame, color: str) -> None:
    """Write DataFrame to BigQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcs-credentials")

    data_frame.to_gbq(
        destination_table=f"trips_data_all.{color}_trips",
        project_id="dtc-de-course-375723",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow(name="Main flow")
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    data_frame = transform(path)
    write_bq(data_frame, color)


if __name__ == "__main__":
    etl_gcs_to_bq()
