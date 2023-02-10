"""Parametrized ETL flow from web to GCS"""
import os
from pathlib import Path
from datetime import timedelta
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
#from prefect.tasks import task_input_hash

import pandas as pd


@task(name="Fetch taxi data from web", retries=3, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    data_frame = pd.read_csv(dataset_url)
    return data_frame


@task(name="Clean dtype issues", log_prints=True)
def clean(data_frame: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues depending on name of dataset"""
    if color == "green":
        data_frame['lpep_pickup_datetime'] = pd.to_datetime(
            data_frame['lpep_pickup_datetime'])
        data_frame['lpep_dropoff_datetime'] = pd.to_datetime(
            data_frame['lpep_dropoff_datetime'])
    elif color == "yellow":
        data_frame['tpep_pickup_datetime'] = pd.to_datetime(
            data_frame['tpep_pickup_datetime'])
        data_frame['tpep_dropoff_datetime'] = pd.to_datetime(
            data_frame['tpep_dropoff_datetime'])
    print(data_frame.head(2))
    print(f"columns: {data_frame.dtypes}")
    print(f"rows: {len(data_frame)}")
    return data_frame


@task(name="Write DataFrame to parquet file")
def write_local(data_frame: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    # if there's no directory structure, make ones
    if not os.path.isdir(f"../data/{color}"):
        os.makedirs(f"../data/{color}", exist_ok=True)
    path = Path(f"../data/{color}/{dataset_file}.parquet")
    data_frame.to_parquet(path, compression="gzip")
    return path


@task(name="Upload parquet file to GCS")
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""
    # change path to posix type, requires prefect-gcp[cloud_storage]==0.2.4
    # (fix Windows double backslashes to slashes)
    path = Path(path).as_posix()
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=f"{path[3:]}"
    )


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data\
                    /releases/download/{color}/{dataset_file}.csv.gz"

    data_frame = fetch(dataset_url)
    df_clean = clean(data_frame, color)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int], year: int = 2021, color: str = "yellow"
):
    """Parent flow"""
    if not months:
        months = [1, 2]
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    O_COLOR = "yellow"
    O_MONTHS = [1, 2, 3]
    O_YEAR = 2021

    etl_parent_flow(O_MONTHS, O_YEAR, O_COLOR)
