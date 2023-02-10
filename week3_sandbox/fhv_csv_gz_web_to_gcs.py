"""ETL web to GCS"""
import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
#from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    data_frame = pd.read_csv(dataset_url)
    return data_frame


# @task(log_prints=True)
# def clean(data_frame: pd.DataFrame, color: str) -> pd.DataFrame:
#     """Fix dtype issues depending on name of dataset"""
#     # if color == "green":
#     #     data_frame['lpep_pickup_datetime'] = pd.to_datetime(
#     #         data_frame['lpep_pickup_datetime'])
#     #     data_frame['lpep_dropoff_datetime'] = pd.to_datetime(
#     #         data_frame['lpep_dropoff_datetime'])
#     # elif color == "yellow":
#     #     data_frame['tpep_pickup_datetime'] = pd.to_datetime(
#     #         data_frame['tpep_pickup_datetime'])
#     #     data_frame['tpep_dropoff_datetime'] = pd.to_datetime(
#     #         data_frame['tpep_dropoff_datetime'])
#     # print(data_frame.head(2))
#     # print(f"columns: {data_frame.dtypes}")
#     # print(f"rows: {len(data_frame)}")
#     return data_frame


@task()
def write_local(data_frame: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    # if there's no directory structure, make ones
    if not os.path.isdir(f"data/{color}"):
        os.makedirs(f"data/{color}", exist_ok=True)
    #path = Path(f"data/{color}/{dataset_file}.parquet")
    path = Path(f"data/{color}/{dataset_file}.csv.gz")
    #data_frame.to_parquet(path, compression="gzip")
    data_frame.to_csv(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""
    # change path to posix type, requires prefect-gcp[cloud_storage]==0.2.4
    # (fix Windows double backslashes to slashes)
    path = Path(path).as_posix()
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=f"{path}"
    )


@flow(log_prints=True, retries=3)
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "fhv"
    year = 2019
    # color = "yellow"
    # year = 2021
    # month = 3
    months = [*range(1,13)]
    for month in months:
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
        data_frame = fetch(dataset_url)
        # df_clean = clean(data_frame, color)
        path = write_local(data_frame, color, dataset_file)
        print(f"Written locally file {dataset_file}.csv.gz")
        write_gcs(path)
        print(f"Written locally file {dataset_file}.csv.gz to GCS Bucket")

if __name__ == "__main__":
    etl_web_to_gcs()
