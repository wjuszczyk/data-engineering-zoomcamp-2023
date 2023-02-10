"""ETL for uploading local parquet file to GCS"""
import os
from datetime import timedelta
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
#from prefect.tasks import task_input_hash


@task(name="Fetch taxi data from web", retries=3, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    df = pd.read_csv(dataset_url)
    return df

@task(name="Clean dtype issues", log_prints=True)
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues depending on name of dataset"""
    if color == "green":
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    elif color == "yellow":
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

@task(name="Write DataFrame to parquet file")
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    # if there's no directory structure, make ones
    if not os.path.isdir(f"data/{color}"):
        os.makedirs(f"data/{color}", exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(name="Upload parquet file to GCS")
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""
    # change path to posix type, requires prefect-gcp[cloud_storage]==0.2.4 (fix Windows double backslashes to slashes)
    path = Path(path).as_posix()
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=f"{path}"
    )

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df, color)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
    months: list, year: int = 2021, color: str = "yellow"
):
    """Parent flow"""
    if not months:
        months = [1, 2]
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == "__main__":
    o_color = "yellow"
    o_months = [1, 2, 3]
    o_year = 2021

    etl_parent_flow(o_months, o_year, o_color)
