"""ETL web to GCS"""
import os
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
#from random import randint


@task(retries=3, log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    data_frame = pd.read_csv(dataset_url)
    print(f"1. Retireved {dataset_url}")
    return data_frame


@task(log_prints=True)
def clean(data_frame: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues depending on name of dataset"""
    if color == "green":
        data_frame['lpep_pickup_datetime'] = pd.to_datetime(
            data_frame['lpep_pickup_datetime'])
        data_frame['lpep_dropoff_datetime'] = pd.to_datetime(
            data_frame['lpep_dropoff_datetime'])
        data_frame["trip_type"] = data_frame["trip_type"].astype('Int64')
        data_frame["VendorID"] = data_frame["VendorID"].astype('Int64')
        data_frame["RatecodeID"] = data_frame["RatecodeID"].astype('Int64')
        data_frame["PULocationID"] = data_frame["PULocationID"].astype('Int64')
        data_frame["DOLocationID"] = data_frame["DOLocationID"].astype('Int64')
        data_frame["passenger_count"] = data_frame["passenger_count"].astype(
            'Int64')
        data_frame["payment_type"] = data_frame["payment_type"].astype('Int64')

    if color == "yellow":
        data_frame['tpep_pickup_datetime'] = pd.to_datetime(
            data_frame['tpep_pickup_datetime'])
        data_frame['tpep_dropoff_datetime'] = pd.to_datetime(
            data_frame['tpep_dropoff_datetime'])
        data_frame["VendorID"] = data_frame["VendorID"].astype('Int64')
        data_frame["RatecodeID"] = data_frame["RatecodeID"].astype('Int64')
        data_frame["PULocationID"] = data_frame["PULocationID"].astype('Int64')
        data_frame["DOLocationID"] = data_frame["DOLocationID"].astype('Int64')
        data_frame["passenger_count"] = data_frame["passenger_count"].astype(
            'Int64')
        data_frame["payment_type"] = data_frame["payment_type"].astype('Int64')
    # print(data_frame.head(2))
    # print(f"columns: {data_frame.dtypes}")
    # print(f"rows: {len(data_frame)}")
    print("2. Cleaned data set")
    return data_frame


@task(log_prints=True)
def write_local(data_frame: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    # if there's no directory structure, make ones
    if not os.path.isdir(f"../data/{color}"):
        os.makedirs(f"../data/{color}", exist_ok=True)
    path = Path(f"../data/{color}/{dataset_file}.parquet")
    data_frame.to_parquet(path, compression="gzip")
    print(f"3. Written ../data/{color}/{dataset_file}.parquet to disk")
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> Path:
    """Uploading local parquet file to GCS"""
    # change path to posix type, requires prefect-gcp[cloud_storage]==0.2.4
    # (fix Windows double backslashes to slashes)
    path = Path(path).as_posix()
    # remove trailing "../"
    to_path = path[3:]
    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=f"{to_path}"
    )
    print(f"4. Written {path} to GCS")
    return to_path


@flow(log_prints=True)
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    colors = ["green", "yellow"]
    years = [2019, 2020]
    months = [*range(1, 13)]
    for color in colors:
        for year in years:
            for month in months:
                dataset_file = f"{color}_tripdata_{year}-{month:02}"
                dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
                data_frame = fetch(dataset_url)
                df_clean = clean(data_frame, color)
                path = write_local(df_clean, color, dataset_file)
                to_path = write_gcs(path)
                print(
                    f"MAIN FLOW: Written {color}_tripdata_{year}-{month:02}.parquet file to GCS Bucket (path: {to_path})")


if __name__ == "__main__":
    etl_web_to_gcs()
