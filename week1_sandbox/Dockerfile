FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

WORKDIR /app
COPY ingest_data_parquet_and_csv.py ingest_data_parquet_and_csv.py

ENTRYPOINT [ "python" , "ingest_data_parquet_and_csv.py" ]