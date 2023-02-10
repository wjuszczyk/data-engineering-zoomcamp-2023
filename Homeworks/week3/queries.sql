-- Prep
CREATE OR REPLACE TABLE
  dtc-de-course -375723.fhv.data AS
SELECT
  *
FROM
  dtc-de-course -375723.fhv.external_data;

-- Question 1.
-- Count fhv vehicle records for 2019
SELECT
  COUNT(*)
FROM
  fhv.data
WHERE
  TRUE;

-- Question 2.
-- Count distinct number of Affiliated_base_number on both tables
SELECT DISTINCT
  (Affiliated_base_number)
FROM
  fhv.data;

SELECT DISTINCT
  (Affiliated_base_number)
FROM
  fhv.external_data;

-- Question 3.
-- Count records with null on PUlocationID and DOlocationID
SELECT
  COUNT(*)
FROM
  fhv.data
WHERE
  PUlocationID IS NULL
  AND DOlocationID IS NULL;

-- Question 4 and 5
CREATE OR REPLACE TABLE
  dtc-de-course -375723.fhv.data_partitioned
PARTITION BY
  DATE(pickup_datetime)
CLUSTER BY
  Affiliated_base_number AS
SELECT
  *
FROM
  dtc-de-course -375723.fhv.data;

SELECT DISTINCT
  (Affiliated_base_number)
FROM
  dtc-de-course -375723.fhv.data_partitioned
WHERE
  DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

SELECT DISTINCT
  (Affiliated_base_number)
FROM
  dtc-de-course -375723.fhv.data
WHERE
  DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';
