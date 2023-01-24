## Week 1 Homework solutions

## Question 1. Knowing docker tags

    $ docker build --help | grep 'image ID'
          --iidfile string          Write the image ID to the file

## Question 2. Understanding docker first run 

    $ docker run -it --entrypoint=bash python:3.9
    Unable to find image 'python:3.9' locally
    3.9: Pulling from library/python
    bbeef03cda1f: Already exists
    f049f75f014e: Already exists
    56261d0e6b05: Already exists
    9bd150679dbd: Already exists
    5b282ee9da04: Already exists
    03f027d5e312: Already exists
    79903339cfdb: Already exists
    efbad12427dd: Already exists
    862894708010: Already exists
    Digest: sha256:7af616b934168e213d469bff23bd8e4f07d09ccbe87e82c464cacd8e2fb244bf
    Status: Downloaded newer image for python:3.9
    root@fd4b4f3d6dff:/# pip list
    Package    Version
    ---------- -------
    pip        22.0.4
    setuptools 58.1.0
    wheel      0.38.4
    root@fd4b4f3d6dff:/#

## Question 3. Count records 

```sql
-- How many taxi trips were totally made on January 15?
SELECT
    count(*)
FROM
    green_taxi_data t
WHERE
    CAST (t.lpep_pickup_datetime AS DATE) = '2019-01-15'
    AND CAST (t.lpep_dropoff_datetime AS DATE) = '2019-01-15';
```

Output:

    +-------+
    | count |
    |-------|
    | 20530 |
    +-------+
    SELECT 1
    Time: 1.038s (1 second), executed in: 1.031s (1 second)

## Question 4. Largest trip for each day

```sql
-- Which was the day with the largest trip distance
SELECT
    MAX(t.trip_distance) AS "distance",
    CAST (t.lpep_pickup_datetime AS DATE) AS "pickup day"
FROM
    green_taxi_data t
GROUP BY
    CAST (t.lpep_pickup_datetime AS DATE)
ORDER BY
    "distance" DESC
LIMIT
    1;
```

    Output:
    +----------+------------+
    | distance | pickup day |
    |----------+------------|
    | 117.99   | 2019-01-15 |
    +----------+------------+
    SELECT 1
    Time: 2.271s (2 seconds), executed in: 2.199s (2 seconds)

## Question 5. The number of passengers

```sql
SELECT
    count(*),
    t.passenger_count
FROM
    green_taxi_data t
WHERE
    CAST (t.lpep_pickup_datetime AS DATE) = '2019-01-01'
    AND (
        t.passenger_count = 2
        OR t.passenger_count = 3
    )
GROUP BY
    t.passenger_count;
```

    Output:

    +-------+-----------------+
    | count | passenger_count |
    |-------+-----------------|
    | 1282  | 2               |
    | 254   | 3               |
    +-------+-----------------+
    SELECT 2
    Time: 0.777s

## Question 6. Largest tip

```sql
-- For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip? We want the name of the zone, not the id.
SELECT
    t.tip_amount,
    zpu."Zone" AS "pickup_loc",
    zdo."Zone" AS "dropoff_loc"
FROM
    green_taxi_data t
    JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
    JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE
    zpu."Zone" = 'Astoria'
ORDER BY
    t.tip_amount DESC
LIMIT
    1;
```

    Output:

    +------------+------------+-------------------------------+
    | tip_amount | pickup_loc | dropoff_loc                   |
    |------------+------------+-------------------------------|
    | 88.0       | Astoria    | Long Island City/Queens Plaza |
    +------------+------------+-------------------------------+
    SELECT 1
    Time: 0.788s