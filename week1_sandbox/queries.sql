-- How many taxi trips were totally made on January 15?
SELECT
    count(*)
FROM
    green_taxi_data t
WHERE
    CAST (t.lpep_pickup_datetime AS DATE) = '2019-01-15'
    AND CAST (t.lpep_dropoff_datetime AS DATE) = '2019-01-15';

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

-- In 2019-01-01 how many trips had 2 and 3 passengers
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
    t.tip_amount DESC;