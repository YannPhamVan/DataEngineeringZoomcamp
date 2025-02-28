WITH valid_trips AS (
    SELECT
        'yellow' AS service_type,
        EXTRACT(YEAR FROM tpep_pickup_datetime) AS year,
        EXTRACT(MONTH FROM tpep_pickup_datetime) AS month,
        fare_amount
    FROM `noted-lead-448822-q9.zoomcamp_dbt_4.yellow_tripdata`
    WHERE fare_amount > 0
      AND trip_distance > 0
      AND payment_type IN (1, 2)  -- 1 = Credit Card, 2 = Cash

    UNION ALL

    SELECT
        'green' AS service_type,
        EXTRACT(YEAR FROM lpep_pickup_datetime) AS year,
        EXTRACT(MONTH FROM lpep_pickup_datetime) AS month,
        fare_amount
    FROM `noted-lead-448822-q9.zoomcamp_dbt_4.green_tripdata`
    WHERE fare_amount > 0
      AND trip_distance > 0
      AND payment_type IN (1, 2)
),

percentiles AS (
    SELECT
        service_type,
        year,
        month,
        APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(97)] AS p97,
        APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(95)] AS p95,
        APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(90)] AS p90
    FROM valid_trips
    GROUP BY service_type, year, month
)

-- Sélection des valeurs pour avril 2020
SELECT *
FROM percentiles
WHERE year = 2020 AND month = 4;
