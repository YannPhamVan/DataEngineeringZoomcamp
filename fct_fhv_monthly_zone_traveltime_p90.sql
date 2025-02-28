WITH trip_durations AS (
    SELECT
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        PUlocationID,
        DOlocationID,
        APPROX_QUANTILES(TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND), 100)[91] AS p90_trip_duration
    FROM `noted-lead-448822-q9.zoomcamp_dbt_4.fhv_tripdata`
    WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019 AND EXTRACT(MONTH FROM pickup_datetime) = 11
    GROUP BY year, month, PUlocationID, DOlocationID
),
ranked_trips AS (
    SELECT
        t.*,
        z_pu.zone AS pickup_zone,
        z_do.zone AS dropoff_zone,
        RANK() OVER (PARTITION BY PUlocationID ORDER BY p90_trip_duration DESC) AS rank
    FROM trip_durations t
    JOIN `noted-lead-448822-q9.dbt_yphamvan.dim_zones` z_pu 
        ON t.PUlocationID = z_pu.locationid
    JOIN `noted-lead-448822-q9.dbt_yphamvan.dim_zones` z_do 
        ON t.DOlocationID = z_do.locationid
    WHERE z_pu.zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
)
SELECT pickup_zone, dropoff_zone, p90_trip_duration
FROM ranked_trips
WHERE rank = 2;
