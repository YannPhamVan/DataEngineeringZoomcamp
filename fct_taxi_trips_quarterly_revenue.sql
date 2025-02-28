WITH quarterly_revenue AS (
    -- Étape 1 : Calculer les revenus trimestriels pour chaque année et taxi type
    SELECT
        'yellow' AS service_type,
        EXTRACT(YEAR FROM tpep_pickup_datetime) AS year,
        EXTRACT(QUARTER FROM tpep_pickup_datetime) AS quarter,
        CONCAT(EXTRACT(YEAR FROM tpep_pickup_datetime), '/Q', EXTRACT(QUARTER FROM tpep_pickup_datetime)) AS year_quarter,
        SUM(total_amount) AS revenue
    FROM `noted-lead-448822-q9.zoomcamp_dbt_4.yellow_tripdata`
    WHERE total_amount > 0  -- On exclut les valeurs invalides
    GROUP BY 1, 2, 3, 4

    UNION ALL

    SELECT
        'green' AS service_type,
        EXTRACT(YEAR FROM lpep_pickup_datetime) AS year,
        EXTRACT(QUARTER FROM lpep_pickup_datetime) AS quarter,
        CONCAT(EXTRACT(YEAR FROM lpep_pickup_datetime), '/Q', EXTRACT(QUARTER FROM lpep_pickup_datetime)) AS year_quarter,
        SUM(total_amount) AS revenue
    FROM `noted-lead-448822-q9.zoomcamp_dbt_4.green_tripdata`
    WHERE total_amount > 0
    GROUP BY 1, 2, 3, 4
),

revenue_with_growth AS (
    -- Étape 2 : Calculer la croissance YoY (comparaison avec l’année précédente)
    SELECT 
        q1.service_type,
        q1.year,
        q1.quarter,
        q1.year_quarter,
        q1.revenue AS current_revenue,
        q2.revenue AS previous_revenue,
        ROUND((q1.revenue - q2.revenue) / q2.revenue * 100, 2) AS yoy_growth
    FROM quarterly_revenue q1
    LEFT JOIN quarterly_revenue q2
        ON q1.service_type = q2.service_type
        AND q1.year = q2.year + 1
        AND q1.quarter = q2.quarter
)

-- Étape 3 : Sélection finale des résultats
SELECT *
FROM revenue_with_growth
ORDER BY service_type, year, quarter;
