SELECT is_weekend, CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       COUNT(*) AS trip_count, COUNT(DISTINCT pickup_date) AS observed_days,
       ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT pickup_date), 4) AS trips_per_observed_day,
       SUM(total_amount) AS recorded_revenue_usd,
       ROUND(AVG(total_amount), 4) AS avg_recorded_amount_usd,
       COUNT(total_amount) AS total_observation_count,
       ROUND(AVG(trip_distance), 4) AS avg_distance_miles,
       ROUND(AVG(trip_duration_min), 4) AS avg_duration_min
FROM trips
GROUP BY is_weekend
ORDER BY is_weekend
