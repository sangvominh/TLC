SELECT pickup_hour, COUNT(*) AS trip_count,
       SUM(total_amount) AS recorded_revenue_usd,
       SUM(fare_amount) AS metered_fare_usd,
       COUNT(total_amount) AS total_observation_count,
       COUNT(*) - COUNT(total_amount) AS missing_total_count,
       COUNT_IF(total_amount < 0) AS negative_total_count
FROM trips
GROUP BY pickup_hour
ORDER BY pickup_hour
