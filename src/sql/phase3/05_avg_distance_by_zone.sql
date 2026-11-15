SELECT PULocationID, Borough, Zone, zone_match_status, COUNT(*) AS trip_count,
       ROUND(AVG(trip_distance), 4) AS avg_distance_miles,
       SUM(trip_distance) AS total_distance_miles
FROM trips
GROUP BY PULocationID, Borough, Zone, zone_match_status
ORDER BY PULocationID
