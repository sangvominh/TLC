WITH counts AS (
    SELECT PULocationID, Borough, Zone, zone_match_status, COUNT(*) AS trip_count
    FROM trips
    GROUP BY PULocationID, Borough, Zone, zone_match_status
)
SELECT *, ROUND(100.0 * trip_count / SUM(trip_count) OVER (), 4) AS trip_share_pct
FROM counts
ORDER BY trip_count DESC, PULocationID ASC
LIMIT 10
