WITH stats AS (
    SELECT payment_type, COUNT(*) AS trip_count,
           COUNT_IF(tip_amount >= 0) AS valid_tip_records,
           COUNT_IF(tip_amount > 0) AS positive_tip_records,
           SUM(CASE WHEN tip_amount >= 0 THEN tip_amount END) AS recorded_tip_usd,
           SUM(CASE WHEN tip_amount >= 0 THEN fare_amount END) AS eligible_fare_usd,
           COUNT_IF(tip_amount IS NULL) AS missing_tip_count,
           COUNT_IF(tip_amount < 0) AS negative_tip_count
    FROM trips
    GROUP BY payment_type
)
SELECT *, CASE payment_type
           WHEN 1 THEN 'Credit card' WHEN 2 THEN 'Cash'
           WHEN 3 THEN 'No charge' WHEN 4 THEN 'Dispute'
           WHEN 5 THEN 'Unknown' WHEN 6 THEN 'Voided trip'
           ELSE CONCAT('Unknown code: ', COALESCE(CAST(payment_type AS STRING), 'null'))
       END AS payment_label,
       CASE WHEN payment_type = 1
            THEN ROUND(100 * TRY_DIVIDE(recorded_tip_usd, eligible_fare_usd), 4)
       END AS tip_rate_pct,
       CASE WHEN payment_type = 1
            THEN ROUND(100.0 * TRY_DIVIDE(positive_tip_records, valid_tip_records), 4)
       END AS tipped_trip_pct,
       CASE WHEN payment_type = 1 THEN 'credit_card_tips_recorded'
            WHEN payment_type = 2 THEN 'cash_tips_not_recorded'
            ELSE 'not_comparable_to_credit_card_tips'
       END AS interpretation
FROM stats
ORDER BY payment_type NULLS LAST
