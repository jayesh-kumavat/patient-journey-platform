-- Therapy switching rate
SELECT
    therapy_area,
    COUNT(DISTINCT patient_id) as patients_switched,
    ROUND(CAST(COUNT(DISTINCT patient_id) AS FLOAT) /
        NULLIF((SELECT COUNT(DISTINCT patient_id) FROM fact_prescription fp2
                WHERE fp2.therapy_area = ts.therapy_area), 0) * 100, 2) as switch_rate
FROM therapy_switches ts
GROUP BY therapy_area
ORDER BY switch_rate DESC;

-- Drop-off (patients with <= 2 rx)
SELECT therapy_area, COUNT(*) as total,
    SUM(CASE WHEN rx_count <= 2 THEN 1 ELSE 0 END) as dropoffs,
    ROUND(CAST(SUM(CASE WHEN rx_count <= 2 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) * 100, 2) as dropoff_pct
FROM (
    SELECT patient_id, therapy_area, COUNT(*) as rx_count
    FROM fact_prescription GROUP BY patient_id, therapy_area
) t
GROUP BY therapy_area;

-- Monthly physician volume
SELECT physician_id, TO_CHAR(prescription_date, 'YYYY-MM') as month,
    COUNT(*) as rx_count, AVG(quantity) as avg_qty
FROM fact_prescription
GROUP BY physician_id, TO_CHAR(prescription_date, 'YYYY-MM')
ORDER BY physician_id, month;

-- Anomalies by region
SELECT dp.region, COUNT(*) as flagged_physicians, AVG(ar.confidence) as avg_confidence
FROM anomaly_results ar
JOIN dim_physician dp ON ar.physician_id = dp.physician_id
GROUP BY dp.region;
