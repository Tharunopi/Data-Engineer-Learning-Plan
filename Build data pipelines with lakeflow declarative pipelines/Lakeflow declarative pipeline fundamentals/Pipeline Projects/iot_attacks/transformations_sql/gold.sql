-- Live attack feed
CREATE OR REFRESH MATERIALIZED VIEW gold.live_attack_feed AS 
SELECT 
  label AS attack_label,
  COUNT(label) AS event_count,
  (COUNT(label) / (SELECT COUNT(*) FROM silver.question_1)) AS percentage_of_traffic
FROM silver.question_1
GROUP BY label;