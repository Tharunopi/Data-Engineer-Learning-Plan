CREATE OR REFRESH MATERIALIZED VIEW guns.silver.stress_silver
-- (CONSTRAINT stress_filter EXPECT (stress_level IS NOT NULL) ON VIOLATION FAIL UPDATE)
AS 
SELECT 
  bullying,
  AVG(depression) AS average_depression
FROM  guns.bronze.stress_level_bronze
GROUP BY bullying;