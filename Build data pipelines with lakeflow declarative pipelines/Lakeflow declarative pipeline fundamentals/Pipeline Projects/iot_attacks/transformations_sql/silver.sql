-- Live attack feed
CREATE OR REFRESH STREAMING TABLE silver.question_1 AS 
SELECT 
    label
FROM STREAM bronze.data_parts;