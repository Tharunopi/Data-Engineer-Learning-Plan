CREATE OR REFRESH STREAMING TABLE bronze.data_parts 
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
AS 
SELECT * FROM STREAM read_files(
    '/Volumes/cic_iot_dataset_2023/ops/data',
    format => 'csv',
    inferSchema => 'true',
    header => 'true'
)