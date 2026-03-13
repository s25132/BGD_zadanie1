# BGD_zadanie1

docker compose -f generate_data_docker.yml build 

docker compose -f generate_data_docker.yml up


docker compose -f pipeline_docker.yml build 

docker compose -f pipeline_docker.yml up


docker compose -f show_data.yml build 

docker compose -f show_data.yml up


SELECT count(1) FROM "raw".transactions_raw

SELECT count(1) FROM silver.transactions_clean

select * from gold.fact_transactions limit 1000


SELECT 
    schemaname,
    relname AS table_name,
    pg_total_relation_size(relid) / 1024 / 1024 / 1024 AS size_gb,
	pg_total_relation_size(relid) / 1024 / 1024 AS mb_gb
FROM pg_catalog.pg_statio_user_tables
ORDER BY size_gb DESC


select * from gold.dim_date


