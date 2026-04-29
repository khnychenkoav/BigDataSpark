SELECT 'stage.mock_data_raw' AS object_name, count(*) AS rows_count FROM stage.mock_data_raw
UNION ALL SELECT 'dw.fact_sales', count(*) FROM dw.fact_sales
UNION ALL SELECT 'dw.dim_customer', count(*) FROM dw.dim_customer
UNION ALL SELECT 'dw.dim_seller', count(*) FROM dw.dim_seller
UNION ALL SELECT 'dw.dim_product', count(*) FROM dw.dim_product
UNION ALL SELECT 'dw.dim_store', count(*) FROM dw.dim_store
UNION ALL SELECT 'dw.dim_supplier', count(*) FROM dw.dim_supplier
UNION ALL SELECT 'dw.dim_country', count(*) FROM dw.dim_country
ORDER BY object_name;

SELECT
    count(*) AS fact_rows,
    count(*) FILTER (WHERE is_total_consistent) AS consistent_total_rows,
    count(*) FILTER (WHERE NOT is_total_consistent) AS inconsistent_total_rows,
    min(source_sale_total_amount) AS min_source_total,
    max(source_sale_total_amount) AS max_source_total,
    min(calculated_total_amount) AS min_calculated_total,
    max(calculated_total_amount) AS max_calculated_total
FROM dw.fact_sales;
