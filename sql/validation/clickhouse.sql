SELECT
    table_name,
    rows_count
FROM (
    SELECT 'sales_by_product' AS table_name, count() AS rows_count FROM reports.sales_by_product
    UNION ALL SELECT 'sales_by_customer', count() FROM reports.sales_by_customer
    UNION ALL SELECT 'sales_by_time', count() FROM reports.sales_by_time
    UNION ALL SELECT 'sales_by_store', count() FROM reports.sales_by_store
    UNION ALL SELECT 'sales_by_supplier', count() FROM reports.sales_by_supplier
    UNION ALL SELECT 'product_quality', count() FROM reports.product_quality
)
ORDER BY table_name;

SELECT product_name, total_units_sold, source_revenue
FROM reports.sales_by_product
WHERE is_top_10_by_units = 1
ORDER BY product_units_rank
LIMIT 10;
