MATCH (n:SalesByProductReport) RETURN 'sales_by_product' AS report_name, count(n) AS rows_count
UNION ALL
MATCH (n:SalesByCustomerReport) RETURN 'sales_by_customer' AS report_name, count(n) AS rows_count
UNION ALL
MATCH (n:SalesByTimeReport) RETURN 'sales_by_time' AS report_name, count(n) AS rows_count
UNION ALL
MATCH (n:SalesByStoreReport) RETURN 'sales_by_store' AS report_name, count(n) AS rows_count
UNION ALL
MATCH (n:SalesBySupplierReport) RETURN 'sales_by_supplier' AS report_name, count(n) AS rows_count
UNION ALL
MATCH (n:ProductQualityReport) RETURN 'product_quality' AS report_name, count(n) AS rows_count;
