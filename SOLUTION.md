# Решение лабораторной работы №2

## Состав решения

Реализован полный Spark ETL-контур:

- PostgreSQL: загрузка 10 CSV в staging и хранение итоговой модели звезда/снежинка.
- Apache Spark: чтение staging из PostgreSQL, построение измерений и факта, расчет отчетов.
- ClickHouse: 6 обязательных отчетных таблиц.
- Cassandra: те же 6 отчетов отдельными CQL-таблицами.
- MongoDB: те же 6 отчетов отдельными коллекциями.
- Neo4j: те же 6 отчетов отдельными наборами узлов.
- Valkey: те же 6 отчетов в hash-записях с индексами и meta-ключами.

## Запуск

```powershell
.\scripts\run_etl.ps1
```

Полная пересборка с нуля:

```powershell
docker compose down -v
.\scripts\run_etl.ps1
```

## Подключения

PostgreSQL:

- Host: `localhost`
- Port: `5433`
- Database: `spark_lab`
- User: `lab`
- Password: `lab`

ClickHouse:

- HTTP port: `8124`
- Native port: `9001`
- Database: `reports`
- User: `lab`
- Password: `lab`

Cassandra:

- Host: `localhost`
- Port: `9043`
- Keyspace: `reports`

MongoDB:

- URI: `mongodb://localhost:27018`
- Database: `reports`

Neo4j:

- Browser: `http://localhost:7475`
- Bolt: `bolt://localhost:7688`
- Auth: disabled

Valkey:

- Host: `localhost`
- Port: `6380`

## Spark job

Основной код находится в `jobs/spark_etl.py`.

Spark выполняет:

1. Чтение `stage.v_mock_data_typed` из PostgreSQL через JDBC.
2. Построение `dw.fact_sales` и измерений `dw.dim_*` в PostgreSQL.
3. Расчет отчетов:
   - `sales_by_product`
   - `sales_by_customer`
   - `sales_by_time`
   - `sales_by_store`
   - `sales_by_supplier`
   - `product_quality`
4. Загрузку этих отчетов в ClickHouse, Cassandra, MongoDB, Neo4j и Valkey.

## Модель PostgreSQL

Зерно факта: одна строка исходного CSV, то есть одна продажа. Исходные поля `id`, `sale_customer_id`, `sale_seller_id`, `sale_product_id` не используются как глобальные ключи, потому что повторяются в каждом файле. Они сохраняются в факте как source-поля для трассировки.

Факт:

- `dw.fact_sales`

Измерения:

- `dw.dim_customer`
- `dw.dim_seller`
- `dw.dim_store`
- `dw.dim_supplier`
- `dw.dim_product`
- `dw.dim_date`
- `dw.dim_country`
- `dw.dim_product_category`
- `dw.dim_product_brand`
- `dw.dim_product_material`
- `dw.dim_product_color`
- `dw.dim_product_size`
- `dw.dim_pet`
- `dw.dim_pet_type`
- `dw.dim_pet_breed`
- `dw.dim_pet_category`

## Проверка

```powershell
.\scripts\validate.ps1
```

Ожидаемые ключевые результаты:

- `stage.mock_data_raw`: 10000 строк.
- `dw.fact_sales`: 10000 строк.
- `sales_by_product`: 10000 строк в каждой NoSQL БД.
- `sales_by_customer`: 10000 строк в каждой NoSQL БД.
- `sales_by_time`: 12 строк в каждой NoSQL БД.
- `sales_by_store`: 10000 строк в каждой NoSQL БД.
- `sales_by_supplier`: 10000 строк в каждой NoSQL БД.
- `product_quality`: 10000 строк в каждой NoSQL БД.

В данных сохраняется выявленная проблема качества: исходный `sale_total_price` не совпадает с `product_price * sale_quantity`. Поэтому в факте и отчетах есть две суммы: исходная и рассчитанная.
