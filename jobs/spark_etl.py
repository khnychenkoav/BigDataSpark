import json
import os
import time
from datetime import date, datetime
from decimal import Decimal

from cassandra.cluster import Cluster
from clickhouse_driver import Client as ClickHouseClient
from neo4j import GraphDatabase
import psycopg2
from pymongo import MongoClient
import redis
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    coalesce,
    col,
    concat_ws,
    corr,
    count,
    date_format,
    lit,
    md5,
    monotonically_increasing_id,
    month,
    round as spark_round,
    row_number,
    sum as spark_sum,
    to_date,
    when,
    year,
)
from pyspark.sql.types import BooleanType, DecimalType, DoubleType, LongType


PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/spark_lab")
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "spark_lab")
PG_USER = os.getenv("PG_USER", "lab")
PG_PASSWORD = os.getenv("PG_PASSWORD", "lab")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_NATIVE_PORT = int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "reports")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "lab")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "lab")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "reports")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
MONGO_DB = os.getenv("MONGO_DB", "reports")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
VALKEY_HOST = os.getenv("VALKEY_HOST", "valkey")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", "6379"))


REPORT_SCHEMAS = {
    "sales_by_product": [
        ("report_row_id", "long"),
        ("product_key", "long"),
        ("product_name", "string"),
        ("product_category_name", "string"),
        ("product_brand_name", "string"),
        ("sales_count", "long"),
        ("total_units_sold", "long"),
        ("source_revenue", "double"),
        ("calculated_revenue", "double"),
        ("revenue_delta", "double"),
        ("avg_unit_price", "double"),
        ("product_rating", "double"),
        ("product_reviews", "long"),
        ("category_source_revenue", "double"),
        ("product_units_rank", "long"),
        ("is_top_10_by_units", "boolean"),
    ],
    "sales_by_customer": [
        ("report_row_id", "long"),
        ("customer_key", "long"),
        ("customer_email", "string"),
        ("customer_name", "string"),
        ("customer_country", "string"),
        ("sales_count", "long"),
        ("total_units_bought", "long"),
        ("source_revenue", "double"),
        ("calculated_revenue", "double"),
        ("avg_check", "double"),
        ("customer_revenue_rank", "long"),
        ("is_top_10_by_revenue", "boolean"),
        ("country_customer_count", "long"),
    ],
    "sales_by_time": [
        ("report_row_id", "long"),
        ("sales_year", "long"),
        ("sales_month", "long"),
        ("period_start", "string"),
        ("sales_count", "long"),
        ("total_units_sold", "long"),
        ("source_revenue", "double"),
        ("calculated_revenue", "double"),
        ("avg_order_amount", "double"),
        ("prev_month_source_revenue", "double"),
        ("source_revenue_delta", "double"),
    ],
    "sales_by_store": [
        ("report_row_id", "long"),
        ("store_key", "long"),
        ("store_name", "string"),
        ("store_city", "string"),
        ("store_country", "string"),
        ("sales_count", "long"),
        ("total_units_sold", "long"),
        ("source_revenue", "double"),
        ("calculated_revenue", "double"),
        ("avg_check", "double"),
        ("store_revenue_rank", "long"),
        ("is_top_5_by_revenue", "boolean"),
        ("city_source_revenue", "double"),
        ("country_source_revenue", "double"),
    ],
    "sales_by_supplier": [
        ("report_row_id", "long"),
        ("supplier_key", "long"),
        ("supplier_name", "string"),
        ("supplier_city", "string"),
        ("supplier_country", "string"),
        ("sales_count", "long"),
        ("total_units_sold", "long"),
        ("source_revenue", "double"),
        ("calculated_revenue", "double"),
        ("avg_product_unit_price", "double"),
        ("supplier_revenue_rank", "long"),
        ("is_top_5_by_revenue", "boolean"),
        ("country_source_revenue", "double"),
    ],
    "product_quality": [
        ("report_row_id", "long"),
        ("product_key", "long"),
        ("product_name", "string"),
        ("product_category_name", "string"),
        ("product_rating", "double"),
        ("product_reviews", "long"),
        ("total_units_sold", "long"),
        ("source_revenue", "double"),
        ("rating_sales_correlation", "double"),
        ("best_rating_rank", "long"),
        ("worst_rating_rank", "long"),
        ("reviews_rank", "long"),
        ("is_top_10_by_reviews", "boolean"),
        ("is_top_10_by_rating", "boolean"),
        ("is_bottom_10_by_rating", "boolean"),
    ],
}


def retry(action, attempts=60, sleep_seconds=5):
    last_error = None
    for _ in range(attempts):
        try:
            return action()
        except Exception as exc:
            last_error = exc
            time.sleep(sleep_seconds)
    raise last_error


def jdbc_execute(spark, statements):
    connection = psycopg2.connect(host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            for sql in statements:
                cursor.execute(sql)
    finally:
        connection.close()


def write_postgres(df, table_name):
    (
        df.write.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table_name)
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def json_value(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def collect_records(df):
    records = []
    for row in df.orderBy("report_row_id").collect():
        records.append({key: json_value(value) for key, value in row.asDict().items()})
    return records


def create_clickhouse_tables(client):
    ch_type_map = {
        "long": "Int64",
        "double": "Float64",
        "string": "String",
        "boolean": "UInt8",
    }
    client.execute(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
    for table_name, fields in REPORT_SCHEMAS.items():
        client.execute(f"DROP TABLE IF EXISTS {CLICKHOUSE_DB}.{table_name}")
        columns = ",\n".join(f"{name} {ch_type_map[field_type]}" for name, field_type in fields)
        client.execute(
            f"""
            CREATE TABLE {CLICKHOUSE_DB}.{table_name}
            (
                {columns}
            )
            ENGINE = MergeTree
            ORDER BY report_row_id
            """
        )


def write_clickhouse(report_records):
    client = retry(
        lambda: ClickHouseClient(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_NATIVE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        ),
        attempts=60,
        sleep_seconds=3,
    )
    client.execute("SELECT 1")
    create_clickhouse_tables(client)
    for table_name, records in report_records.items():
        fields = [name for name, _ in REPORT_SCHEMAS[table_name]]
        boolean_fields = {name for name, field_type in REPORT_SCHEMAS[table_name] if field_type == "boolean"}
        rows = [
            tuple((1 if record.get(name) else 0) if name in boolean_fields else record.get(name) for name in fields)
            for record in records
        ]
        if rows:
            client.execute(f"INSERT INTO {CLICKHOUSE_DB}.{table_name} ({', '.join(fields)}) VALUES", rows)


def write_cassandra(report_records):
    cql_type_map = {
        "long": "bigint",
        "double": "double",
        "string": "text",
        "boolean": "boolean",
    }
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = retry(lambda: cluster.connect(), attempts=90, sleep_seconds=5)
    try:
        session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
            """
        )
        session.set_keyspace(CASSANDRA_KEYSPACE)
        for table_name, fields in REPORT_SCHEMAS.items():
            session.execute(f"DROP TABLE IF EXISTS {table_name}")
            columns = ", ".join(f"{name} {cql_type_map[field_type]}" for name, field_type in fields)
            session.execute(f"CREATE TABLE {table_name} ({columns}, PRIMARY KEY (report_row_id))")
            placeholders = ", ".join(["?"] * len(fields))
            field_names = [name for name, _ in fields]
            prepared = session.prepare(
                f"INSERT INTO {table_name} ({', '.join(field_names)}) VALUES ({placeholders})"
            )
            for record in report_records[table_name]:
                session.execute(prepared, tuple(record.get(name) for name in field_names))
    finally:
        cluster.shutdown()


def write_mongodb(report_records):
    client = retry(lambda: MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000), attempts=60, sleep_seconds=3)
    client.admin.command("ping")
    database = client[MONGO_DB]
    for table_name, records in report_records.items():
        database.drop_collection(table_name)
        if records:
            database[table_name].insert_many([dict(record) for record in records])
            database[table_name].create_index("report_row_id", unique=True)
    client.close()


def write_neo4j(report_records):
    label_map = {
        "sales_by_product": "SalesByProductReport",
        "sales_by_customer": "SalesByCustomerReport",
        "sales_by_time": "SalesByTimeReport",
        "sales_by_store": "SalesByStoreReport",
        "sales_by_supplier": "SalesBySupplierReport",
        "product_quality": "ProductQualityReport",
    }
    driver = retry(lambda: GraphDatabase.driver(NEO4J_URI, auth=None), attempts=60, sleep_seconds=5)
    try:
        with driver.session() as session:
            session.run("RETURN 1").consume()
            for label in label_map.values():
                session.run(f"MATCH (n:{label}) DETACH DELETE n").consume()
            for table_name, records in report_records.items():
                label = label_map[table_name]
                for i in range(0, len(records), 1000):
                    batch = records[i : i + 1000]
                    session.run(
                        f"UNWIND $rows AS row CREATE (n:{label}) SET n = row",
                        rows=batch,
                    ).consume()
    finally:
        driver.close()


def write_valkey(report_records):
    client = retry(
        lambda: redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, decode_responses=True),
        attempts=60,
        sleep_seconds=3,
    )
    client.ping()
    for key in client.scan_iter("reports:*"):
        client.delete(key)
    for table_name, records in report_records.items():
        index_key = f"reports:{table_name}:index"
        for position, record in enumerate(records, start=1):
            row_key = f"reports:{table_name}:{record['report_row_id']}"
            client.hset(row_key, mapping={key: json.dumps(value, ensure_ascii=False) for key, value in record.items()})
            client.zadd(index_key, {row_key: position})
        client.hset(
            f"reports:{table_name}:meta",
            mapping={"rows_count": len(records), "updated_at": datetime.utcnow().isoformat(timespec="seconds")},
        )


def ranked_dimension(df, key_col):
    return df.withColumn(key_col, (monotonically_increasing_id() + lit(1)).cast(LongType()))


def with_top_rank(df, key_col, rank_col, flag_col, order_cols, limit_count):
    rows = [
        (row[key_col], position)
        for position, row in enumerate(df.orderBy(*order_cols).select(key_col).limit(limit_count).collect(), start=1)
    ]
    if not rows:
        return df.withColumn(rank_col, lit(0).cast(LongType())).withColumn(flag_col, lit(False))
    rank_df = df.sparkSession.createDataFrame(rows, [key_col, rank_col])
    return (
        df.join(rank_df, key_col, "left")
        .fillna({rank_col: 0})
        .withColumn(rank_col, col(rank_col).cast(LongType()))
        .withColumn(flag_col, col(rank_col) > lit(0))
    )


def build_dw(spark):
    raw = (
        spark.read.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "stage.v_mock_data_typed")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "1000")
        .load()
    )

    hash_value = lambda columns: md5(concat_ws("|", *[coalesce(col(name).cast("string"), lit("")) for name in columns]))
    typed = (
        raw.withColumn(
            "product_nk_hash",
            hash_value(
                [
                    "product_name",
                    "product_category",
                    "product_brand",
                    "product_material",
                    "product_color",
                    "product_size",
                    "product_weight",
                    "product_description",
                    "product_rating",
                    "product_reviews",
                    "product_release_date",
                    "product_expiry_date",
                ]
            ),
        )
        .withColumn(
            "pet_nk_hash",
            hash_value(["customer_pet_name", "customer_pet_type", "customer_pet_breed", "pet_category"]),
        )
        .cache()
    )

    countries = (
        typed.select(col("customer_country").alias("country_name"))
        .union(typed.select(col("seller_country").alias("country_name")))
        .union(typed.select(col("store_country").alias("country_name")))
        .union(typed.select(col("supplier_country").alias("country_name")))
        .where(col("country_name").isNotNull())
        .distinct()
    )
    dim_country = ranked_dimension(countries, "country_key").select("country_key", "country_name")

    dim_product_category = ranked_dimension(
        typed.select(col("product_category").alias("product_category_name")).distinct(),
        "product_category_key",
    )
    dim_product_brand = ranked_dimension(
        typed.select(col("product_brand").alias("product_brand_name")).distinct(),
        "product_brand_key",
    )
    dim_product_material = ranked_dimension(
        typed.select(col("product_material").alias("product_material_name")).distinct(),
        "product_material_key",
    )
    dim_product_color = ranked_dimension(
        typed.select(col("product_color").alias("product_color_name")).distinct(),
        "product_color_key",
    )
    dim_product_size = ranked_dimension(
        typed.select(col("product_size").alias("product_size_name")).distinct(),
        "product_size_key",
    )
    dim_pet_type = ranked_dimension(
        typed.select(col("customer_pet_type").alias("pet_type_name")).distinct(),
        "pet_type_key",
    )
    dim_pet_breed = ranked_dimension(
        typed.select(col("customer_pet_breed").alias("pet_breed_name")).distinct(),
        "pet_breed_key",
    )
    dim_pet_category = ranked_dimension(
        typed.select(col("pet_category").alias("pet_category_name")).distinct(),
        "pet_category_key",
    )

    first_customer = (
        typed.withColumn("rn", row_number().over(Window.partitionBy("customer_email").orderBy("raw_id")))
        .where(col("rn") == 1)
        .drop("rn")
    )
    dim_customer = (
        first_customer.join(dim_country, first_customer.customer_country == dim_country.country_name)
        .select(
            "sale_customer_id",
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "country_key",
            "customer_postal_code",
        )
        .withColumnRenamed("sale_customer_id", "source_customer_id")
        .withColumnRenamed("customer_first_name", "first_name")
        .withColumnRenamed("customer_last_name", "last_name")
        .withColumnRenamed("customer_age", "age")
        .withColumnRenamed("customer_email", "email")
        .withColumnRenamed("customer_postal_code", "postal_code")
    )
    dim_customer = ranked_dimension(dim_customer, "customer_key").select(
        "customer_key", "source_customer_id", "first_name", "last_name", "age", "email", "country_key", "postal_code"
    )

    first_seller = (
        typed.withColumn("rn", row_number().over(Window.partitionBy("seller_email").orderBy("raw_id")))
        .where(col("rn") == 1)
        .drop("rn")
    )
    dim_seller = (
        first_seller.join(dim_country, first_seller.seller_country == dim_country.country_name)
        .select(
            "sale_seller_id",
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "country_key",
            "seller_postal_code",
        )
        .withColumnRenamed("sale_seller_id", "source_seller_id")
        .withColumnRenamed("seller_first_name", "first_name")
        .withColumnRenamed("seller_last_name", "last_name")
        .withColumnRenamed("seller_email", "email")
        .withColumnRenamed("seller_postal_code", "postal_code")
    )
    dim_seller = ranked_dimension(dim_seller, "seller_key").select(
        "seller_key", "source_seller_id", "first_name", "last_name", "email", "country_key", "postal_code"
    )

    first_store = (
        typed.withColumn("rn", row_number().over(Window.partitionBy("store_email").orderBy("raw_id")))
        .where(col("rn") == 1)
        .drop("rn")
    )
    dim_store = (
        first_store.join(dim_country, first_store.store_country == dim_country.country_name)
        .select(
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "country_key",
            "store_phone",
            "store_email",
        )
        .withColumnRenamed("store_location", "store_location")
        .withColumnRenamed("store_city", "city")
        .withColumnRenamed("store_state", "state")
        .withColumnRenamed("store_phone", "phone")
        .withColumnRenamed("store_email", "email")
    )
    dim_store = ranked_dimension(dim_store, "store_key").select(
        "store_key", "store_name", "store_location", "city", "state", "country_key", "phone", "email"
    )

    first_supplier = (
        typed.withColumn("rn", row_number().over(Window.partitionBy("supplier_email").orderBy("raw_id")))
        .where(col("rn") == 1)
        .drop("rn")
    )
    dim_supplier = (
        first_supplier.join(dim_country, first_supplier.supplier_country == dim_country.country_name)
        .select(
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "country_key",
        )
        .withColumnRenamed("supplier_contact", "contact_name")
        .withColumnRenamed("supplier_email", "email")
        .withColumnRenamed("supplier_phone", "phone")
        .withColumnRenamed("supplier_address", "address")
        .withColumnRenamed("supplier_city", "city")
    )
    dim_supplier = ranked_dimension(dim_supplier, "supplier_key").select(
        "supplier_key", "supplier_name", "contact_name", "email", "phone", "address", "city", "country_key"
    )

    first_pet = (
        typed.withColumn("rn", row_number().over(Window.partitionBy("pet_nk_hash").orderBy("raw_id")))
        .where(col("rn") == 1)
        .drop("rn")
    )
    dim_pet = (
        first_pet.join(dim_pet_type, first_pet.customer_pet_type == dim_pet_type.pet_type_name)
        .join(dim_pet_breed, first_pet.customer_pet_breed == dim_pet_breed.pet_breed_name)
        .join(dim_pet_category, first_pet.pet_category == dim_pet_category.pet_category_name)
        .select("pet_nk_hash", "customer_pet_name", "pet_type_key", "pet_breed_key", "pet_category_key")
        .withColumnRenamed("customer_pet_name", "pet_name")
    )
    dim_pet = ranked_dimension(dim_pet, "pet_key").select(
        "pet_key", "pet_nk_hash", "pet_name", "pet_type_key", "pet_breed_key", "pet_category_key"
    )

    first_product = (
        typed.withColumn("rn", row_number().over(Window.partitionBy("product_nk_hash").orderBy("raw_id")))
        .where(col("rn") == 1)
        .drop("rn")
    )
    dim_product = (
        first_product.join(dim_product_category, first_product.product_category == dim_product_category.product_category_name)
        .join(dim_product_brand, first_product.product_brand == dim_product_brand.product_brand_name)
        .join(dim_product_material, first_product.product_material == dim_product_material.product_material_name)
        .join(dim_product_color, first_product.product_color == dim_product_color.product_color_name)
        .join(dim_product_size, first_product.product_size == dim_product_size.product_size_name)
        .select(
            "product_nk_hash",
            "sale_product_id",
            "product_name",
            "product_category_key",
            "product_brand_key",
            "product_material_key",
            "product_color_key",
            "product_size_key",
            "product_weight",
            "product_description",
            "product_rating",
            "product_reviews",
            "product_release_date",
            "product_expiry_date",
        )
        .withColumnRenamed("sale_product_id", "source_product_id")
    )
    dim_product = ranked_dimension(dim_product, "product_key").select(
        "product_key",
        "product_nk_hash",
        "source_product_id",
        "product_name",
        "product_category_key",
        "product_brand_key",
        "product_material_key",
        "product_color_key",
        "product_size_key",
        "product_weight",
        "product_description",
        "product_rating",
        "product_reviews",
        "product_release_date",
        "product_expiry_date",
    )

    fact_sales = (
        typed.join(dim_customer.select("customer_key", col("email").alias("customer_email")), "customer_email")
        .join(dim_seller.select("seller_key", col("email").alias("seller_email")), "seller_email")
        .join(dim_product.select("product_key", "product_nk_hash"), "product_nk_hash")
        .join(dim_store.select("store_key", col("email").alias("store_email")), "store_email")
        .join(dim_supplier.select("supplier_key", col("email").alias("supplier_email")), "supplier_email")
        .join(dim_pet.select("pet_key", "pet_nk_hash"), "pet_nk_hash")
        .select(
            col("raw_id").cast(LongType()).alias("sale_key"),
            col("raw_id").alias("source_row_id"),
            "source_file",
            "source_id",
            col("sale_customer_id").alias("source_customer_id"),
            col("sale_seller_id").alias("source_seller_id"),
            col("sale_product_id").alias("source_product_id"),
            "sale_date",
            "customer_key",
            "seller_key",
            "product_key",
            "store_key",
            "supplier_key",
            "pet_key",
            "sale_quantity",
            col("product_quantity").alias("source_product_quantity"),
            col("product_price").alias("product_unit_price"),
            col("sale_total_price").alias("source_sale_total_amount"),
            spark_round(col("product_price") * col("sale_quantity"), 2)
            .cast(DecimalType(14, 2))
            .alias("calculated_total_amount"),
        )
        .withColumn(
            "is_total_consistent",
            (spark_abs(col("source_sale_total_amount") - col("calculated_total_amount")) <= lit(Decimal("0.01"))).cast(
                BooleanType()
            ),
        )
    )

    create_dw = [
        "DROP SCHEMA IF EXISTS dw CASCADE",
        "CREATE SCHEMA dw",
        "CREATE TABLE dw.dim_country (country_key bigint PRIMARY KEY, country_name text NOT NULL UNIQUE)",
        "CREATE TABLE dw.dim_product_category (product_category_key bigint PRIMARY KEY, product_category_name text NOT NULL UNIQUE)",
        "CREATE TABLE dw.dim_product_brand (product_brand_key bigint PRIMARY KEY, product_brand_name text NOT NULL UNIQUE)",
        "CREATE TABLE dw.dim_product_material (product_material_key bigint PRIMARY KEY, product_material_name text NOT NULL UNIQUE)",
        "CREATE TABLE dw.dim_product_color (product_color_key bigint PRIMARY KEY, product_color_name text NOT NULL UNIQUE)",
        "CREATE TABLE dw.dim_product_size (product_size_key bigint PRIMARY KEY, product_size_name text NOT NULL UNIQUE)",
        "CREATE TABLE dw.dim_pet_type (pet_type_key bigint PRIMARY KEY, pet_type_name text NOT NULL UNIQUE)",
        "CREATE TABLE dw.dim_pet_breed (pet_breed_key bigint PRIMARY KEY, pet_breed_name text NOT NULL UNIQUE)",
        "CREATE TABLE dw.dim_pet_category (pet_category_key bigint PRIMARY KEY, pet_category_name text NOT NULL UNIQUE)",
        """CREATE TABLE dw.dim_customer (
            customer_key bigint PRIMARY KEY,
            source_customer_id integer NOT NULL,
            first_name text NOT NULL,
            last_name text NOT NULL,
            age integer NOT NULL,
            email text NOT NULL UNIQUE,
            country_key bigint NOT NULL REFERENCES dw.dim_country(country_key),
            postal_code text
        )""",
        """CREATE TABLE dw.dim_seller (
            seller_key bigint PRIMARY KEY,
            source_seller_id integer NOT NULL,
            first_name text NOT NULL,
            last_name text NOT NULL,
            email text NOT NULL UNIQUE,
            country_key bigint NOT NULL REFERENCES dw.dim_country(country_key),
            postal_code text
        )""",
        """CREATE TABLE dw.dim_store (
            store_key bigint PRIMARY KEY,
            store_name text NOT NULL,
            store_location text NOT NULL,
            city text NOT NULL,
            state text,
            country_key bigint NOT NULL REFERENCES dw.dim_country(country_key),
            phone text NOT NULL,
            email text NOT NULL UNIQUE
        )""",
        """CREATE TABLE dw.dim_supplier (
            supplier_key bigint PRIMARY KEY,
            supplier_name text NOT NULL,
            contact_name text NOT NULL,
            email text NOT NULL UNIQUE,
            phone text NOT NULL,
            address text NOT NULL,
            city text NOT NULL,
            country_key bigint NOT NULL REFERENCES dw.dim_country(country_key)
        )""",
        """CREATE TABLE dw.dim_pet (
            pet_key bigint PRIMARY KEY,
            pet_nk_hash char(32) NOT NULL UNIQUE,
            pet_name text NOT NULL,
            pet_type_key bigint NOT NULL REFERENCES dw.dim_pet_type(pet_type_key),
            pet_breed_key bigint NOT NULL REFERENCES dw.dim_pet_breed(pet_breed_key),
            pet_category_key bigint NOT NULL REFERENCES dw.dim_pet_category(pet_category_key)
        )""",
        """CREATE TABLE dw.dim_product (
            product_key bigint PRIMARY KEY,
            product_nk_hash char(32) NOT NULL UNIQUE,
            source_product_id integer NOT NULL,
            product_name text NOT NULL,
            product_category_key bigint NOT NULL REFERENCES dw.dim_product_category(product_category_key),
            product_brand_key bigint NOT NULL REFERENCES dw.dim_product_brand(product_brand_key),
            product_material_key bigint NOT NULL REFERENCES dw.dim_product_material(product_material_key),
            product_color_key bigint NOT NULL REFERENCES dw.dim_product_color(product_color_key),
            product_size_key bigint NOT NULL REFERENCES dw.dim_product_size(product_size_key),
            product_weight numeric(10, 2) NOT NULL,
            product_description text NOT NULL,
            product_rating numeric(3, 1) NOT NULL,
            product_reviews integer NOT NULL,
            product_release_date date NOT NULL,
            product_expiry_date date NOT NULL
        )""",
        """CREATE TABLE dw.fact_sales (
            sale_key bigint PRIMARY KEY,
            source_row_id bigint NOT NULL UNIQUE,
            source_file text NOT NULL,
            source_id integer NOT NULL,
            source_customer_id integer NOT NULL,
            source_seller_id integer NOT NULL,
            source_product_id integer NOT NULL,
            sale_date date NOT NULL,
            customer_key bigint NOT NULL REFERENCES dw.dim_customer(customer_key),
            seller_key bigint NOT NULL REFERENCES dw.dim_seller(seller_key),
            product_key bigint NOT NULL REFERENCES dw.dim_product(product_key),
            store_key bigint NOT NULL REFERENCES dw.dim_store(store_key),
            supplier_key bigint NOT NULL REFERENCES dw.dim_supplier(supplier_key),
            pet_key bigint NOT NULL REFERENCES dw.dim_pet(pet_key),
            sale_quantity integer NOT NULL,
            source_product_quantity integer NOT NULL,
            product_unit_price numeric(12, 2) NOT NULL,
            source_sale_total_amount numeric(14, 2) NOT NULL,
            calculated_total_amount numeric(14, 2) NOT NULL,
            is_total_consistent boolean NOT NULL
        )""",
    ]
    jdbc_execute(spark, create_dw)

    tables = [
        ("dw.dim_country", dim_country),
        ("dw.dim_product_category", dim_product_category),
        ("dw.dim_product_brand", dim_product_brand),
        ("dw.dim_product_material", dim_product_material),
        ("dw.dim_product_color", dim_product_color),
        ("dw.dim_product_size", dim_product_size),
        ("dw.dim_pet_type", dim_pet_type),
        ("dw.dim_pet_breed", dim_pet_breed),
        ("dw.dim_pet_category", dim_pet_category),
        ("dw.dim_customer", dim_customer),
        ("dw.dim_seller", dim_seller),
        ("dw.dim_store", dim_store),
        ("dw.dim_supplier", dim_supplier),
        ("dw.dim_pet", dim_pet),
        ("dw.dim_product", dim_product),
        ("dw.fact_sales", fact_sales),
    ]
    for table_name, df in tables:
        write_postgres(df, table_name)

    return {
        "dim_country": dim_country.cache(),
        "dim_product_category": dim_product_category.cache(),
        "dim_product_brand": dim_product_brand.cache(),
        "dim_customer": dim_customer.cache(),
        "dim_seller": dim_seller.cache(),
        "dim_store": dim_store.cache(),
        "dim_supplier": dim_supplier.cache(),
        "dim_product": dim_product.cache(),
        "fact_sales": fact_sales.cache(),
    }


def build_reports(dw):
    fact_sales = dw["fact_sales"]
    dim_product = dw["dim_product"]
    dim_product_category = dw["dim_product_category"]
    dim_product_brand = dw["dim_product_brand"]
    dim_customer = dw["dim_customer"]
    dim_store = dw["dim_store"]
    dim_supplier = dw["dim_supplier"]
    dim_country = dw["dim_country"]
    product_star = (
        fact_sales.join(dim_product, "product_key")
        .join(dim_product_category, "product_category_key")
        .join(dim_product_brand, "product_brand_key")
    )
    product_report = (
        product_star.groupBy(
            "product_key",
            "product_name",
            "product_category_name",
            "product_brand_name",
            "product_rating",
            "product_reviews",
        )
        .agg(
            count("*").cast(LongType()).alias("sales_count"),
            spark_sum("sale_quantity").cast(LongType()).alias("total_units_sold"),
            spark_round(spark_sum("source_sale_total_amount"), 2).cast(DoubleType()).alias("source_revenue"),
            spark_round(spark_sum("calculated_total_amount"), 2).cast(DoubleType()).alias("calculated_revenue"),
            spark_round(avg("product_unit_price"), 2).cast(DoubleType()).alias("avg_unit_price"),
        )
        .withColumn("revenue_delta", spark_round(col("calculated_revenue") - col("source_revenue"), 2))
        .withColumn(
            "category_source_revenue",
            spark_round(spark_sum("source_revenue").over(Window.partitionBy("product_category_name")), 2),
        )
    )
    product_report = (
        with_top_rank(
            product_report,
            "product_key",
            "product_units_rank",
            "is_top_10_by_units",
            [col("total_units_sold").desc(), col("product_key")],
            10,
        )
        .withColumn("report_row_id", col("product_key"))
        .select(*[name for name, _ in REPORT_SCHEMAS["sales_by_product"]])
    )

    customer_country = dim_country.select(col("country_key"), col("country_name").alias("customer_country"))
    customer_report = (
        fact_sales.join(dim_customer, "customer_key")
        .join(customer_country, "country_key")
        .groupBy("customer_key", "email", "first_name", "last_name", "customer_country")
        .agg(
            count("*").cast(LongType()).alias("sales_count"),
            spark_sum("sale_quantity").cast(LongType()).alias("total_units_bought"),
            spark_round(spark_sum("source_sale_total_amount"), 2).cast(DoubleType()).alias("source_revenue"),
            spark_round(spark_sum("calculated_total_amount"), 2).cast(DoubleType()).alias("calculated_revenue"),
            spark_round(avg("source_sale_total_amount"), 2).cast(DoubleType()).alias("avg_check"),
        )
        .withColumn("customer_name", concat_ws(" ", col("first_name"), col("last_name")))
        .withColumn(
            "country_customer_count", count("*").over(Window.partitionBy("customer_country")).cast(LongType())
        )
    )
    customer_report = (
        with_top_rank(
            customer_report,
            "customer_key",
            "customer_revenue_rank",
            "is_top_10_by_revenue",
            [col("source_revenue").desc(), col("customer_key")],
            10,
        )
        .withColumn("report_row_id", col("customer_key"))
        .withColumnRenamed("email", "customer_email")
        .select(*[name for name, _ in REPORT_SCHEMAS["sales_by_customer"]])
    )

    time_base = (
        fact_sales.groupBy(year("sale_date").alias("sales_year"), month("sale_date").alias("sales_month"))
        .agg(
            count("*").cast(LongType()).alias("sales_count"),
            spark_sum("sale_quantity").cast(LongType()).alias("total_units_sold"),
            spark_round(spark_sum("source_sale_total_amount"), 2).cast(DoubleType()).alias("source_revenue"),
            spark_round(spark_sum("calculated_total_amount"), 2).cast(DoubleType()).alias("calculated_revenue"),
            spark_round(avg("source_sale_total_amount"), 2).cast(DoubleType()).alias("avg_order_amount"),
        )
        .withColumn("period_index", col("sales_year") * lit(12) + col("sales_month"))
        .withColumn("report_row_id", col("sales_year") * lit(100) + col("sales_month"))
        .withColumn(
            "period_start",
            date_format(to_date(concat_ws("-", col("sales_year"), col("sales_month"), lit(1))), "yyyy-MM-dd"),
        )
    )
    previous_month = time_base.select(
        (col("period_index") + lit(1)).alias("period_index"),
        col("source_revenue").alias("prev_month_source_revenue"),
    )
    time_report = (
        time_base.join(previous_month, "period_index", "left")
        .fillna({"prev_month_source_revenue": 0.0})
        .withColumn("source_revenue_delta", spark_round(col("source_revenue") - col("prev_month_source_revenue"), 2))
        .select(*[name for name, _ in REPORT_SCHEMAS["sales_by_time"]])
    )

    store_country = dim_country.select(col("country_key"), col("country_name").alias("store_country"))
    store_report = (
        fact_sales.join(dim_store, "store_key")
        .join(store_country, "country_key")
        .groupBy("store_key", "store_name", col("city").alias("store_city"), "store_country")
        .agg(
            count("*").cast(LongType()).alias("sales_count"),
            spark_sum("sale_quantity").cast(LongType()).alias("total_units_sold"),
            spark_round(spark_sum("source_sale_total_amount"), 2).cast(DoubleType()).alias("source_revenue"),
            spark_round(spark_sum("calculated_total_amount"), 2).cast(DoubleType()).alias("calculated_revenue"),
            spark_round(avg("source_sale_total_amount"), 2).cast(DoubleType()).alias("avg_check"),
        )
        .withColumn("city_source_revenue", spark_round(spark_sum("source_revenue").over(Window.partitionBy("store_city")), 2))
        .withColumn(
            "country_source_revenue",
            spark_round(spark_sum("source_revenue").over(Window.partitionBy("store_country")), 2),
        )
    )
    store_report = (
        with_top_rank(
            store_report,
            "store_key",
            "store_revenue_rank",
            "is_top_5_by_revenue",
            [col("source_revenue").desc(), col("store_key")],
            5,
        )
        .withColumn("report_row_id", col("store_key"))
        .select(*[name for name, _ in REPORT_SCHEMAS["sales_by_store"]])
    )

    supplier_country = dim_country.select(col("country_key"), col("country_name").alias("supplier_country"))
    supplier_report = (
        fact_sales.join(dim_supplier, "supplier_key")
        .join(supplier_country, "country_key")
        .groupBy("supplier_key", "supplier_name", col("city").alias("supplier_city"), "supplier_country")
        .agg(
            count("*").cast(LongType()).alias("sales_count"),
            spark_sum("sale_quantity").cast(LongType()).alias("total_units_sold"),
            spark_round(spark_sum("source_sale_total_amount"), 2).cast(DoubleType()).alias("source_revenue"),
            spark_round(spark_sum("calculated_total_amount"), 2).cast(DoubleType()).alias("calculated_revenue"),
            spark_round(avg("product_unit_price"), 2).cast(DoubleType()).alias("avg_product_unit_price"),
        )
        .withColumn(
            "country_source_revenue",
            spark_round(spark_sum("source_revenue").over(Window.partitionBy("supplier_country")), 2),
        )
    )
    supplier_report = (
        with_top_rank(
            supplier_report,
            "supplier_key",
            "supplier_revenue_rank",
            "is_top_5_by_revenue",
            [col("source_revenue").desc(), col("supplier_key")],
            5,
        )
        .withColumn("report_row_id", col("supplier_key"))
        .select(*[name for name, _ in REPORT_SCHEMAS["sales_by_supplier"]])
    )

    quality_base = (
        product_star.groupBy("product_key", "product_name", "product_category_name", "product_rating", "product_reviews")
        .agg(
            spark_sum("sale_quantity").cast(LongType()).alias("total_units_sold"),
            spark_round(spark_sum("source_sale_total_amount"), 2).cast(DoubleType()).alias("source_revenue"),
        )
        .cache()
    )
    correlation_value = quality_base.select(corr("product_rating", "total_units_sold")).first()[0]
    if correlation_value is None:
        correlation_value = 0.0
    quality_report = quality_base.withColumn("rating_sales_correlation", lit(float(correlation_value)))
    quality_report = with_top_rank(
        quality_report,
        "product_key",
        "best_rating_rank",
        "is_top_10_by_rating",
        [col("product_rating").desc(), col("product_key")],
        10,
    )
    quality_report = with_top_rank(
        quality_report,
        "product_key",
        "worst_rating_rank",
        "is_bottom_10_by_rating",
        [col("product_rating").asc(), col("product_key")],
        10,
    )
    quality_report = (
        with_top_rank(
            quality_report,
            "product_key",
            "reviews_rank",
            "is_top_10_by_reviews",
            [col("product_reviews").desc(), col("product_key")],
            10,
        )
        .withColumn("report_row_id", col("product_key"))
        .select(*[name for name, _ in REPORT_SCHEMAS["product_quality"]])
    )

    return {
        "sales_by_product": product_report.cache(),
        "sales_by_customer": customer_report.cache(),
        "sales_by_time": time_report.cache(),
        "sales_by_store": store_report.cache(),
        "sales_by_supplier": supplier_report.cache(),
        "product_quality": quality_report.cache(),
    }


def main():
    spark = (
        SparkSession.builder.appName("BigDataSparkLab")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .config("spark.default.parallelism", os.getenv("SPARK_DEFAULT_PARALLELISM", "4"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.executor.memory", os.getenv("SPARK_EXECUTOR_MEMORY", "1g"))
        .config("spark.driver.maxResultSize", os.getenv("SPARK_DRIVER_MAX_RESULT_SIZE", "256m"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    dw = build_dw(spark)
    reports = build_reports(dw)
    report_records = {table_name: collect_records(df) for table_name, df in reports.items()}

    write_clickhouse(report_records)
    write_cassandra(report_records)
    write_mongodb(report_records)
    write_neo4j(report_records)
    write_valkey(report_records)

    for table_name, records in report_records.items():
        print(f"{table_name}: {len(records)} rows")

    spark.stop()


if __name__ == "__main__":
    main()
