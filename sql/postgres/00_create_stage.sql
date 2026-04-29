CREATE SCHEMA IF NOT EXISTS stage;

DROP TABLE IF EXISTS stage.mock_data_raw CASCADE;

CREATE TABLE stage.mock_data_raw (
    raw_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file text NOT NULL DEFAULT 'unknown',
    id text,
    customer_first_name text,
    customer_last_name text,
    customer_age text,
    customer_email text,
    customer_country text,
    customer_postal_code text,
    customer_pet_type text,
    customer_pet_name text,
    customer_pet_breed text,
    seller_first_name text,
    seller_last_name text,
    seller_email text,
    seller_country text,
    seller_postal_code text,
    product_name text,
    product_category text,
    product_price text,
    product_quantity text,
    sale_date text,
    sale_customer_id text,
    sale_seller_id text,
    sale_product_id text,
    sale_quantity text,
    sale_total_price text,
    store_name text,
    store_location text,
    store_city text,
    store_state text,
    store_country text,
    store_phone text,
    store_email text,
    pet_category text,
    product_weight text,
    product_color text,
    product_size text,
    product_brand text,
    product_material text,
    product_description text,
    product_rating text,
    product_reviews text,
    product_release_date text,
    product_expiry_date text,
    supplier_name text,
    supplier_contact text,
    supplier_email text,
    supplier_phone text,
    supplier_address text,
    supplier_city text,
    supplier_country text
);

DO $$
DECLARE
    file_name text;
    files text[] := ARRAY[
        'MOCK_DATA.csv',
        'MOCK_DATA (1).csv',
        'MOCK_DATA (2).csv',
        'MOCK_DATA (3).csv',
        'MOCK_DATA (4).csv',
        'MOCK_DATA (5).csv',
        'MOCK_DATA (6).csv',
        'MOCK_DATA (7).csv',
        'MOCK_DATA (8).csv',
        'MOCK_DATA (9).csv'
    ];
BEGIN
    FOREACH file_name IN ARRAY files LOOP
        EXECUTE format('ALTER TABLE stage.mock_data_raw ALTER COLUMN source_file SET DEFAULT %L', file_name);
        EXECUTE format($copy$
            COPY stage.mock_data_raw (
                id, customer_first_name, customer_last_name, customer_age, customer_email, customer_country,
                customer_postal_code, customer_pet_type, customer_pet_name, customer_pet_breed,
                seller_first_name, seller_last_name, seller_email, seller_country, seller_postal_code,
                product_name, product_category, product_price, product_quantity, sale_date, sale_customer_id,
                sale_seller_id, sale_product_id, sale_quantity, sale_total_price, store_name, store_location,
                store_city, store_state, store_country, store_phone, store_email, pet_category, product_weight,
                product_color, product_size, product_brand, product_material, product_description,
                product_rating, product_reviews, product_release_date, product_expiry_date, supplier_name,
                supplier_contact, supplier_email, supplier_phone, supplier_address, supplier_city, supplier_country
            )
            FROM %L WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')
        $copy$, '/data/' || file_name);
    END LOOP;
END $$;

ALTER TABLE stage.mock_data_raw ALTER COLUMN source_file SET DEFAULT 'unknown';

CREATE OR REPLACE VIEW stage.v_mock_data_typed AS
SELECT
    raw_id,
    source_file,
    NULLIF(id, '')::integer AS source_id,
    NULLIF(customer_first_name, '') AS customer_first_name,
    NULLIF(customer_last_name, '') AS customer_last_name,
    NULLIF(customer_age, '')::integer AS customer_age,
    lower(NULLIF(customer_email, '')) AS customer_email,
    NULLIF(customer_country, '') AS customer_country,
    NULLIF(customer_postal_code, '') AS customer_postal_code,
    lower(NULLIF(customer_pet_type, '')) AS customer_pet_type,
    NULLIF(customer_pet_name, '') AS customer_pet_name,
    NULLIF(customer_pet_breed, '') AS customer_pet_breed,
    NULLIF(seller_first_name, '') AS seller_first_name,
    NULLIF(seller_last_name, '') AS seller_last_name,
    lower(NULLIF(seller_email, '')) AS seller_email,
    NULLIF(seller_country, '') AS seller_country,
    NULLIF(seller_postal_code, '') AS seller_postal_code,
    NULLIF(product_name, '') AS product_name,
    NULLIF(product_category, '') AS product_category,
    NULLIF(product_price, '')::numeric(12, 2) AS product_price,
    NULLIF(product_quantity, '')::integer AS product_quantity,
    to_date(NULLIF(sale_date, ''), 'MM/DD/YYYY') AS sale_date,
    NULLIF(sale_customer_id, '')::integer AS sale_customer_id,
    NULLIF(sale_seller_id, '')::integer AS sale_seller_id,
    NULLIF(sale_product_id, '')::integer AS sale_product_id,
    NULLIF(sale_quantity, '')::integer AS sale_quantity,
    NULLIF(sale_total_price, '')::numeric(14, 2) AS sale_total_price,
    NULLIF(store_name, '') AS store_name,
    NULLIF(store_location, '') AS store_location,
    NULLIF(store_city, '') AS store_city,
    NULLIF(store_state, '') AS store_state,
    NULLIF(store_country, '') AS store_country,
    NULLIF(store_phone, '') AS store_phone,
    lower(NULLIF(store_email, '')) AS store_email,
    NULLIF(pet_category, '') AS pet_category,
    NULLIF(product_weight, '')::numeric(10, 2) AS product_weight,
    NULLIF(product_color, '') AS product_color,
    NULLIF(product_size, '') AS product_size,
    NULLIF(product_brand, '') AS product_brand,
    NULLIF(product_material, '') AS product_material,
    NULLIF(product_description, '') AS product_description,
    NULLIF(product_rating, '')::numeric(3, 1) AS product_rating,
    NULLIF(product_reviews, '')::integer AS product_reviews,
    to_date(NULLIF(product_release_date, ''), 'MM/DD/YYYY') AS product_release_date,
    to_date(NULLIF(product_expiry_date, ''), 'MM/DD/YYYY') AS product_expiry_date,
    NULLIF(supplier_name, '') AS supplier_name,
    NULLIF(supplier_contact, '') AS supplier_contact,
    lower(NULLIF(supplier_email, '')) AS supplier_email,
    NULLIF(supplier_phone, '') AS supplier_phone,
    NULLIF(supplier_address, '') AS supplier_address,
    NULLIF(supplier_city, '') AS supplier_city,
    NULLIF(supplier_country, '') AS supplier_country
FROM stage.mock_data_raw;

CREATE INDEX ix_mock_data_raw_source_file ON stage.mock_data_raw (source_file);
CREATE INDEX ix_mock_data_raw_source_ids ON stage.mock_data_raw (source_file, id, sale_customer_id, sale_seller_id, sale_product_id);

ANALYZE stage.mock_data_raw;
