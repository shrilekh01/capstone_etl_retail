-- Staging tables
CREATE TABLE IF NOT EXISTS staging_products (
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(100),
    price DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS staging_sales (
    sales_id INT,
    product_id INT,
    store_id INT,
    quantity INT,
    total_sales DECIMAL(10,2),
    sale_date DATE
);

CREATE TABLE IF NOT EXISTS staging_stores (
    store_id INT,
    store_name VARCHAR(100)
);

-- Final tables
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_id INT,
    product_id INT,
    store_id INT,
    quantity INT,
    total_sales DECIMAL(10,2),
    sale_date DATE
);

CREATE TABLE IF NOT EXISTS fact_inventory (
    product_id INT,
    store_id INT,
    quantity_on_hand INT
);

CREATE TABLE IF NOT EXISTS monthly_sales_summary (
    product_id INT,
    year INT,
    month INT,
    monthly_total_sales DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS inventory_levels_by_store (
    store_id INT,
    total_inventory INT
);
