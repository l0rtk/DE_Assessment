-- create tables with distribution keys and stuff
-- customers table - distributed by customer id for faster queries
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    country VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTRIBUTED BY (customer_id);

-- products table - distribute by product id
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    description TEXT,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    stock_quantity INTEGER NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTRIBUTED BY (product_id);

-- sales transactions - using transaction_id for distribution
CREATE TABLE sales_transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    purchase_date DATE NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    total_amount DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTRIBUTED BY (transaction_id);

-- shipping details table - distributed same as sales transactions for better joins
CREATE TABLE shipping_details (
    shipping_id SERIAL PRIMARY KEY,
    transaction_id INTEGER NOT NULL REFERENCES sales_transactions(transaction_id),
    shipping_date DATE NOT NULL,
    address_line1 VARCHAR(100) NOT NULL,
    address_line2 VARCHAR(100),
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50),
    postal_code VARCHAR(20) NOT NULL,
    country VARCHAR(50) NOT NULL,
    shipping_status VARCHAR(20) DEFAULT 'Pending' CHECK (shipping_status IN ('Pending', 'Shipped', 'Delivered', 'Returned')),
    tracking_number VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTRIBUTED BY (transaction_id);

-- add some indexes to make queries faster
CREATE INDEX idx_customers_country ON customers(country);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_sales_purchase_date ON sales_transactions(purchase_date);
CREATE INDEX idx_shipping_status ON shipping_details(shipping_status);

-- query to get monthly sales and 3month moving avg
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', st.purchase_date) AS sales_month,
        COUNT(*) as transaction_count,
        SUM(st.total_amount) as total_sales
    FROM sales_transactions st
    GROUP BY DATE_TRUNC('month', st.purchase_date)
),
moving_average AS (
    SELECT 
        sales_month,
        transaction_count,
        total_sales,
        AVG(total_sales) OVER (
            ORDER BY sales_month
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as three_month_moving_avg
    FROM monthly_sales
)
SELECT 
    TO_CHAR(sales_month, 'YYYY-MM') as month,
    transaction_count,
    ROUND(total_sales, 2) as total_sales,
    ROUND(three_month_moving_avg, 2) as three_month_moving_avg
FROM moving_average
ORDER BY sales_month;