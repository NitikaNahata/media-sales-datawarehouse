# Program: createStarSchema.PractII.SannakkiP.R
# Author: Pratik Sannakki
# Semester: Spring 2025

library(DBI)
library(RMySQL)

# Connect to Cloud MySQL
con <- dbConnect(
  RMySQL::MySQL(),
  dbname = "defaultdb",
  host = "cs5200-practicum-2-njain980316-ab68.i.aivencloud.com",
  port = 28549,
  user = "avnadmin",
  password = "YOUR_PWD",
  ssl.mode = "REQUIRED"
)

# -------------------
# Dimension Tables
# -------------------

# dim_time — new surrogate key
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS dim_time (
    time_id INT AUTO_INCREMENT PRIMARY KEY,
    date DATE,
    month INT,
    quarter INT,
    year INT,
    UNIQUE(date)
  );
")

# dim_country — country_id comes from source DB, no AUTO_INCREMENT
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS dim_country (
    country_id INT PRIMARY KEY,
    country_name VARCHAR(100)
  );
")

# dim_product — product_id from film_id or track_id
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS dim_product (
    product_id INT PRIMARY KEY,
    product_title VARCHAR(255),
    product_type VARCHAR(10) -- 'film' or 'music'
  );
")

# dim_customer — source_customer_id comes from customer_id in original DB
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INT PRIMARY KEY,
    customer_type VARCHAR(10), -- 'film' or 'music'
    country_id INT,
    FOREIGN KEY (country_id) REFERENCES dim_country(country_id)
  );
")

# -------------------
# Fact Table
# -------------------

# fact_sales_summary — surrogate key for this aggregated fact table
dbExecute(con, "
  CREATE TABLE IF NOT EXISTS fact_sales_summary (
    sales_id INT AUTO_INCREMENT PRIMARY KEY,
    time_id INT,
    country_id INT,
    product_type VARCHAR(10),
    total_units_sold INT,
    avg_units_sold DECIMAL(10,2),
    total_revenue DECIMAL(12,2),
    avg_revenue DECIMAL(12,2),
    customer_count INT,
    min_units_sold INT,
    max_units_sold INT,
    min_revenue DECIMAL(12,2),
    max_revenue DECIMAL(12,2),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id),
    FOREIGN KEY (country_id) REFERENCES dim_country(country_id)
  );
")

# Disconnect
dbDisconnect(con)

