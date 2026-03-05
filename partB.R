# Program: loadAnalyticsDB.PractII.SannakkiP.R
# Author: Pratik Sannakki
# Semester: Spring 2025

library(DBI)
library(RMySQL)
library(RSQLite)

# -------------------------------
# Helper: Safe Chunked Insert using REPLACE INTO
# -------------------------------
insert_rows_manually <- function(conn, table_name, df) {
  fields <- paste0("`", names(df), "`", collapse = ", ")
  for (i in seq_len(nrow(df))) {
    row_values <- sapply(df[i, ], function(val) {
      if (is.na(val)) {
        "NULL"
      } else if (is.numeric(val)) {
        as.character(val)
      } else {
        paste0("'", gsub("'", "''", val), "'")
      }
    })
    values <- paste0("(", paste(row_values, collapse = ", "), ")")
    query <- sprintf("REPLACE INTO %s (%s) VALUES %s;", table_name, fields, values)
    dbExecute(conn, query)
  }
}

# -------------------------------
# Connect to Databases
# -------------------------------
con_mysql <- dbConnect(
  RMySQL::MySQL(),
  dbname = "defaultdb",
  host = "cs5200-practicum-2-njain980316-ab68.i.aivencloud.com",
  port = 28549,
  user = "YOUR_PWD",
  password = "AVNS_aaxYX8Qu3vXWhFDcv1k"
)

film_db <- dbConnect(SQLite(), "film-sales.db")
music_db <- dbConnect(SQLite(), "music-sales.db")

chunk_size <- 500

# -------------------------------
# Ensure dim_time has required columns safely
# -------------------------------
tryCatch({ dbExecute(con_mysql, "ALTER TABLE dim_time ADD COLUMN day VARCHAR(2)") },
         error = function(e) message("Column 'day' may already exist"))
tryCatch({ dbExecute(con_mysql, "ALTER TABLE dim_time ADD COLUMN month VARCHAR(2)") },
         error = function(e) message("Column 'month' may already exist"))
tryCatch({ dbExecute(con_mysql, "ALTER TABLE dim_time ADD COLUMN quarter VARCHAR(2)") },
         error = function(e) message("Column 'quarter' may already exist"))
tryCatch({ dbExecute(con_mysql, "ALTER TABLE dim_time ADD COLUMN year VARCHAR(4)") },
         error = function(e) message("Column 'year' may already exist"))
tryCatch({ dbExecute(con_mysql, "ALTER TABLE dim_time MODIFY COLUMN quarter VARCHAR(2)") },
         error = function(e) message("Modify 'quarter' failed: ", e$message))

# -------------------------------
# Add fact_id column to fact_sales_summary if needed
# -------------------------------
tryCatch({
  dbExecute(con_mysql, "ALTER TABLE fact_sales_summary ADD COLUMN fact_id INT")
}, error = function(e) {
  message("Column 'fact_id' may already exist: ", e$message)
})

# -------------------------------
# Load dim_country
# -------------------------------
dbExecute(con_mysql, "DELETE FROM dim_customer")

countries_film <- dbGetQuery(film_db, "SELECT country_id, country AS country_name FROM country")
countries_music <- dbGetQuery(music_db, "SELECT DISTINCT Country AS country_name FROM customers")
countries_music$country_id <- NA

all_countries <- unique(rbind(
  countries_film[, c("country_id", "country_name")],
  countries_music[, c("country_id", "country_name")]
))

max_id <- max(na.omit(all_countries$country_id), na.rm = TRUE)
all_countries$country_id[is.na(all_countries$country_id)] <- seq(max_id + 1, by = 1, length.out = sum(is.na(all_countries$country_id)))

dbExecute(con_mysql, "DELETE FROM dim_country")

for (i in seq(1, nrow(all_countries), by = chunk_size)) {
  chunk <- all_countries[i:min(i + chunk_size - 1, nrow(all_countries)), ]
  insert_rows_manually(con_mysql, "dim_country", chunk)
}

# -------------------------------
# Load dim_product
# -------------------------------
films <- dbGetQuery(film_db, "SELECT film_id AS product_id, title AS product_title, 'film' AS product_type FROM film")
music <- dbGetQuery(music_db, "SELECT TrackId AS product_id, Name AS product_title, 'music' AS product_type FROM tracks")
products <- rbind(films, music)

dbExecute(con_mysql, "DELETE FROM dim_product")

for (i in seq(1, nrow(products), by = chunk_size)) {
  chunk <- products[i:min(i + chunk_size - 1, nrow(products)), ]
  insert_rows_manually(con_mysql, "dim_product", chunk)
}

# -------------------------------
# Load dim_customer
# -------------------------------
film_customers <- dbGetQuery(film_db, "
  SELECT 
    c.customer_id, 
    ci.country_id
  FROM customer c
  JOIN address a ON c.address_id = a.address_id
  JOIN city ci ON a.city_id = ci.city_id
")
film_customers$customer_type <- "film"

music_customers <- dbGetQuery(music_db, "SELECT CustomerId AS customer_id, Country FROM customers")
music_customers <- merge(music_customers, all_countries, by.x = "Country", by.y = "country_name", all.x = TRUE)
music_customers <- music_customers[, c("customer_id", "country_id")]
music_customers$customer_type <- "music"

customers <- rbind(film_customers, music_customers)

dbExecute(con_mysql, "DELETE FROM dim_customer")

for (i in seq(1, nrow(customers), by = chunk_size)) {
  chunk <- customers[i:min(i + chunk_size - 1, nrow(customers)), ]
  insert_rows_manually(con_mysql, "dim_customer", chunk)
}

# -------------------------------
# Load dim_time
# -------------------------------
payment_dates <- dbGetQuery(film_db, "SELECT DISTINCT DATE(payment_date) AS date FROM payment")
invoice_dates <- dbGetQuery(music_db, "SELECT DISTINCT DATE(InvoiceDate) AS date FROM invoices")
all_dates <- unique(rbind(payment_dates, invoice_dates))
all_dates <- all_dates[order(all_dates$date), , drop = FALSE]

all_dates$time_id <- seq_len(nrow(all_dates))
all_dates$day <- format(as.Date(all_dates$date), "%d")
all_dates$month <- format(as.Date(all_dates$date), "%m")
all_dates$quarter <- as.character(as.numeric(substr(quarters(as.Date(all_dates$date)), 2, 2)))  # Q1 → 1
all_dates$year <- format(as.Date(all_dates$date), "%Y")
all_dates <- all_dates[, c("time_id", "date", "day", "month", "quarter", "year")]

dbExecute(con_mysql, "DELETE FROM dim_time")
for (i in seq(1, nrow(all_dates), by = chunk_size)) {
  chunk <- all_dates[i:min(i + chunk_size - 1, nrow(all_dates)), ]
  insert_rows_manually(con_mysql, "dim_time", chunk)
}

# -------------------------------
# Load fact_sales_summary
# -------------------------------
film_facts <- dbGetQuery(film_db, "
  SELECT 
    c.customer_id,
    ci.country_id,
    DATE(p.payment_date) AS date,
    SUM(p.amount) AS total_revenue,
    COUNT(*) AS total_units_sold,
    'film' AS product_type
  FROM payment p
  JOIN customer c ON p.customer_id = c.customer_id
  JOIN address a ON c.address_id = a.address_id
  JOIN city ci ON a.city_id = ci.city_id
  GROUP BY country_id, date
")

music_facts <- dbGetQuery(music_db, "
  SELECT 
    i.CustomerId AS customer_id,
    c.Country,
    DATE(i.InvoiceDate) AS date,
    SUM(ii.UnitPrice * ii.Quantity) AS total_revenue,
    SUM(ii.Quantity) AS total_units_sold,
    'music' AS product_type
  FROM invoice_items ii
  JOIN invoices i ON ii.InvoiceId = i.InvoiceId
  JOIN customers c ON i.CustomerId = c.CustomerId
  GROUP BY c.Country, DATE(i.InvoiceDate)
")

music_facts <- merge(music_facts, all_countries, by.x = "Country", by.y = "country_name", all.x = TRUE)
music_facts$Country <- NULL

facts <- rbind(
  film_facts[, c("country_id", "date", "total_revenue", "total_units_sold", "product_type")],
  music_facts[, c("country_id", "date", "total_revenue", "total_units_sold", "product_type")]
)

facts <- merge(facts, all_dates, by.x = "date", by.y = "date", all.x = TRUE)
facts <- facts[, c("country_id", "time_id", "total_revenue", "total_units_sold", "product_type")]
facts$fact_id <- seq_len(nrow(facts))

dbExecute(con_mysql, "DELETE FROM fact_sales_summary")
for (i in seq(1, nrow(facts), by = chunk_size)) {
  chunk <- facts[i:min(i + chunk_size - 1, nrow(facts)), ]
  insert_rows_manually(con_mysql, "fact_sales_summary", chunk)
}

# -------------------------------
# Disconnect
# -------------------------------
dbDisconnect(film_db)
dbDisconnect(music_db)
dbDisconnect(con_mysql)
