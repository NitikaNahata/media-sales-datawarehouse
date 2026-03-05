media-sales-datawarehouse
End-to-end ETL pipeline integrating film and music sales data into a cloud-hosted MySQL star schema datamart, with business analytics reporting in R.

Overview
Media Distributors, Inc. managed film and music sales in two separate SQLite databases. This project builds an integrated data warehouse to support unified business analytics — extracting data from both operational databases, transforming it into a common format, and loading it into a cloud-hosted MySQL datamart.

Architecture
film-sales.db  (SQLite)  ─┐
                           ├──► ETL Pipeline (R) ──► MySQL Star Schema ──► R Markdown Report
music-sales.db (SQLite)  ─┘
Star Schema Design

Fact tables pre-aggregate revenue, units sold, and customer counts
Dimensions: time (month/quarter/year), geography (country), sales type (film/music)
Indexed and partitioned for fast analytical queries


Tech Stack
LayerToolsOperational DBsSQLite (film-sales.db, music-sales.db)Analytics DBMySQL (cloud-hosted via Aiven)ETL & AnalysisR, RStudioReportingR Markdown, kableExtraModelingKimball Dimensional + One Big Table

Project Structure
├── partA.R                        # Cloud MySQL connection setup & validation
├── partB.R                        # Star schema creation (fact + dimension tables)
├── partC.R                        # ETL pipeline — extract, transform, load
├── partD.Rmd                      # Business analytics report (knits to HTML)
├── sandbox_exploration.R          # Exploratory analysis of source databases
├── film-sales.db                  # Operational film sales database (SQLite)
├── music-sales.db                 # Operational music sales database (SQLite)
└── README.md

Analytics Use Cases Supported

Revenue (total & average) by country, month, quarter, year
Units sold (total & average) by country and time period
Customer counts by country segmented by type (film vs. music)
Min/max/avg revenue and units by country, time period, and sales type


Setup & Usage
Prerequisites

R (≥ 4.0) and RStudio
MySQL instance (cloud or local)
Required R packages: RMySQL, RSQLite, DBI, knitr, kableExtra, rmarkdown

Environment Variables
Do not hardcode credentials. Set the following in your .Renviron file:
AIVEN_DB_HOST=your-host.aivencloud.com
AIVEN_DB_PORT=28549
AIVEN_DB_USER=avnadmin
AIVEN_DB_PASSWORD=your-password
AIVEN_DB_NAME=defaultdb
Then access in R:
rdb <- dbConnect(RMySQL::MySQL(),
  host     = Sys.getenv("AIVEN_DB_HOST"),
  port     = as.integer(Sys.getenv("AIVEN_DB_PORT")),
  user     = Sys.getenv("AIVEN_DB_USER"),
  password = Sys.getenv("AIVEN_DB_PASSWORD"),
  dbname   = Sys.getenv("AIVEN_DB_NAME")
)
Add .Renviron to your .gitignore — never commit credentials.
Running the Project
r# 1. Set up schema
source("partB.R")

# 2. Run ETL pipeline
source("partC.R")

# 3. Knit the analytics report
rmarkdown::render("partD.Rmd")

Key Design Decisions

Scalability: ETL pipeline processes data in chunks via SQL — avoids loading full tables into memory
Pre-aggregation: Facts are computed at load time, not at query time, for fast dashboard performance
Portability: Database connection is abstracted so switching from MySQL to SQLite requires minimal code changes
No dplyr: All data manipulation done in base R and SQL per course requirements


Report Preview
The knitted R Markdown report (partD.html) includes:

Revenue trends over time with drill-down by country
Film vs. music sales comparison
Customer distribution by geography
Formatted tables with kableExtra
