# media-sales-datawarehouse

End-to-end ETL pipeline integrating film and music sales data into a cloud-hosted MySQL star schema datamart, with business analytics reporting in R.
---

## Overview

Media Distributors, Inc. managed film and music sales in two separate SQLite databases. This project builds an integrated data warehouse to support unified business analytics — extracting data from both operational databases, transforming it into a common format, and loading it into a cloud-hosted MySQL datamart.

---

## Architecture
```
film-sales.db  (SQLite)  ─┐
                           ├──► ETL Pipeline (R) ──► MySQL Star Schema ──► R Markdown Report
music-sales.db (SQLite)  ─┘
```

## Tech Stack

| Layer | Tools |
|---|---|
| Operational DBs | SQLite (film-sales.db, music-sales.db) |
| Analytics DB | MySQL (cloud-hosted via Aiven) |
| ETL & Analysis | R, RStudio |
| Reporting | R Markdown, kableExtra |
| Modeling | Kimball Dimensional + One Big Table |

---

## Project Structure
```
├── partA.R                  # Cloud MySQL connection setup & validation
├── partB.R                  # Star schema creation (fact + dimension tables)
├── partC.R                  # ETL pipeline — extract, transform, load
├── partD.Rmd                # Business analytics report (knits to HTML)
├── sandbox_exploration.R    # Exploratory analysis of source databases
├── film-sales.db            # Operational film sales database (SQLite)
├── music-sales.db           # Operational music sales database (SQLite)
└── README.md
```

---

## Analytics Use Cases Supported

- Revenue (total & average) by country, month, quarter, year
- Units sold (total & average) by country and time period
- Customer counts by country segmented by type (film vs. music)
- Min/max/avg revenue and units by country, time period, and sales type

---

## Setup & Usage

### Prerequisites
- R (≥ 4.0) and RStudio
- MySQL instance (cloud or local)
- Required R packages: `RMySQL`, `RSQLite`, `DBI`, `knitr`, `kableExtra`, `rmarkdown`

### Environment Variables

Set the following in your `.Renviron` file and add it to `.gitignore`:
```
AIVEN_DB_HOST=your-host.aivencloud.com
AIVEN_DB_PORT=28549
AIVEN_DB_USER=avnadmin
AIVEN_DB_PASSWORD=your-password
AIVEN_DB_NAME=defaultdb
```

Then access in R:
```r
db <- dbConnect(RMySQL::MySQL(),
  host     = Sys.getenv("AIVEN_DB_HOST"),
  port     = as.integer(Sys.getenv("AIVEN_DB_PORT")),
  user     = Sys.getenv("AIVEN_DB_USER"),
  password = Sys.getenv("AIVEN_DB_PASSWORD"),
  dbname   = Sys.getenv("AIVEN_DB_NAME")
)
```

### Running the Project
```r
source("partB.R")              # 1. Set up schema
source("partC.R")              # 2. Run ETL pipeline
rmarkdown::render("partD.Rmd") # 3. Knit analytics report
```

---

## Key Design Decisions

- **Scalability** — ETL processes data via SQL chunks, avoids loading full tables into memory
- **Pre-aggregation** — Facts computed at load time for fast query performance
- **Portability** — DB connection abstracted; switching MySQL ↔ SQLite requires minimal changes
- **No dplyr** — All manipulation done in base R and SQL per course requirements

---
