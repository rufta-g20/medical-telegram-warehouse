# Ethiopian Medical Business Data Warehouse

An end-to-end data engineering pipeline designed to scrape, clean, and transform Telegram data from Ethiopian medical businesses into a structured Star Schema data warehouse.

## 🚀 Project Overview
This project implements a modern ELT (Extract, Load, Transform) framework:
- **Extract**: Scrapes messages and images from Telegram using Telethon.
- **Load**: Moves raw JSON data into a PostgreSQL "Data Lake" schema.
- **Transform**: Uses dbt (Data Build Tool) to clean data and model it into a Star Schema.

## 📁 Project Structure
```text
medical-telegram-warehouse/
├── .github/workflows/       # CI/CD for unit tests
├── api/                     # FastAPI application (Task 5)
├── data/
│   └── raw/                 # Local Data Lake
│       ├── images/          # Organized by {channel_name}/{msg_id}.jpg
│       └── telegram_messages/ # Partitioned by YYYY-MM-DD/channel.json
├── logs/                    # Scraper and application logs
├── medical_warehouse/       # dbt Project
│   ├── models/
│   │   ├── staging/         # Cleaning & type casting
│   │   └── marts/           # Star Schema (Dimensions & Facts)
│   ├── tests/               # Custom dbt data quality tests
│   └── dbt_project.yml
├── scripts/                 # Utility scripts (loading data to DB)
├── src/
│   └── scraper.py           # Telethon scraping logic
├── .env                     # Environment variables (DB credentials, API keys)
├── docker-compose.yml       # Container orchestration
├── requirements.txt         # Project dependencies
└── README.md

```

## 🛠️ Getting Started

### 1. Prerequisites

* Python 3.9+
* PostgreSQL
* Conda (Recommended)

### 2. Installation & Environment Setup

```bash
# Clone the repository
git clone git@github.com:rufta-g20/medical-telegram-warehouse.git
cd medical-telegram-warehouse

# Create and activate virtual environment
conda create -n telegram_scraper python=3.11 -y
conda activate telegram_scraper

# Install dependencies
pip install -r requirements.txt

```

### 3. Configuration

Create a `.env` file in the root directory:

```text
TG_API_ID=your_api_id
TG_API_HASH=your_api_hash
PHONE=your_phone_number

DB_HOST=localhost
DB_PORT=5432
DB_NAME=medical_db
DB_USER=postgres
DB_PASSWORD=your_password

```

### 4. Execution Pipeline

#### Step 1: Data Scraping (Task 1)

Extract raw data from Telegram channels into the local data lake.

```bash
python -m src.scraper

```

#### Step 2: Load to PostgreSQL (Task 2a)

Load the raw JSON files into the `raw` schema in your database.

```bash
python -m scripts.load_to_db

```

#### Step 3: dbt Transformation (Task 2b)

Transform raw data into a structured Star Schema.

```bash
cd medical_warehouse
dbt debug  # Verify connection
dbt run    # Run transformations
dbt test   # Run data quality tests

```

## 📊 Data Modeling (Star Schema)

The warehouse is designed using a Star Schema for optimized analytical performance:

* **Fact Table**: `fct_messages` (Metrics like views, forwards, and message lengths).
* **Dimension Tables**: `dim_channels` (Channel metadata) and `dim_dates` (Time-based analysis).

## ✅ Data Quality

We use dbt tests to ensure trust in our data:

* **Generic Tests**: `unique` and `not_null` on primary keys.
* **Custom Tests**:
* `assert_no_future_messages`: Ensures no data post-dates the current time.
* `assert_positive_views`: Ensures view counts are not negative.



## 📝 Documentation

To view the data lineage and model documentation:

```bash
dbt docs generate
dbt docs serve

```