import os
import subprocess
from dagster import asset, Definitions, AssetExecutionContext

# --- 1. Scrape Telegram Data ---
@asset(group_name="data_pipeline")
def scrape_telegram_data(context: AssetExecutionContext):
    """Runs the Telethon scraper to fetch new messages and images."""
    context.log.info("Starting Telegram Scraper...")
    # Replace with your actual scraper script path
    result = subprocess.run(["python", "-m", "src.scraper"], check=True)
    return result.returncode

# --- 2. Load Raw JSON to Postgres ---
@asset(deps=[scrape_telegram_data], group_name="data_pipeline")
def load_raw_to_postgres(context: AssetExecutionContext):
    """Loads the scraped JSON files into the PostgreSQL raw schema."""
    context.log.info("Loading JSON data to Postgres...")
    result = subprocess.run(["python", "-m", "scripts.load_to_db"], check=True)
    return result.returncode

# --- 3. Run Object Detection (YOLO) ---
@asset(deps=[scrape_telegram_data], group_name="data_pipeline")
def run_yolo_enrichment(context: AssetExecutionContext):
    """Analyzes images using YOLOv8 and saves results to CSV."""
    context.log.info("Starting YOLOv8 Object Detection...")
    result = subprocess.run(["python", "-m", "src.yolo_detect"], check=True)
    return result.returncode

# --- 4. Load YOLO results to Postgres ---
@asset(deps=[run_yolo_enrichment], group_name="data_pipeline")
def load_yolo_to_postgres(context: AssetExecutionContext):
    """Loads the YOLO CSV results into the PostgreSQL raw schema."""
    context.log.info("Loading YOLO detections to Postgres...")
    result = subprocess.run(["python", "-m", "scripts.load_yolo_to_db"], check=True)
    return result.returncode

# --- 5. Execute dbt Transformations ---
@asset(deps=[load_raw_to_postgres, load_yolo_to_postgres], group_name="data_pipeline")
def run_dbt_transformations(context: AssetExecutionContext):
    """Runs dbt models to transform raw data into analytics marts."""
    context.log.info("Running dbt models...")
    # Note: Make sure to point to the correct directory where your dbt project lives
    result = subprocess.run(["dbt", "run"], cwd="medical_warehouse", check=True)
    return result.returncode

# --- Dagster Definitions ---
defs = Definitions(
    assets=[
        scrape_telegram_data, 
        load_raw_to_postgres, 
        run_yolo_enrichment, 
        load_yolo_to_postgres,
        run_dbt_transformations
    ]
)