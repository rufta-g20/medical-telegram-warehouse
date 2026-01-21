import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

# Database connection
engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

def load_raw_data():
    base_path = 'data/raw/telegram_messages'
    all_data = []

    # Read all JSON files in the partitioned directory
    for date_dir in os.listdir(base_path):
        date_path = os.path.join(base_path, date_dir)
        if os.path.isdir(date_path):
            for file in os.listdir(date_path):
                if file.endswith('.json'):
                    with open(os.path.join(date_path, file), 'r', encoding='utf-8') as f:
                        all_data.extend(json.load(f))

    df = pd.DataFrame(all_data)
    
    # Create schema and load data
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.commit()
    
    

    with engine.begin() as conn:
        # This empties the data but keeps the table and its dependent views intact
        conn.execute(text("TRUNCATE TABLE raw.telegram_messages CASCADE;"))
    
    df.to_sql('telegram_messages', engine, schema='raw', if_exists='append', index=False)
    print(f"Successfully loaded {len(df)} rows to raw.telegram_messages")

if __name__ == "__main__":
    load_raw_data()