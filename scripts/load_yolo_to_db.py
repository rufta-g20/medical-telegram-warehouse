import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

df = pd.read_csv('data/processed/yolo_detections.csv')
df.to_sql('raw_yolo_detections', engine, schema='raw', if_exists='replace', index=False)