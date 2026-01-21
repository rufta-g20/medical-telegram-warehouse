import time
import logging
from fastapi import FastAPI, Depends, Request, HTTPException, Query 
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from .database import get_db
from . import schemas


# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/api_usage.log"), # Saves to a file
        logging.StreamHandler()                    # Also prints to your terminal
    ]
)
logger = logging.getLogger("medical_api")

app = FastAPI(
    title="Medical Telegram Analytics API",
    description="API for accessing insights from Ethiopian Medical Telegram Channels",
    version="1.0.0"
)

# --- LOGGING MIDDLEWARE ---
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.perf_counter()
    response = await call_next(request)
    process_time = time.perf_counter() - start_time
    
    # Logs: Method, Path, Status Code, and how long it took
    logger.info(
        f"RID: {request.scope.get('root_path')} | "
        f"{request.method} {request.url.path} | "
        f"Status: {response.status_code} | "
        f"Duration: {process_time:.4f}s"
    )
    return response

@app.get("/")
def read_root():
    return {"message": "Welcome to the Medical Telegram Analytics API", "docs": "/docs"}

# --- ENDPOINTS ---

@app.get("/api/reports/top-products", response_model=List[schemas.ProductRanking], tags=["Reports"])
def get_top_products(limit: int = 10, db: Session = Depends(get_db)):
    """Returns the most frequently mentioned medical products."""
    query = text("SELECT message_text as product_name, COUNT(*) as mention_count FROM main.fct_messages GROUP BY 1 ORDER BY 2 DESC LIMIT :limit")
    result = db.execute(query, {"limit": limit}).fetchall()
    return result

@app.get("/api/channels/{channel_name}/activity", response_model=schemas.ChannelActivity, tags=["Channels"])
def get_channel_activity(channel_name: str, db: Session = Depends(get_db)):
    """Returns posting activity and trends for a specific channel."""
    query = text("SELECT channel_name, COUNT(*) as total_messages, AVG(view_count) as avg_views FROM main.fct_messages WHERE channel_name = :channel_name GROUP BY 1")
    result = db.execute(query, {"channel_name": channel_name}).fetchone()
    return result

@app.get("/api/search/messages", response_model=List[schemas.MessageSearch], tags=["Search"])
def search_messages(query: str, limit: int = 20, db: Session = Depends(get_db)):
    """Searches for messages containing a specific keyword."""
    logger.info(f"User searched for keyword: {query}")
    
    # We JOIN fct_messages with dim_channels to get the actual name
    sql_query = text("""
        SELECT 
            f.message_id, 
            d.channel_name, 
            f.message_text, 
            f.view_count
        FROM main.fct_messages f
        JOIN main.dim_channels d ON f.channel_key = d.channel_key
        WHERE f.message_text ILIKE :search_str 
        LIMIT :limit
    """)
    
    try:
        result = db.execute(sql_query, {"search_str": f"%{query}%", "limit": limit}).fetchall()
        return result
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/reports/visual-content", response_model=List[schemas.VisualStats], tags=["Reports"])
def get_visual_stats(db: Session = Depends(get_db)):
    """Returns statistics about image usage and YOLO detection categories."""
    query = text("SELECT image_category, COUNT(*) as total_count, AVG(confidence) as avg_confidence FROM main.fct_image_detections GROUP BY 1")
    result = db.execute(query).fetchall()
    return result