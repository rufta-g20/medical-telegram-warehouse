import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from telethon import TelegramClient

# Load credentials
load_dotenv()
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')
phone = os.getenv('PHONE')

# Configure Logging 
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename='logs/scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Target channels 
channels = [
    'https://t.me/CheMed123', # Placeholder based on task
    'https://t.me/lobelia4cosmetics',
    'https://t.me/tikvahpharma',
    'https://t.me/tenamereja',
    'https://t.me/doctorfasil'
]

async def scrape_channel(client, channel_url):
    try:
        entity = await client.get_entity(channel_url)
        channel_name = entity.username or str(entity.id)
        
        # Prepare image folder 
        image_dir = f"data/raw/images/{channel_name}"
        os.makedirs(image_dir, exist_ok=True)
        
        scraped_data = []
        
        # Iterating messages 
        async for message in client.iter_messages(entity, limit=100):
            msg_data = {
                "message_id": message.id,
                "channel_name": channel_name,
                "message_date": message.date.isoformat(),
                "message_text": message.text,
                "views": message.views,
                "forwards": message.forwards,
                "has_media": message.media is not None
            }
            
            # Download images if present 
            if message.photo:
                img_filename = f"{image_dir}/{message.id}.jpg"
                await client.download_media(message.photo, img_filename)
                msg_data["image_path"] = img_filename
            
            scraped_data.append(msg_data)
        
        # Store in partitioned Data Lake 
        date_str = datetime.now().strftime('%Y-%m-%d')
        json_dir = f"data/raw/telegram_messages/{date_str}"
        os.makedirs(json_dir, exist_ok=True)
        
        with open(f"{json_dir}/{channel_name}.json", 'w', encoding='utf-8') as f:
            json.dump(scraped_data, f, indent=4, ensure_ascii=False)
            
        logging.info(f"Successfully scraped {len(scraped_data)} messages from {channel_name}")
        
    except Exception as e:
        logging.error(f"Error scraping {channel_url}: {str(e)}") 

async def main():
    async with TelegramClient('scraping_session', api_id, api_hash) as client:
        for channel in channels:
            try:
                await scrape_channel(client, channel)
            except Exception as e:
                logging.error(f"Error scraping {channel}: {str(e)}")

if __name__ == "__main__":
    # Create basic folder structure
    os.makedirs('data/raw', exist_ok=True)
    
    import asyncio
    asyncio.run(main())