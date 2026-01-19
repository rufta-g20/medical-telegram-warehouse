import os
import pandas as pd
from ultralytics import YOLO
import glob

# Load the pre-trained YOLOv8 nano model
model = YOLO('yolov8n.pt')

def classify_image(detected_objects):
    """
    Classification Scheme:
    - promotional: person + product (bottle/cup/bowl/etc)
    - product_display: bottle/container, no person
    - lifestyle: person, no product
    - other: everything else
    """
    has_person = 'person' in detected_objects
    # YOLO COCO classes that represent products/containers
    product_classes = {'bottle', 'cup', 'bowl', 'wine glass', 'cell phone'} 
    has_product = any(obj in product_classes for obj in detected_objects)

    if has_person and has_product:
        return 'promotional'
    elif has_product and not has_person:
        return 'product_display'
    elif has_person and not has_product:
        return 'lifestyle'
    else:
        return 'other'

def run_detection():
    image_paths = glob.glob('data/raw/images/**/*.jpg', recursive=True)
    results_data = []

    print(f"Found {len(image_paths)} images. Starting detection...")

    for path in image_paths:
        # Extract metadata from path (assuming structure: data/raw/images/channel_name/msg_id.jpg)
        parts = path.replace('\\', '/').split('/')
        channel_name = parts[-2]
        message_id = parts[-1].replace('.jpg', '')

        # Run YOLOv8 inference
        results = model(path, conf=0.25, verbose=False)
        
        for r in results:
            detected_classes = [model.names[int(c)] for c in r.boxes.cls]
            scores = [float(s) for s in r.boxes.conf]
            
            # Record each detection
            for obj, score in zip(detected_classes, scores):
                results_data.append({
                    'message_id': int(message_id),
                    'channel_name': channel_name,
                    'detected_obj': obj,
                    'confidence': score,
                    'image_category': classify_image(detected_classes)
                })

    # Save to CSV
    df = pd.DataFrame(results_data)
    os.makedirs('data/processed', exist_ok=True)
    df.to_csv('data/processed/yolo_detections.csv', index=False)
    print(f"Success! Results saved to data/processed/yolo_detections.csv")

if __name__ == "__main__":
    run_detection()