import os
import json
import requests
import mysql.connector
import open_clip
import torch
import numpy as np
import database
import pymysql
from PIL import Image
from io import BytesIO
from collections import defaultdict
from sklearn.metrics.pairwise import cosine_similarity

device = "cuda" if torch.cuda.is_available() else "cpu"
model, _, preprocess = open_clip.create_model_and_transforms('ViT-B-32', pretrained='openai', device=device)

def fetch_items_from_db():
    conn = database.get_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SELECT id, item_number, images FROM crawled_items WHERE downloaded = 0")
    items = cursor.fetchall()
    cursor.close()
    return items

def mark_as_downloaded(item_db_id):
    conn = database.get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE crawled_items SET downloaded = 1 WHERE id = %s", (item_db_id,))
    conn.commit()
    cursor.close()

def download_images(items):
    for item in items:
        item_db_id = item['id']
        item_number = item['item_number']
        images_json = item['images']

        try:
            images_list = json.loads(images_json) if isinstance(images_json, str) else images_json
        except Exception as e:
            logging.info(f"get images failed: {e}")
            continue

        if not images_list:
            logging.info(f"Item {item_number} has no image")
            continue

        folder_path = os.path.join('carousell_img', str(item_number))
        os.makedirs(folder_path, exist_ok=True)

        for idx, url in enumerate(images_list, start=1):
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    img = Image.open(BytesIO(response.content)).convert('RGB')
                    img.save(os.path.join(folder_path, f'image{idx}.jpg'))
                    logging.info(f"Downloaded: {item_number} - image{idx}.jpg")
            except Exception as e:
                logging.info(f"Download failed {url}: {e}")

        mark_as_downloaded(item_db_id)

def load_image_features():
    item_images = {}
    item_features = {}

    for root, dirs, files in os.walk('carousell_img'): #find folders
        for file in files:
            if file.lower().endswith(('.png', '.jpg', '.jpeg')):
                img_path = os.path.join(root, file) 
                try:
                    img = Image.open(img_path).convert('RGB') #format RGB
                    #make sure the img is standard(model can interpret)
                    img_input = preprocess(img).unsqueeze(0).to(device) 
                    with torch.no_grad(): #no need gradient
                        feature = model.encode_image(img_input) #encode feature

                    item_number = extract_item_number_from_path(img_path)

                    relative_path = os.path.relpath(img_path, 'carousell_img')

                    if item_number not in item_images:
                        item_images[item_number] = []
                        item_features[item_number] = []

                    item_images[item_number].append(relative_path)
                    item_features[item_number].append(feature.cpu().numpy())

                except Exception as e:
                    logging.info(f"Failed to process {img_path}: {e}")

    return item_images, item_features

def calculate_similarity_between_different_items(item_images, item_features):
    similarities = []

    item_numbers = list(item_images.keys())
    num_items = len(item_numbers)

    for i in range(num_items):
        for j in range(i + 1, num_items): #no repeat compare
            item1 = item_numbers[i]
            item2 = item_numbers[j]

            paths1 = item_images[item1]
            paths2 = item_images[item2]

            features1 = item_features[item1]
            features2 = item_features[item2]

            for idx1, feature1 in enumerate(features1):
                for idx2, feature2 in enumerate(features2):
                    #calculate features similarity
                    sim = cosine_similarity(feature1.reshape(1, -1), feature2.reshape(1, -1))[0][0]
                    similarities.append((paths1[idx1], paths2[idx2], sim))

    return similarities

def save_similarities_to_db(similarities):
    conn = database.get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS image_similarities (
            id INT AUTO_INCREMENT PRIMARY KEY,
            image_path1 VARCHAR(255),
            image_path2 VARCHAR(255),
            similarity FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    for path1, path2, sim in similarities:
        cursor.execute(
            "INSERT INTO image_similarities (image_path1, image_path2, similarity) VALUES (%s, %s, %s)",
            (path1, path2, float(sim))
        )

    conn.commit()
    cursor.close()

def extract_item_number_from_path(img_path):
    parts = img_path.split(os.sep)
    if len(parts) >= 3:
        return parts[-3]
    return None

def main():
    items = fetch_items_from_db()
    download_images(items)

    item_images, item_features = load_image_features()

    for item_number, paths in item_images.items():
        logging.info(f"item_num: {item_number}, path: {paths}")

    similarities = calculate_similarity_between_different_items(item_images, item_features)

    for path1, path2, sim in similarities:
        logging.info(f"{path1} vs {path2}: similarity = {sim}")
    save_similarities_to_db(similarities)

if __name__ == "__main__":
    main()
