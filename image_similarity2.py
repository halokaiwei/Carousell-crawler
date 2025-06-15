import pymysql
import os
import torch
import open_clip
import numpy as np
import database
from sklearn.metrics.pairwise import cosine_similarity
from PIL import Image
from concurrent.futures import ThreadPoolExecutor

def get_db_connection():
    logging.info("Connecting to database...")
    connection = database.get_connection()

    return connection.cursor(pymysql.cursors.DictCursor), connection

def fetch_similar_item_pairs(conn):
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    logging.info("Fetching similar_items...")
    cursor.execute("SELECT id, item_number1, item_number2 FROM similar_items")
    pairs = cursor.fetchall()
    return pairs

def load_clip_model():
    device = "cuda" if torch.cuda.is_available() else "cpu"
    logging.info(f"Loading CLIP model on {device}...")
    model, _, preprocess = open_clip.create_model_and_transforms('ViT-B-32', pretrained='openai')
    model = model.to(device)
    model.eval()
    return model, preprocess, device

def load_item_images(item_number):
    base_dir = f'carousell_img/{item_number}'
    if not os.path.exists(base_dir):
        return []
    
    images = []
    for filename in sorted(os.listdir(base_dir)):
        if filename.lower().endswith(('.jpg', '.jpeg', '.png', '.bmp', '.webp')):
            images.append(os.path.join(base_dir, filename))
    return images

def extract_features(image_paths, model, preprocess, device):
    images = []
    for path in image_paths:
        try:
            img = Image.open(path).convert('RGB')
            img = preprocess(img)
            images.append(img)
        except Exception as e:
            logging.info(f"Failed to process image {path}: {e}")
    
    if not images:
        return np.array([])

    images = torch.stack(images).to(device)
    with torch.no_grad():
        features = model.encode_image(images)
    features = features.cpu().numpy()
    return features

def compute_highest_image_similarity(item_number1, item_number2, model, preprocess, device):
    image_paths1 = load_item_images(item_number1)
    image_paths2 = load_item_images(item_number2)

    if not image_paths1 or not image_paths2:
        logging.info(f"No images for {item_number1} or {item_number2}, skipping...")
        return None

    features1 = extract_features(image_paths1, model, preprocess, device)
    features2 = extract_features(image_paths2, model, preprocess, device)

    if features1.size == 0 or features2.size == 0:
        logging.info(f"empty feature arrays {item_number1} or {item_number2}, skipping...")
        return None

    sims = cosine_similarity(features1, features2)
    highest_sim = np.max(sims)
    return highest_sim

DETAILS_SIM_THRESHOLD = 50  #threshold
IMAGE_SIM_THRESHOLD = 50 

def update_image_similarity(cursor, conn, pair_id, highest_sim):
    cursor.execute(
        "UPDATE similar_items SET images_highest_sim = %s WHERE id = %s",
        (highest_sim, pair_id)
    )

    cursor.execute(
        "SELECT details_highest_sim, images_highest_sim FROM similar_items WHERE id = %s",
        (pair_id,)
    )
    result = cursor.fetchone()
    if result:
        details_sim = result['details_highest_sim']
        images_sim = result['images_highest_sim']
        if details_sim > DETAILS_SIM_THRESHOLD and images_sim > IMAGE_SIM_THRESHOLD:
            cursor.execute(
                "UPDATE similar_items SET is_identified = 1 WHERE id = %s",
                (pair_id,)
            )

    conn.commit()

def process_pair(pair, model, preprocess, device):
    cursor, conn = get_db_connection()
    try:
        pair_id = pair['id']
        item1 = pair['item_number1']
        item2 = pair['item_number2']

        highest_sim = compute_highest_image_similarity(item1, item2, model, preprocess, device)
        if highest_sim is not None:
            logging.info(f"Similarity pair {pair_id} ({item1}, {item2}) highest sim: {highest_sim:.2f}")
            update_image_similarity(cursor, conn, pair_id, highest_sim)
    finally:
        conn.close()
        
def main():
    cursor, conn = get_db_connection()
    model, preprocess, device = load_clip_model()

    pairs = fetch_similar_item_pairs(conn)

    with ThreadPoolExecutor(max_workers=10) as executor:
        for pair in pairs:
            executor.submit(process_pair, pair, model, preprocess, device)

    conn.close()

if __name__ == "__main__":
    main()
