import pymysql
import database
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import logging

def fetch_all_items():
    conn = database.get_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    ("Fetching all items...")
    cursor.execute("SELECT * FROM crawled_items")
    items = cursor.fetchall()
    conn.close()
    logging.info(f"Fetched {len(items)} items.")
    return items

def calculate_text_similarity(text1, text2):
    #remove space
    text1 = text1.strip() if text1 else ''
    text2 = text2.strip() if text2 else ''

    if not text1 and not text2:
        return 0.0

    try:
        vectorizer = TfidfVectorizer()
        vectors = vectorizer.fit_transform([text1, text2])
        sim = cosine_similarity(vectors[0], vectors[1])[0][0]
        return sim * 100
    except ValueError as e:
        logging.info(f"Error TfidVectorizer text1: '{text1}', text2: '{text2}'")
        return 0.0

def group_similar_items(items):
    grouped = []  
    used_pairs = set()

    logging.info("Grouping items based on title+description similarity...")

    for i in range(len(items)):
        item1 = items[i]
        for j in range(i + 1, len(items)):  #compare next item
            item2 = items[j]
            
            #avoid comparing same item
            if item1['item_number'] == item2['item_number']:
                continue

            pair_key = tuple(sorted((item1['item_number'], item2['item_number'])))
            if pair_key in used_pairs:
                continue
            used_pairs.add(pair_key)

            cat_sim = calculate_text_similarity(item1['category'] or '', item2['category'] or '')
            title_sim = calculate_text_similarity(item1['title'] or '', item2['title'] or '')
            desc_sim = calculate_text_similarity(item1['description'] or '', item2['description'] or '')

            details_highest_sim = max(title_sim, desc_sim)

            #+5% if category is same
            if cat_sim > 80:
                details_highest_sim = min(details_highest_sim + 5, 100)

            if details_highest_sim == title_sim:
                logging.info(f"Title similarity for pair ({item1['item_number']}, {item2['item_number']}): {title_sim:.2f} (highest similarity)")
            else:
                logging.info(f"Title similarity for pair ({item1['item_number']}, {item2['item_number']}): {title_sim:.2f}")

            if details_highest_sim == desc_sim:
                logging.info(f"Description similarity for pair ({item1['item_number']}, {item2['item_number']}): {desc_sim:.2f} (highest similarity)")
            else:
                logging.info(f"Description similarity for pair ({item1['item_number']}, {item2['item_number']}): {desc_sim:.2f}")

            if details_highest_sim > 50:
                logging.info(f"details_highest_sim={details_highest_sim:.2f} > 50, added pair: ({item1['item_number']}, {item2['item_number']})")
                grouped.append((item1, item2, details_highest_sim))
            else:
                logging.info(f"details_highest_sim={details_highest_sim:.2f} < 50, skipped pair: ({item1['item_number']}, {item2['item_number']})")

    return grouped

def insert_similar_items(item_number1, item_number2, reason, details_highest_sim):
    conn = database.get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS similar_items (
            id INT AUTO_INCREMENT PRIMARY KEY,
            item_number1 VARCHAR(255),
            item_number2 VARCHAR(255),
            reason VARCHAR(255),
            details_highest_sim FLOAT DEFAULT NULL,
            images_highest_sim FLOAT DEFAULT NULL,
            is_verified TINYINT(1) DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    sql = """
    INSERT INTO similar_items (item_number1, item_number2, reason, details_highest_sim)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE details_highest_sim = GREATEST(details_highest_sim, %s)
    """
    cursor.execute(sql, (item_number1, item_number2, reason, details_highest_sim, details_highest_sim))
    conn.commit()
    logging.info(f"Inserted similar pair ({item_number1}, {item_number2}), reason: {reason} and details_highest_sim: {details_highest_sim}")

# 执行
def main():
    items = fetch_all_items()
    grouped_items = group_similar_items(items)
    for item1, item2, details_highest_sim in grouped_items:
        reason = "category_sim>50" if calculate_text_similarity(item1['category'], item2['category']) > 50 else "title_desc_sim >50"
        insert_similar_items(item1['item_number'], item2['item_number'], reason, details_highest_sim)

if __name__ == "__main__":
    main()
