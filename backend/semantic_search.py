# semantic_search.py
# Databricks-compatible version
# Reads facility data from Databricks
# Saves FAISS index + metadata locally in models/

import os
import faiss
import numpy as np
import pandas as pd

from sentence_transformers import SentenceTransformer
from database import get_cursor

# ---------------------------------------------------
# Paths
# ---------------------------------------------------
MODEL_DIR = "models"
os.makedirs(MODEL_DIR, exist_ok=True)

INDEX_PATH = os.path.join(MODEL_DIR, "facility_index.faiss")
META_PATH = os.path.join(MODEL_DIR, "facility_meta.pkl")

# ---------------------------------------------------
# Embedding model
# ---------------------------------------------------
model = SentenceTransformer("all-MiniLM-L6-v2")


# ---------------------------------------------------
# Fetch data from Databricks
# ---------------------------------------------------
def fetch_facilities():
    conn, cursor = get_cursor()

    cursor.execute("""
        SELECT
            name,
            specialties,
            equipment
        FROM facilities_cleaned
    """)

    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    return pd.DataFrame(rows, columns=cols)


# ---------------------------------------------------
# Build semantic index
# ---------------------------------------------------
def build_index():
    df = fetch_facilities()

    # Clean nulls
    df["name"] = df["name"].fillna("").astype(str)
    df["specialties"] = df["specialties"].fillna("").astype(str)
    df["equipment"] = df["equipment"].fillna("").astype(str)

    # Combined searchable text
    df["text"] = (
        df["name"] + " " +
        df["specialties"] + " " +
        df["equipment"]
    )

    texts = df["text"].tolist()

    if len(texts) == 0:
        return "No records found to index"

    # Create embeddings
    embeddings = model.encode(
        texts,
        show_progress_bar=True
    )

    embeddings = np.array(embeddings).astype("float32")

    # Build FAISS index
    dim = embeddings.shape[1]
    index = faiss.IndexFlatL2(dim)
    index.add(embeddings)

    # Save index + metadata
    faiss.write_index(index, INDEX_PATH)
    df.to_pickle(META_PATH)

    return {
        "message": "Semantic index built successfully",
        "records_indexed": len(df),
        "index_path": INDEX_PATH
    }


# ---------------------------------------------------
# Query semantic index
# ---------------------------------------------------
def semantic_query(query: str, top_k: int = 5):
    if not os.path.exists(INDEX_PATH):
        return {
            "error": "Index not found. Run /semantic/build first."
        }

    if not os.path.exists(META_PATH):
        return {
            "error": "Metadata not found. Run /semantic/build first."
        }

    index = faiss.read_index(INDEX_PATH)
    meta = pd.read_pickle(META_PATH)

    q_emb = model.encode([query])
    q_emb = np.array(q_emb).astype("float32")

    distances, indices = index.search(q_emb, top_k)

    results = meta.iloc[indices[0]][[
        "name",
        "specialties",
        "equipment"
    ]].copy()

    results["score"] = distances[0]

    return results.to_dict(orient="records")