import sqlite3
import pandas as pd
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

DB_PATH = "../database/healthcare.db"

model = SentenceTransformer("all-MiniLM-L6-v2")


def build_index():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT rowid, name, specialties, equipment FROM facilities_cleaned", conn)

    df["text"] = (
        df["name"].fillna("") + " " +
        df["specialties"].fillna("") + " " +
        df["equipment"].fillna("")
    )

    texts = df["text"].tolist()
    embeddings = model.encode(texts)

    index = faiss.IndexFlatL2(embeddings.shape[1])
    index.add(np.array(embeddings).astype("float32"))

    faiss.write_index(index, "facility_index.faiss")
    df.to_pickle("facility_meta.pkl")

    conn.close()
    return "Semantic index built"


def semantic_query(query):
    index = faiss.read_index("facility_index.faiss")
    meta = pd.read_pickle("facility_meta.pkl")

    q_emb = model.encode([query]).astype("float32")
    D, I = index.search(q_emb, 5)

    results = meta.iloc[I[0]][["name", "specialties", "equipment"]]

    return results.to_dict(orient="records")