from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from queries import *
from ai_features import natural_search, generate_trust_scores
from ml_models import train_anomaly_model, predict_anomalies
from llm_engine import parse_query
from semantic_search import build_index, semantic_query
from ml_recommend import (
    train_random_forest,
    predict_random_forest,
    compare_models,
    recommend_ngo_by_specialty_ml
)

app = FastAPI(
    title="Virtue Health API",
    version="2.0",
    description="Healthcare intelligence platform powered by Databricks + AI"
)

# -----------------------------------
# CORS (frontend access)
# -----------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # restrict later if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------
# Root
# -----------------------------------
@app.get("/")
def home():
    return {
        "message": "Virtue Health API Running",
        "database": "Databricks"
    }

# -----------------------------------
# Health Check
# -----------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}

# -----------------------------------
# Facilities
# -----------------------------------
@app.get("/facilities/count")
def count_facilities():
    return get_total_facilities()

@app.get("/facilities")
def all_facilities(limit: int = 20):
    return get_all_facilities(limit)

@app.get("/country/{country}")
def by_country(country: str):
    return search_country(country)

@app.get("/specialty/{name}")
def by_specialty(name: str):
    return search_specialty(name)

@app.get("/type/{facility_type}")
def by_type(facility_type: str):
    return search_type(facility_type)

# -----------------------------------
# Search / AI
# -----------------------------------
@app.get("/search")
def smart_search(q: str):
    return natural_search(q)

@app.get("/ai/parse")
def ai_parse(q: str):
    return parse_query(q)

@app.get("/trust-score")
def trust_score():
    return generate_trust_scores()

@app.get("/semantic/build")
def build_semantic():
    return {"message": build_index()}

@app.get("/semantic")
def semantic(q: str):
    return semantic_query(q)

# -----------------------------------
# Dashboard / Analytics
# -----------------------------------
@app.get("/dashboard/countries")
def dashboard_countries():
    return facilities_by_country()

@app.get("/dashboard/cities")
def dashboard_cities():
    return facilities_by_city()

@app.get("/dashboard/specialties")
def dashboard_specialties():
    return top_specialties()

@app.get("/top-equipment")
def top_equipment():
    return top_equipped()

@app.get("/alerts/low-capacity")
def low_capacity():
    return low_capacity_alert()

@app.get("/map")
def get_map_data():
    return map_data()

# -----------------------------------
# ML
# -----------------------------------
@app.get("/ml/train")
def train_model():
    return {"message": train_anomaly_model()}

@app.get("/ml/anomalies")
def anomalies():
    return predict_anomalies()

@app.get("/ml/train-ngo")
def train_ngo():
    return train_random_forest()

@app.get("/ml/recommend-ngo")
def ml_ngo():
    return predict_random_forest()

@app.get("/ml/compare-models")
def compare():
    return compare_models()

@app.get("/ml/recommend-ngo-specialty")
def recommend_specialty_ml(specialty: str):
    return recommend_ngo_by_specialty_ml(specialty)