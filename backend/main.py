from fastapi import FastAPI
from queries import *
from ai_features import natural_search
from ai_features import generate_trust_scores
from ml_models import train_anomaly_model, predict_anomalies
from llm_engine import parse_query
from semantic_search import build_index, semantic_query
from ml_recommend import train_random_forest, predict_random_forest
from ml_recommend import compare_models
from ml_recommend import recommend_ngo_by_specialty_ml

app = FastAPI(title="Virtue Health API")

@app.get("/")
def home():
    return {"message": "API Running"}

@app.get("/facilities/count")
def count_facilities():
    return get_total_facilities()

@app.get("/facilities")
def all_facilities():
    return get_all_facilities()

@app.get("/stats/total")
def total():
    return get_total_facilities()

@app.get("/stats/cities")
def cities():
    return facilities_by_city()

@app.get("/country/{country}")
def by_country(country: str):
    return search_country(country)

@app.get("/specialty/{name}")
def by_specialty(name: str):
    return search_specialty(name)

@app.get("/type/{facility_type}")
def by_type(facility_type: str):
    return search_type(facility_type)

@app.get("/search")
def smart_search(q: str):
    return natural_search(q)

@app.get("/trust-score")
def trust_score():
    return generate_trust_scores()

@app.get("/dashboard/countries")
def dashboard_countries():
    return facilities_by_country()

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

@app.get("/ml/train")
def train_model():
    return {"message": train_anomaly_model()}


@app.get("/ml/anomalies")
def anomalies():
    return predict_anomalies()

@app.get("/ai/parse")
def ai_parse(q: str):
    return parse_query(q)

@app.get("/semantic/build")
def build_semantic():
    return {"message": build_index()}


@app.get("/semantic")
def semantic(q: str):
    return semantic_query(q)

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