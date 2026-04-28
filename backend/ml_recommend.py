import sqlite3
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor
import numpy as np

DB_PATH = "../database/healthcare.db"

SPECIALTY_MAP = {
    "eye-care": ["ophthalmology", "optometry", "eye"],
    "maternal": ["obstetrics", "gynecology", "maternity"],
    "cardiology": ["cardiology", "heart"],
    "dental": ["dentistry", "dental"],
    "pediatrics": ["pediatrics", "children"],
    "emergency": ["emergency", "trauma"]
}


def build_city_dataset():
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql("""
        SELECT address_city, capacity, numberDoctors, specialties
        FROM facilities_cleaned
        WHERE address_city IS NOT NULL
    """, conn)

    conn.close()

    df["capacity_num"] = pd.to_numeric(df["capacity"], errors="coerce").fillna(0)
    df["doctors_num"] = pd.to_numeric(df["numberDoctors"], errors="coerce").fillna(0)
    df["specialty_score"] = df["specialties"].fillna("").astype(str).apply(
        lambda x: 1 if x.strip() and x.lower() != "unknown" else 0
    )

    city = df.groupby("address_city").agg(
        facility_count=("address_city", "count"),
        total_capacity=("capacity_num", "sum"),
        total_doctors=("doctors_num", "sum"),
        specialty_coverage=("specialty_score", "sum")
    ).reset_index()

    # Existing heuristic score = training target
    city["need_score"] = (
        100
        - city["facility_count"] * 5
        - city["total_capacity"] * 0.2
        - city["total_doctors"] * 1.5
        - city["specialty_coverage"] * 3
    ).clip(0, 100)

    return city

def train_random_forest():
    city = build_city_dataset()

    X = city[[
        "facility_count",
        "total_capacity",
        "total_doctors",
        "specialty_coverage"
    ]]

    y = city["need_score"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestRegressor(
        n_estimators=200,
        random_state=42
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)

    joblib.dump(model, "ngo_rf_model.pkl")

    return {"message": "Random Forest trained", "MAE": round(mae, 2)}

def predict_random_forest():
    city = build_city_dataset()

    X = city[[
        "facility_count",
        "total_capacity",
        "total_doctors",
        "specialty_coverage"
    ]]

    model = joblib.load("ngo_rf_model.pkl")

    city["predicted_need"] = model.predict(X)

    city = city.sort_values("predicted_need", ascending=False)

    return city[[
        "address_city",
        "predicted_need",
        "facility_count",
        "total_capacity",
        "total_doctors"
    ]].head(10).to_dict(orient="records")
    
def compare_models():
    city = build_city_dataset()

    X = city[[
        "facility_count",
        "total_capacity",
        "total_doctors",
        "specialty_coverage"
    ]]

    y = city["need_score"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    models = {
        "Random Forest": RandomForestRegressor(
            n_estimators=200,
            random_state=42
        ),
        "XGBoost": XGBRegressor(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=5,
            random_state=42
        )
    }

    results = []

    for name, model in models.items():
        model.fit(X_train, y_train)
        preds = model.predict(X_test)

        mae = mean_absolute_error(y_test, preds)
        rmse = np.sqrt(mean_squared_error(y_test, preds))
        r2 = r2_score(y_test, preds)

        results.append({
            "model": name,
            "MAE": round(mae, 2),
            "RMSE": round(rmse, 2),
            "R2": round(r2, 3)
        })

    return results


def recommend_ngo_by_specialty_ml(specialty: str):
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql("""
        SELECT
            address_city,
            capacity,
            numberDoctors,
            specialties
        FROM facilities_cleaned
        WHERE address_city IS NOT NULL
    """, conn)

    conn.close()

    # Clean numeric fields
    df["capacity_num"] = pd.to_numeric(df["capacity"], errors="coerce").fillna(0)
    df["doctors_num"] = pd.to_numeric(df["numberDoctors"], errors="coerce").fillna(0)
    df["specialties"] = df["specialties"].fillna("").astype(str).str.lower()

    # Resolve keywords
    keywords = SPECIALTY_MAP.get(specialty.lower(), [specialty.lower()])

    def has_specialty(text):
        return 1 if any(k in text for k in keywords) else 0

    df["requested_specialty_present"] = df["specialties"].apply(has_specialty)

    # Aggregate city features
    city = df.groupby("address_city").agg(
        facility_count=("address_city", "count"),
        total_capacity=("capacity_num", "sum"),
        total_doctors=("doctors_num", "sum"),
        specialty_coverage=("requested_specialty_present", "sum")
    ).reset_index()

    # Load trained Random Forest model
    model = joblib.load("ngo_rf_model.pkl")

    X = city[[
        "facility_count",
        "total_capacity",
        "total_doctors",
        "specialty_coverage"
    ]]

    # Base ML prediction
    city["predicted_need"] = model.predict(X)

    # Specialty penalty / bonus
    # If no specialty exists => increase need
    city["final_score"] = city["predicted_need"] + (
        (city["specialty_coverage"] == 0) * 15
    ) + (
        (city["specialty_coverage"] == 1) * 8
    )

    city["final_score"] = city["final_score"].clip(0, 100)

    # Explanation
    def explain(row):
        reasons = []

        if row["specialty_coverage"] == 0:
            reasons.append(f"no {specialty} coverage")
        elif row["specialty_coverage"] == 1:
            reasons.append(f"limited {specialty} coverage")

        if row["facility_count"] < 3:
            reasons.append("low facility count")

        if row["total_doctors"] < 10:
            reasons.append("few doctors")

        return ", ".join(reasons) if reasons else "ML-ranked underserved area"

    city["reason"] = city.apply(explain, axis=1)

    city = city.sort_values("final_score", ascending=False)

    return city[[
        "address_city",
        "predicted_need",
        "final_score",
        "specialty_coverage",
        "reason"
    ]].head(10).to_dict(orient="records")