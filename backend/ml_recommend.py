# ml_recommend.py
# Full Databricks-compatible version
# Reads training data from Databricks
# Saves/loads models from backend/models/

import pandas as pd
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from xgboost import XGBRegressor

from database import get_cursor
from model_store import save_model, load_model


# --------------------------------------------------
# Specialty keyword map
# --------------------------------------------------
SPECIALTY_MAP = {
    "eye-care": ["ophthalmology", "optometry", "eye"],
    "maternal": ["obstetrics", "gynecology", "maternity"],
    "cardiology": ["cardiology", "heart"],
    "dental": ["dentistry", "dental"],
    "pediatrics": ["pediatrics", "children"],
    "emergency": ["emergency", "trauma"]
}


# --------------------------------------------------
# Fetch data from Databricks
# --------------------------------------------------
def fetch_df():
    conn, cursor = get_cursor()

    cursor.execute("""
        SELECT
            address_city,
            capacity,
            numberDoctors,
            specialties
        FROM facilities_cleaned
        WHERE address_city IS NOT NULL
          AND trim(address_city) != ''
    """)

    rows = cursor.fetchall()
    cols = [desc[0] for desc in cursor.description]

    cursor.close()
    conn.close()

    return pd.DataFrame(rows, columns=cols)


# --------------------------------------------------
# Build city-level dataset
# --------------------------------------------------
def build_city_dataset():
    df = fetch_df()

    # Numeric cleanup
    df["capacity_num"] = pd.to_numeric(
        df["capacity"], errors="coerce"
    ).fillna(0)

    df["doctors_num"] = pd.to_numeric(
        df["numberDoctors"], errors="coerce"
    ).fillna(0)

    df["specialty_score"] = (
        df["specialties"]
        .fillna("")
        .astype(str)
        .apply(lambda x: 1 if x.strip() and x.lower() != "unknown" else 0)
    )

    city = df.groupby("address_city").agg(
        facility_count=("address_city", "count"),
        total_capacity=("capacity_num", "sum"),
        total_doctors=("doctors_num", "sum"),
        specialty_coverage=("specialty_score", "sum")
    ).reset_index()

    # Heuristic training target
    city["need_score"] = (
        100
        - city["facility_count"] * 5
        - city["total_capacity"] * 0.2
        - city["total_doctors"] * 1.5
        - city["specialty_coverage"] * 3
    ).clip(0, 100)

    return city


# --------------------------------------------------
# Train Random Forest
# --------------------------------------------------
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
        X,
        y,
        test_size=0.2,
        random_state=42
    )

    model = RandomForestRegressor(
        n_estimators=200,
        random_state=42
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)

    save_model(model, "ngo_rf_model.pkl")

    return {
        "message": "Random Forest trained successfully",
        "rows_used": len(city),
        "MAE": round(mae, 2)
    }


# --------------------------------------------------
# Predict underserved cities
# --------------------------------------------------
def predict_random_forest():
    city = build_city_dataset()

    model = load_model("ngo_rf_model.pkl")

    X = city[[
        "facility_count",
        "total_capacity",
        "total_doctors",
        "specialty_coverage"
    ]]

    city["predicted_need"] = model.predict(X)

    city = city.sort_values(
        "predicted_need",
        ascending=False
    )

    return city[[
        "address_city",
        "predicted_need",
        "facility_count",
        "total_capacity",
        "total_doctors"
    ]].head(10).to_dict(orient="records")


# --------------------------------------------------
# Compare Models
# --------------------------------------------------
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
        X,
        y,
        test_size=0.2,
        random_state=42
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


# --------------------------------------------------
# Recommend NGOs by specialty
# --------------------------------------------------
def recommend_ngo_by_specialty_ml(specialty: str):
    df = fetch_df()

    # -----------------------------
    # Clean numeric fields
    # -----------------------------
    df["capacity_num"] = pd.to_numeric(
        df["capacity"], errors="coerce"
    ).fillna(0)

    df["doctors_num"] = pd.to_numeric(
        df["numberDoctors"], errors="coerce"
    ).fillna(0)

    df["specialties"] = (
        df["specialties"]
        .fillna("")
        .astype(str)
        .str.lower()
    )

    # -----------------------------
    # Resolve specialty keywords
    # -----------------------------
    keywords = SPECIALTY_MAP.get(
        specialty.lower(),
        [specialty.lower()]
    )

    def has_specialty(text):
        return 1 if any(k in text for k in keywords) else 0

    df["requested_specialty_present"] = df["specialties"].apply(
        has_specialty
    )

    # -----------------------------
    # Aggregate by city
    # -----------------------------
    city = df.groupby("address_city").agg(
        facility_count=("address_city", "count"),
        total_capacity=("capacity_num", "sum"),
        total_doctors=("doctors_num", "sum"),
        specialty_coverage=("requested_specialty_present", "sum")
    ).reset_index()

    # -----------------------------
    # Load trained model
    # -----------------------------
    model = load_model("ngo_rf_model.pkl")

    X = city[[
        "facility_count",
        "total_capacity",
        "total_doctors",
        "specialty_coverage"
    ]]

    city["predicted_need"] = model.predict(X)

    # -----------------------------
    # Improved scoring (no saturation)
    # -----------------------------
    city["raw_score"] = (
        city["predicted_need"] * 0.85
        + (city["specialty_coverage"] == 0) * 10
        + (city["specialty_coverage"] == 1) * 5
        + (city["facility_count"] < 3) * 4
        + (city["total_doctors"] < 10) * 4
    )

    # Normalize to 0-100
    min_score = city["raw_score"].min()
    max_score = city["raw_score"].max()

    city["final_score"] = (
        (city["raw_score"] - min_score) /
        (max_score - min_score + 1e-9)
    ) * 100

    city["final_score"] = city["final_score"].round(2)

    # -----------------------------
    # Explanation text
    # -----------------------------
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

        if not reasons:
            reasons.append("ML-ranked underserved area")

        return ", ".join(reasons)

    city["reason"] = city.apply(explain, axis=1)

    city = city.sort_values(
        "final_score",
        ascending=False
    )

    return city[[
        "address_city",
        "predicted_need",
        "final_score",
        "specialty_coverage",
        "reason"
    ]].head(10).to_dict(orient="records")