import pandas as pd
import sqlite3
import joblib
from sklearn.ensemble import IsolationForest

DB_PATH = "../database/healthcare.db"


def train_anomaly_model():
    conn = sqlite3.connect(DB_PATH)

    df = pd.read_sql("SELECT * FROM facilities_cleaned", conn)

    # Create numeric features
    df["capacity_num"] = pd.to_numeric(df["capacity"], errors="coerce").fillna(0)
    df["doctors_num"] = pd.to_numeric(df["numberDoctors"], errors="coerce").fillna(0)

    df["equipment_len"] = df["equipment"].fillna("").astype(str).apply(len)
    df["specialties_len"] = df["specialties"].fillna("").astype(str).apply(len)

    X = df[["capacity_num", "doctors_num", "equipment_len", "specialties_len"]]

    model = IsolationForest(
        n_estimators=100,
        contamination=0.1,
        random_state=42
    )

    model.fit(X)

    joblib.dump(model, "anomaly_model.pkl")
    conn.close()

    return "Model trained successfully"


def predict_anomalies():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM facilities_cleaned", conn)

    df["capacity_num"] = pd.to_numeric(df["capacity"], errors="coerce").fillna(0)
    df["doctors_num"] = pd.to_numeric(df["numberDoctors"], errors="coerce").fillna(0)
    df["equipment_len"] = df["equipment"].fillna("").astype(str).apply(len)
    df["specialties_len"] = df["specialties"].fillna("").astype(str).apply(len)

    X = df[["capacity_num", "doctors_num", "equipment_len", "specialties_len"]]

    model = joblib.load("anomaly_model.pkl")
    preds = model.predict(X)

    df["anomaly_flag"] = preds
    suspicious = df[df["anomaly_flag"] == -1]

    conn.close()

    return suspicious[[
        "name",
        "address_city",
        "capacity",
        "numberDoctors"
    ]].to_dict(orient="records")