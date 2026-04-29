import pandas as pd
from sklearn.ensemble import IsolationForest
from database import get_cursor
from model_store import save_model, load_model

def fetch_df():
    conn, cursor = get_cursor()
    cursor.execute("""
        SELECT *
        FROM facilities_cleaned
    """)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    cursor.close()
    conn.close()
    return pd.DataFrame(rows, columns=cols)

def prepare(df):
    df["capacity_num"] = pd.to_numeric(df["capacity"], errors="coerce").fillna(0)
    df["doctors_num"] = pd.to_numeric(df["numberDoctors"], errors="coerce").fillna(0)
    df["equipment_len"] = df["equipment"].fillna("").astype(str).str.len()
    df["specialties_len"] = df["specialties"].fillna("").astype(str).str.len()
    return df

def train_anomaly_model():
    df = prepare(fetch_df())

    X = df[[
        "capacity_num",
        "doctors_num",
        "equipment_len",
        "specialties_len"
    ]]

    model = IsolationForest(
        n_estimators=100,
        contamination=0.1,
        random_state=42
    )

    model.fit(X)
    save_model(model, "anomaly_model.pkl")

    return "Anomaly model trained and saved"

def predict_anomalies():
    df = prepare(fetch_df())

    X = df[[
        "capacity_num",
        "doctors_num",
        "equipment_len",
        "specialties_len"
    ]]

    model = load_model("anomaly_model.pkl")
    df["flag"] = model.predict(X)

    out = df[df["flag"] == -1]

    return out[[
        "name",
        "address_city",
        "capacity",
        "numberDoctors"
    ]].to_dict(orient="records")