import pandas as pd
from database import get_connection

def natural_search(user_query: str):
    q = user_query.lower()

    conditions = []

    # country detection
    if "ghana" in q:
        conditions.append("address_country LIKE '%Ghana%'")

    # specialty detection
    if "cardiology" in q:
        conditions.append("specialties LIKE '%cardiology%'")

    # facility type detection
    if "hospital" in q:
        conditions.append("facilityTypeId LIKE '%hospital%'")

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    sql = f"""
        SELECT *
        FROM facilities_cleaned
        WHERE {where_clause}
        LIMIT 50
    """

    conn = get_connection()
    df = pd.read_sql(sql, conn)
    conn.close()

    return {
        "query": user_query,
        "sql_used": sql,
        "results": df.to_dict(orient="records")
    }
    
def generate_trust_scores():
    conn = get_connection()

    sql = """
    SELECT *,
    MIN(
        100,
        MAX(
            0,
            50
            + CASE WHEN officialPhone IS NOT NULL AND TRIM(officialPhone) != '' AND LOWER(officialPhone) != 'unknown' THEN 10 ELSE 0 END
            + CASE WHEN officialWebsite IS NOT NULL AND TRIM(officialWebsite) != '' AND LOWER(officialWebsite) != 'unknown' THEN 10 ELSE 0 END
            + CASE WHEN numberDoctors IS NOT NULL AND TRIM(numberDoctors) != '' AND LOWER(numberDoctors) != 'unknown' THEN 15 ELSE -20 END
            + CASE WHEN capacity IS NOT NULL AND TRIM(capacity) != '' AND LOWER(capacity) != 'unknown' THEN 15 ELSE 0 END
            + CASE WHEN equipment IS NOT NULL AND TRIM(equipment) != '' AND LOWER(equipment) != 'unknown' THEN 20 ELSE -20 END
            + CASE WHEN specialties IS NOT NULL AND TRIM(specialties) != '' AND LOWER(specialties) != 'unknown' THEN 10 ELSE 0 END
            + CASE WHEN address_city IS NOT NULL AND TRIM(address_city) != '' THEN 10 ELSE -15 END
        )
    ) AS trust_score
    FROM facilities_cleaned
    LIMIT 200
    """

    df = pd.read_sql(sql, conn)
    conn.close()
    return df.to_dict(orient="records")