import pandas as pd
from database import get_connection
from ghana_city_coords import CITY_COORDS

def get_total_facilities():
    conn = get_connection()
    df = pd.read_sql("SELECT COUNT(*) as total FROM facilities_cleaned", conn)
    conn.close()
    return df.to_dict(orient="records")

def get_all_facilities(limit=20):
    conn = get_connection()
    df = pd.read_sql(f"SELECT * FROM facilities_cleaned LIMIT {limit}", conn)
    conn.close()
    return df.to_dict(orient="records")

def search_country(country):
    conn = get_connection()
    query = f"""
        SELECT * FROM facilities_cleaned
        WHERE address_country LIKE '%{country}%'
        LIMIT 50
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_dict(orient="records")

def search_specialty(skill):
    conn = get_connection()
    query = f"""
        SELECT * FROM facilities_cleaned
        WHERE specialties LIKE '%{skill}%'
        LIMIT 50
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_dict(orient="records")

def facilities_by_country():
    conn = get_connection()

    sql = """
    SELECT address_country, COUNT(*) as total
    FROM facilities_cleaned
    GROUP BY address_country
    ORDER BY total DESC
    """

    df = pd.read_sql(sql, conn)
    conn.close()
    return df.to_dict(orient="records")

def facilities_by_city():
    conn = get_connection()
    sql = """
    SELECT address_city, COUNT(*) AS total
    FROM facilities_cleaned
    GROUP BY address_city
    ORDER BY total DESC
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return df.to_dict(orient="records")

def search_type(facility_type):
    conn = get_connection()
    sql = f"""
    SELECT *
    FROM facilities_cleaned
    WHERE facilityTypeId LIKE '%{facility_type}%'
    LIMIT 50
    """
    df = pd.read_sql(sql, conn)
    conn.close()
    return df.to_dict(orient="records")

def top_specialties():
    conn = get_connection()

    sql = """
    SELECT specialties, COUNT(*) as total
    FROM facilities_cleaned
    WHERE specialties IS NOT NULL
    GROUP BY specialties
    ORDER BY total DESC
    LIMIT 10
    """

    df = pd.read_sql(sql, conn)
    conn.close()
    return df.to_dict(orient="records")


def top_equipped():
    conn = get_connection()

    sql = """
    SELECT 
        name,
        address_city,
        address_country,
        equipment,
        specialties
    FROM facilities_cleaned
    WHERE equipment IS NOT NULL
      AND TRIM(equipment) != ''
      AND LOWER(equipment) != 'unknown'
    ORDER BY LENGTH(equipment) DESC
    LIMIT 20
    """

    df = pd.read_sql(sql, conn)
    conn.close()

    return df.to_dict(orient="records")

def low_capacity_alert():
    conn = get_connection()

    sql = """
    SELECT
        name,
        address_city,
        address_country,
        capacity,
        numberDoctors,
        specialties
    FROM facilities_cleaned
    WHERE
        (
            CAST(capacity AS INTEGER) <= 10
            OR capacity IS NULL
            OR TRIM(capacity) = ''
            OR LOWER(capacity) = 'unknown'
        )
    ORDER BY
        CASE
            WHEN capacity IS NULL OR TRIM(capacity) = '' OR LOWER(capacity) = 'unknown'
            THEN 0
            ELSE CAST(capacity AS INTEGER)
        END ASC
    LIMIT 20
    """

    df = pd.read_sql(sql, conn)
    conn.close()

    return df.to_dict(orient="records")

def map_data():
    conn = get_connection()

    sql = """
    SELECT
        name,
        address_city,
        address_country,
        facilityTypeId,
        specialties
    FROM facilities_cleaned
    WHERE address_city IS NOT NULL
    """

    df = pd.read_sql(sql, conn)
    conn.close()

    # Clean city names
    df["address_city"] = df["address_city"].replace({
        "Accra Metro": "Accra",
        "Greater Accra": "Accra",
        "Kumasi Central": "Kumasi"
    })
    df["address_city"] = df["address_city"].astype(str).str.strip()

    # Map coordinates
    df["lat"] = df["address_city"].apply(
        lambda city: CITY_COORDS.get(city, [None, None])[0]
    )

    df["lng"] = df["address_city"].apply(
        lambda city: CITY_COORDS.get(city, [None, None])[1]
    )

    # Keep only rows with coordinates
    df = df[df["lat"].notnull() & df["lng"].notnull()]

    return df.to_dict(orient="records")

