from database import get_cursor
from ghana_city_coords import CITY_COORDS


def rows_to_dict(cursor):
    cols = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]


def run_query(sql):
    conn, cursor = get_cursor()
    cursor.execute(sql)
    data = rows_to_dict(cursor)
    cursor.close()
    conn.close()
    return data


def get_total_facilities():
    return run_query("""
        SELECT COUNT(*) AS total
        FROM facilities_cleaned
    """)


def get_all_facilities(limit=20):
    return run_query(f"""
        SELECT *
        FROM facilities_cleaned
        LIMIT {int(limit)}
    """)


def search_country(country):
    country = country.replace("'", "''")
    return run_query(f"""
        SELECT *
        FROM facilities_cleaned
        WHERE lower(address_country) LIKE '%{country.lower()}%'
        LIMIT 50
    """)


def search_specialty(skill):
    skill = skill.replace("'", "''")
    return run_query(f"""
        SELECT *
        FROM facilities_cleaned
        WHERE lower(specialties) LIKE '%{skill.lower()}%'
        LIMIT 50
    """)


def facilities_by_country():
    return run_query("""
        SELECT address_country, COUNT(*) AS total
        FROM facilities_cleaned
        GROUP BY address_country
        ORDER BY total DESC
    """)


def facilities_by_city():
    return run_query("""
        SELECT address_city, COUNT(*) AS total
        FROM facilities_cleaned
        GROUP BY address_city
        ORDER BY total DESC
    """)


def search_type(facility_type):
    facility_type = facility_type.replace("'", "''")
    return run_query(f"""
        SELECT *
        FROM facilities_cleaned
        WHERE lower(facilityTypeId) LIKE '%{facility_type.lower()}%'
        LIMIT 50
    """)


def top_specialties():
    return run_query("""
        SELECT specialties, COUNT(*) AS total
        FROM facilities_cleaned
        WHERE specialties IS NOT NULL
          AND trim(specialties) != ''
        GROUP BY specialties
        ORDER BY total DESC
        LIMIT 10
    """)


def top_equipped():
    return run_query("""
        SELECT
            name,
            address_city,
            address_country,
            equipment,
            specialties
        FROM facilities_cleaned
        WHERE equipment IS NOT NULL
          AND trim(equipment) != ''
          AND lower(equipment) != 'unknown'
        ORDER BY length(equipment) DESC
        LIMIT 20
    """)


def low_capacity_alert():
    return run_query("""
        SELECT
            name,
            address_city,
            address_country,
            capacity,
            numberDoctors,
            specialties
        FROM facilities_cleaned
        WHERE
            CAST(capacity AS INT) <= 10
            OR capacity IS NULL
            OR trim(capacity) = ''
            OR lower(capacity) = 'unknown'
        LIMIT 20
    """)


def map_data():
    data = run_query("""
        SELECT
            name,
            address_city,
            address_country,
            facilityTypeId,
            specialties
        FROM facilities_cleaned
        WHERE address_city IS NOT NULL
          AND trim(address_city) != ''
    """)

    # Add coordinates in Python
    cleaned = []

    rename_map = {
        "Accra Metro": "Accra",
        "Greater Accra": "Accra",
        "Kumasi Central": "Kumasi"
    }

    for row in data:
        city = str(row["address_city"]).strip()
        city = rename_map.get(city, city)

        coords = CITY_COORDS.get(city)

        if coords:
            row["lat"] = coords[0]
            row["lng"] = coords[1]
            cleaned.append(row)

    return cleaned