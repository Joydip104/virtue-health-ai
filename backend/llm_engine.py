def parse_query(user_query: str):
    q = user_query.lower()

    filters = {}

    if "accra" in q:
        filters["city"] = "Accra"

    if "ghana" in q:
        filters["country"] = "Ghana"

    if "cardiology" in q:
        filters["specialty"] = "cardiology"

    if "clinic" in q:
        filters["type"] = "clinic"

    if "low capacity" in q:
        filters["capacity_max"] = 10

    return {
        "user_query": user_query,
        "filters": filters
    }