from queries import search_country, search_specialty, get_all_facilities

def natural_search(user_query: str):
    q = user_query.lower()

    if "ghana" in q:
        return {"query": user_query, "results": search_country("ghana")}

    if "cardiology" in q:
        return {"query": user_query, "results": search_specialty("cardiology")}

    if "hospital" in q:
        return {"query": user_query, "results": get_all_facilities(50)}

    return {"query": user_query, "results": get_all_facilities(20)}

def generate_trust_scores():
    data = get_all_facilities(200)

    def valid(value):
        if value is None:
            return False

        text = str(value).strip().lower()

        invalid = {
            "",
            "unknown",
            "n/a",
            "na",
            "null",
            "none",
            "-"
        }

        return text not in invalid

    for row in data:
        score = 50

        if valid(row.get("officialPhone")):
            score += 10

        if valid(row.get("officialWebsite")):
            score += 10

        if valid(row.get("numberDoctors")):
            score += 15
        else:
            score -= 20

        if valid(row.get("capacity")):
            score += 15

        if valid(row.get("equipment")):
            score += 20
        else:
            score -= 20

        if valid(row.get("specialties")):
            score += 10

        if valid(row.get("address_city")):
            score += 10
        else:
            score -= 15

        row["trust_score"] = max(0, min(100, score))

    return data