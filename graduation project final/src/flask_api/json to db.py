@app.post("/api/v1/ivf-case")
def create_ivf_case():
    payload = request.get_json()

    # هنا نعمل validation سريع
    required = ["case_id", "female_age", "male_age", "protocol_type", "et_date"]
    for r in required:
        if r not in payload:
            return {"error": f"Missing field: {r}"}, 400

    conn = sqlite3.connect(RAW_DB)
    pd.DataFrame([payload]).to_sql("ivf_patients", conn, if_exists="append", index=False)
    conn.close()

    return {"status": "ok"}, 201
