import os
import json
import sqlite3
from datetime import datetime
import pathlib

# ===== PATHS =====
BASE_DIR = pathlib.Path.cwd().parents[1]
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_JSON_DIR = os.path.join(DATA_DIR, "raw_json")
STAGING_DB_PATH = os.path.join(DATA_DIR, "staging", "staging.db")

# ===== 1) لو مفيش DB → نعمل واحدة =========
def init_staging_db():
    os.makedirs(os.path.dirname(STAGING_DB_PATH), exist_ok=True)

    conn = sqlite3.connect(STAGING_DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ivf_staging (
            female_id TEXT,
            case_id TEXT,
            doctor_id TEXT,
            male_id TEXT,
            female_age INTEGER,
            female_bmi REAL,
            amh_level REAL,
            fsh_level REAL,
            afc INTEGER,
            male_age INTEGER,
            male_factor TEXT,
            semen_count_mill_per_ml REAL,
            motility_percent REAL,
            morphology_percent REAL,
            protocol_id TEXT,
            protocol_type TEXT,
            stimulation_days INTEGER,
            total_fsh_dose REAL,
            trigger_type TEXT,
            e2_on_trigger REAL,
            endometrium_thickness REAL,
            follicles_18mm INTEGER,
            retrieved_oocytes INTEGER,
            injected_mii INTEGER,
            fertilized_oocytes INTEGER,
            blastocyst_d5 INTEGER,
            good_embryos INTEGER,
            risk_level TEXT,
            response_type TEXT,
            failure_reason TEXT,
            saved_at TEXT
        )
        """
    )
    conn.commit()
    conn.close()

# ===== 2) امسح كل اللي موجود في staging ==========
def clear_staging():
    conn = sqlite3.connect(STAGING_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM ivf_staging;")
    conn.commit()
    conn.close()

# ===== 3) اقرأ JSON الجديد بس ===========
def load_today_data():
    conn = sqlite3.connect(STAGING_DB_PATH)
    cursor = conn.cursor()

    today = datetime.now().strftime("%Y-%m-%d")

    for file in os.listdir(RAW_JSON_DIR):
        filepath = os.path.join(RAW_JSON_DIR, file)

        # لو مش JSON → طنشيه
        if not file.endswith(".json"):
            continue

        # لو مش بتاع النهارده !!! طنشيه
        file_date = file.split("_")[1][:8]  # YYYYMMDD
        if file_date != today.replace("-", ""):
            continue

        # افتح الملف واقرأ البيانات
        with open(filepath, "r", encoding="utf-8") as f:
            record = json.load(f)

        # لو في patient_id حوّليه إلى female_id
        if "patient_id" in record and "female_id" not in record:
            record["female_id"] = record.pop("patient_id")

        # INSERT INTO staging:
        columns = ", ".join(record.keys())
        placeholders = ", ".join(["?"] * len(record))

        cursor.execute(
            f"INSERT INTO ivf_staging ({columns}) VALUES ({placeholders})",
            tuple(record.values()),
        )

    conn.commit()
    conn.close()

# ===== MAIN FUNCTION =====
def run_staging_loader():
    init_staging_db()
    clear_staging()
    load_today_data()
    print("Staging DB is refreshed with today's data!")

if __name__ == "__main__":
    run_staging_loader()

