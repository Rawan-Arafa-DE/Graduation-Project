import os
import logging
import sqlite3
from datetime import datetime
import pandas as pd
import numpy as np

# ===========================================================
#                 CONFIGURATION
# ===========================================================
BASE_PATH = r"/mnt/e/work/final_code/graduation_promax"
RAW_DB    = fr"{BASE_PATH}/data/raw/ivf_patients_test.db"
STAR_DB   = fr"{BASE_PATH}/data/warehouse_final/ivf_star_schema.db"
SCHEMA_SQL = fr"{BASE_PATH}/src/ETL/create_star_schema.sql"
LOG_FILE   = fr"{BASE_PATH}/src/ETL/logs/etl_log_ivf.txt"

os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
os.makedirs(os.path.dirname(STAR_DB), exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# ===========================================================
#                SCHEMA SQL (FULL REFRESH)
# ===========================================================
def run_schema_sql():
    conn = sqlite3.connect(STAR_DB)
    with open(SCHEMA_SQL, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()
    logging.info("Schema (fresh) created successfully.")

# ===========================================================
#                   RAW LOADING
# ===========================================================
def load_raw_df():
    try:
        conn = sqlite3.connect(RAW_DB)
        df = pd.read_sql("SELECT * FROM ivf_patients", conn)
        conn.close()
        logging.info(f"Loaded {len(df)} raw rows.")
        return df
    except Exception as e:
        logging.exception(f"Raw load FAILED: {e}")
        raise

# ===========================================================
#                   CLEAN DATA
# ===========================================================
def clean_data(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df = df.drop_duplicates()
    return df

# ===========================================================
#    SAFE PLACEHOLDER & ID GENERATION FOR MISSING DATA
# ===========================================================
def apply_placeholder_and_ids(df):
    df["female_id"] = [f"f_{i}" for i in df.index]
    df["male_id"]   = [f"m_{i}" for i in df.index]

    required_cols_protocol = ["protocol_type", "stimulation_days", "total_fsh_dose", "trigger_type"]
    for col in required_cols_protocol:
        if col not in df.columns:
            df[col] = "Unknown"

    df["protocol_id"] = (
        "prot_" + df["protocol_type"].astype(str).str.lower().str.replace(" ","_") +
        "_d" + df["stimulation_days"].astype(str) +
        "_dose" + df["total_fsh_dose"].astype(str) +
        "_trg_" + df["trigger_type"].astype(str).str.lower().str.replace(" ","_")
    )

    df["doctor_id"] = "dr_unknown"

    if "risk_level" not in df.columns:
        df["risk_level"] = "Unknown"
    if "response_type" not in df.columns:
        df["response_type"] = "Unknown"

    df["outcome_id"] = (
        "out_" + df["risk_level"].astype(str).str.lower().str.replace(" ","_") +
        "_" + df["response_type"].astype(str).str.lower().str.replace(" ","_")
    )

    if "fresh_et_stage" not in df.columns:
        df["fresh_et_stage"] = "NA"
    if "grading" not in df.columns:
        df["grading"] = "NA"

    df["embryo_id"] = [
        f"emb_{row.fresh_et_stage}_{row.grading}_{i}"
        for i, row in df.iterrows()
    ]

    df["transfer_time_id"] = pd.to_datetime(df.get("et_date", None), errors="coerce").dt.strftime("%Y-%m-%d")

    return df

# ===========================================================
#   SAFE INSERT → NO DUPLICATION (APPEND MODE)
# ===========================================================
def insert_or_ignore(table, df_subset, conn):
    cols = df_subset.columns.tolist()
    placeholders = ",".join("?" * len(cols))
    sql = f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
    conn.executemany(sql, df_subset.values.tolist())
    conn.commit()

# ===========================================================
#       DIMENSIONS LOADING (BOTH MODES SUPPORTED)
# ===========================================================
def load_dimensions(df, conn, refresh=True):
    dim_tables = {
        "dim_female":  ["female_id","female_age","female_bmi","amh_level","fsh_level","afc"],
        "dim_male":    ["male_id","male_age","male_factor","semen_count_mill_per_ml","motility_percent","morphology_percent"],
        "dim_protocol":["protocol_id","protocol_type","stimulation_days","total_fsh_dose","trigger_type","recommended_protocol"],
        "dim_doctor":  ["doctor_id","doctor_recommendation"],
        "dim_outcome": ["outcome_id","risk_level","response_type","suggested_waiting_period_days","failure_reason"],
        "dim_embryo":  ["embryo_id","fresh_et_stage","grading","class_a_rate"]
    }

    for table, cols in dim_tables.items():
        subset = df[cols].drop_duplicates()

        if refresh:
            subset.to_sql(table, conn, if_exists="replace", index=False)
        else:
            insert_or_ignore(table, subset, conn)

        logging.info(f"{table}: {len(subset)} processed.")

# ===========================================================
#       DIM_TIME → ALWAYS APPEND (NO DUPLICATES)
# ===========================================================
def build_dim_time(df, conn):
    tmp = pd.to_datetime(df.get("et_date", None), errors="coerce").dropna().drop_duplicates()

    time_dim = pd.DataFrame({
        "full_date": tmp.dt.strftime("%Y-%m-%d"),
        "day": tmp.dt.day,
        "month": tmp.dt.month,
        "month_name": tmp.dt.month_name(),
        "quarter": tmp.dt.quarter,
        "year": tmp.dt.year,
        "week": tmp.dt.isocalendar().week.astype(int)
    })

    for _, row in time_dim.iterrows():
        conn.execute(
            """
            INSERT OR IGNORE INTO dim_time
            (full_date, day, month, month_name, quarter, year, week)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            tuple(row)
        )
    conn.commit()
    logging.info("dim_time DONE.")

# ===========================================================
#              FACT TABLES (APPEND SAFE)
# ===========================================================
def load_fact_tables(df, conn):
    time_df = pd.read_sql("SELECT time_id, full_date FROM dim_time;", conn)
    date_to_id = dict(zip(time_df["full_date"], time_df["time_id"]))

    fact_needed_cols = [
        "e2_on_trigger","endometrium_thickness","follicles_18mm",
        "gv_count","injected_m2","fertilized_oocytes",
        "cleavage_d3","blastocyst_d5","good_embryos"
    ]
    for col in fact_needed_cols:
        if col not in df.columns:
            df[col] = 0

    df["cycle_start_time_id"] = df["transfer_time_id"].map(date_to_id)

    fact_cycle = df.drop_duplicates(subset=["case_id"])[[
        "case_id","female_id","male_id","protocol_id","doctor_id","outcome_id",
        "cycle_start_time_id","e2_on_trigger","endometrium_thickness",
        "follicles_18mm","retrieved_oocytes","m2_count","gv_count",
        "injected_m2","fertilized_oocytes","fertilization_rate",
        "cleavage_d3","blastocyst_d5","good_embryos"
    ]]
    insert_or_ignore("fact_ivf_cycle", fact_cycle, conn)

    # ---------- FACT 2 ----------
    if all(col in df.columns for col in [
        "case_id","transfer_time_id","doctor_id","embryos_transferred"
    ]):
        tmp = df.drop_duplicates(subset=["case_id"]).copy()
        tmp["transfer_time_fk"] = tmp["transfer_time_id"].map(date_to_id)
        fact_transfer = tmp[[
            "case_id","transfer_time_fk","doctor_id","embryos_transferred",
            "pregnancy_test_result","clinical_pregnancy",
            "live_birth","outcome_id","success_probability_score"
        ]]
        insert_or_ignore("fact_transfer", fact_transfer, conn)

    # ---------- FACT 3 ----------
    try:
        existing_transfer = pd.read_sql("SELECT transfer_sk, case_id FROM fact_transfer;", conn)
        if not existing_transfer.empty and "embryo_id" in df.columns:
            df_merge = df.merge(existing_transfer, on="case_id", how="inner")
            fact_embryo = df_merge[["transfer_sk","embryo_id"]].drop_duplicates()
            insert_or_ignore("fact_transfer_embryo", fact_embryo, conn)
    except Exception:
        logging.warning("fact_transfer_embryo skipped.")

# ===========================================================
#                    MAIN ETL
# ===========================================================
def run_full_etl(refresh=True):
    logging.info("===== ETL STARTED =====")
    if refresh:
        run_schema_sql()

    df = load_raw_df()
    df = clean_data(df)

    rename_map = {"mii_count": "m2_count", "injected_mii": "injected_m2"}
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    df = apply_placeholder_and_ids(df)

    conn = sqlite3.connect(STAR_DB)
    load_dimensions(df, conn, refresh=refresh)
    build_dim_time(df, conn)
    load_fact_tables(df, conn)
    conn.close()

    logging.info("ETL COMPLETED SUCCESSFULLY.")
    print("ETL Done ✔")

if __name__ == "__main__":
    run_full_etl(refresh=False)
