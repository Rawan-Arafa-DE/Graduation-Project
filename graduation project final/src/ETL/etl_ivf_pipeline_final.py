import os
import logging
import sqlite3
import uuid
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
logger = logging.getLogger("ivf_etl")

# ===========================================================
#                SCHEMA SQL (FULL REFRESH)
# ===========================================================
def run_schema_sql():
    try:
        conn = sqlite3.connect(STAR_DB)
        with open(SCHEMA_SQL, "r", encoding="utf-8") as f:
            conn.executescript(f.read())
        conn.commit()
        conn.close()
        logging.info("Schema (fresh) created successfully.")
    except Exception as e:
        logging.exception("run_schema_sql FAILED")
        raise

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
        logging.exception("load_raw_df FAILED")
        raise

# ===========================================================
#                   CLEAN DATA
# ===========================================================
def clean_data(df):
    try:
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        df = df.drop_duplicates()
        return df
    except Exception as e:
        logging.exception("clean_data FAILED")
        raise

# ===========================================================
#    CHECK REQUIRED COLUMNS (ONLY 3)
# ===========================================================
def check_required(df):
    required = ["case_id", "female_id", "male_id"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        logging.error(f"Missing required columns: {missing}")
        raise ValueError(f"Missing required columns: {missing}")

# ===========================================================
#   GENERATE ID IF MISSING (preserve existing IDs)
# ===========================================================
def gen_if_missing_series(series, prefix):
    # returns a series where missing/empty entries are replaced with prefix_uuid
    def gen(v):
        if pd.isna(v) or str(v).strip() == "":
            return f"{prefix}_{uuid.uuid4().hex[:8]}"
        return v
    return series.apply(gen)

# ===========================================================
#   SET NON-CRITICAL IDs TO NULL + GEN FOR protocol/outcome/embryo
# ===========================================================
def handle_ids(df):
    try:
        # ensure columns exist
        for c in ["protocol_id", "outcome_id", "embryo_id"]:
            if c not in df.columns:
                df[c] = None

        # generate IDs only if missing (preserve existing)
        df["protocol_id"] = gen_if_missing_series(df["protocol_id"], "prot")
        df["outcome_id"] = gen_if_missing_series(df["outcome_id"], "out")
        df["embryo_id"] = gen_if_missing_series(df["embryo_id"], "emb")

        # optional columns
        if "fresh_et_stage" not in df.columns:
            df["fresh_et_stage"] = None
        if "grading" not in df.columns:
            df["grading"] = None

        # transfer_time_id from et_date (string YYYY-MM-DD) or None
        df["transfer_time_id"] = pd.to_datetime(df.get("et_date", None), errors="coerce").dt.strftime("%Y-%m-%d")

        return df
    except Exception as e:
        logging.exception("handle_ids FAILED")
        raise

# ===========================================================
#     MAP DOCTOR NAME → AUTO INCREMENT doctor_id + recommendation
# ===========================================================
def map_doctor_ids(df, conn):
    try:
        # Ensure both columns exist
        if "doctor_name" not in df.columns:
            df["doctor_name"] = "Unknown"

        if "doctor_recommendation" not in df.columns:
            df["doctor_recommendation"] = None

        # Ensure table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_doctor (
                doctor_id INTEGER PRIMARY KEY AUTOINCREMENT,
                doctor_name TEXT,
                doctor_recommendation TEXT,
                UNIQUE (doctor_name, doctor_recommendation)
            )
        """)
        conn.commit()

        # Read existing doctor combinations
        existing = pd.read_sql(
            "SELECT doctor_id, doctor_name, doctor_recommendation FROM dim_doctor",
            conn
        )

        # Build natural key: name + recommendation
        existing["nat_key"] = (
            existing["doctor_name"].astype(str).str.strip()
            + "|" +
            existing["doctor_recommendation"].astype(str).str.strip()
        )

        df["nat_key"] = (
            df["doctor_name"].astype(str).str.strip()
            + "|" +
            df["doctor_recommendation"].astype(str).str.strip()
        )

        # Map existing nat_keys → doctor_id
        nat_to_id = dict(zip(existing["nat_key"], existing["doctor_id"]))

        # Identify new doctor rows
        new_rows = df.loc[~df["nat_key"].isin(nat_to_id)][
            ["doctor_name", "doctor_recommendation", "nat_key"]
        ].drop_duplicates()

        # Insert new rows
        for _, row in new_rows.iterrows():
            cur = conn.execute(
                "INSERT OR IGNORE INTO dim_doctor (doctor_name, doctor_recommendation) VALUES (?, ?)",
                (row["doctor_name"], row["doctor_recommendation"])
            )
            conn.commit()

            # Retrieve id after insert
            doctor_id = conn.execute(
                "SELECT doctor_id FROM dim_doctor WHERE doctor_name = ? AND doctor_recommendation = ?",
                (row["doctor_name"], row["doctor_recommendation"])
            ).fetchone()[0]

            nat_to_id[row["nat_key"]] = doctor_id

        # Assign final doctor_ids
        df["doctor_id"] = df["nat_key"].map(nat_to_id)

        logging.info(f"Mapped {len(nat_to_id)} unique doctor entries.")
        return df

    except Exception:
        logging.exception("map_doctor_ids FAILED")
        raise


# ===========================================================
#   SAFE INSERT → NO DUPLICATION (helper)
# ===========================================================
def insert_or_ignore(table, df_subset, conn):
    if df_subset is None or df_subset.shape[0] == 0:
        return
    cols = df_subset.columns.tolist()
    placeholders = ",".join("?" * len(cols))
    sql = f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
    try:
        conn.executemany(sql, df_subset.values.tolist())
        conn.commit()
    except Exception:
        logging.exception(f"insert_or_ignore FAILED for {table}")
        raise

# ===========================================================
#       DIMENSIONS LOADING (kept behavior; fallback on error)
# ===========================================================
def load_dimensions(df, conn, refresh=True):
    dim_tables = {
        "dim_female":  ["female_id","female_age","female_bmi","amh_level","fsh_level","afc"],
        "dim_male":    ["male_id","male_age","male_factor","semen_count_mill_per_ml",
                        "motility_percent","morphology_percent"],
        "dim_protocol":["protocol_id","protocol_type","stimulation_days",
                        "total_fsh_dose","trigger_type","recommended_protocol"],
        "dim_outcome": ["outcome_id","risk_level","response_type",
                        "suggested_waiting_period_days","failure_reason"],
        "dim_embryo":  ["embryo_id","fresh_et_stage","grading","class_a_rate"]
    }

    for table, cols in dim_tables.items():
        subset = df[cols].drop_duplicates()
        try:
            if refresh:
                # try the original behavior using pandas.to_sql (replace)
                subset.to_sql(table, conn, if_exists="replace", index=False)
            else:
                # non-refresh: safe insert-or-ignore in batch
                insert_or_ignore(table, subset, conn)
            logging.info(f"{table}: {len(subset)} processed.")
        except sqlite3.IntegrityError:
            # fallback: try to insert rows one by one with INSERT OR IGNORE to avoid UNIQUE constraint crash
            logging.warning(f"{table}: IntegrityError on bulk insert — falling back to row-by-row INSERT OR IGNORE")
            placeholders = ",".join("?" * len(cols))
            columns = ",".join(cols)
            sql = f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})"
            cur = conn.cursor()
            for row in subset.values.tolist():
                try:
                    cur.execute(sql, row)
                except Exception:
                    logging.exception(f"{table}: failed to insert row {row}")
            conn.commit()
        except Exception:
            logging.exception(f"load_dimensions failed for {table}")
            raise

# ===========================================================
#       DIM_TIME
# ===========================================================
def build_dim_time(df, conn):
    try:
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
            conn.execute("""
                INSERT OR IGNORE INTO dim_time
                (full_date, day, month, month_name, quarter, year, week)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, tuple(row))
        conn.commit()
        logging.info("build_dim_time done.")
    except Exception:
        logging.exception("build_dim_time FAILED")
        raise

# ===========================================================
#              FACT TABLES
# ===========================================================
def load_fact_tables(df, conn):
    try:
        time_df = pd.read_sql("SELECT time_id, full_date FROM dim_time;", conn)
        date_to_id = dict(zip(time_df["full_date"], time_df["time_id"]))

        # Fill missing fact numeric columns with 0
        fact_needed = [
            "e2_on_trigger","endometrium_thickness","follicles_18mm",
            "gv_count","injected_m2","fertilized_oocytes",
            "cleavage_d3","blastocyst_d5","good_embryos"
        ]
        for col in fact_needed:
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

        # ---------------- FACT TRANSFER ----------------
        if all(col in df.columns for col in [
            "case_id","transfer_time_id","doctor_id","embryos_transferred"
        ]):
            tmp = df.drop_duplicates(subset=["case_id"]).copy()
            tmp["transfer_time_fk"] = tmp["transfer_time_id"].map(date_to_id)
            fact_transfer = tmp[[
                "case_id","transfer_time_fk","doctor_id",
                "embryos_transferred","pregnancy_test_result",
                "clinical_pregnancy","live_birth",
                "outcome_id","success_probability_score"
            ]]
            insert_or_ignore("fact_transfer", fact_transfer, conn)

        # ---------------- FACT TRANSFER EMBRYO ----------------
        try:
            existing_transfer = pd.read_sql("SELECT transfer_sk, case_id FROM fact_transfer;", conn)
            if not existing_transfer.empty and "embryo_id" in df.columns:
                df_merge = df.merge(existing_transfer, on="case_id", how="inner")
                fact_embryo = df_merge[["transfer_sk","embryo_id"]].drop_duplicates()
                insert_or_ignore("fact_transfer_embryo", fact_embryo, conn)
        except Exception:
            logging.exception("fact_transfer_embryo skipped.")
    except Exception:
        logging.exception("load_fact_tables FAILED")
        raise

# ===========================================================
#                    MAIN ETL
# ===========================================================
def run_full_etl(refresh=True):
    logging.info("===== ETL STARTED =====")
    try:
        if refresh:
            run_schema_sql()

        df = load_raw_df()
        df = clean_data(df)

        # Required columns check
        check_required(df)

        # Normalize MII/M2 naming (safeguard)
        rename_map = {"mii_count": "m2_count", "injected_mii": "injected_m2"}
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        # IDs and optional fields
        df = handle_ids(df)

        conn = sqlite3.connect(STAR_DB)

        # Map doctor (auto inc) and recommendation handling
        df = map_doctor_ids(df, conn)

        # Dimensions
        load_dimensions(df, conn, refresh=refresh)

        # Time dimension
        build_dim_time(df, conn)

        # Fact tables
        load_fact_tables(df, conn)

        conn.close()
        logging.info("ETL COMPLETED SUCCESSFULLY.")
        print("ETL Done ✔")
        return True
    except Exception as e:
        logging.exception("run_full_etl FAILED")
        print(f"ETL FAILED: {e}")
        return False

# ===========================================================
#                      RUN
# ===========================================================
if __name__ == "__main__":
    run_full_etl(refresh=False)
