import logging
import sqlite3
import pandas as pd
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    filename="etl_ivf.log",
)

RAW_DB = r"E:\work\DEPI\Graduation Project\Graduation-Project\Source Code\ETL\ivf_database.db"
STAR_DB = r"E:\work\DEPI\Graduation Project\ivf_star_schema.db"
SCHEMA_SQL = r"E:\work\DEPI\Graduation Project\create_star_schema.sql"

# ---------- Helpers ----------

def run_schema_sql():
    conn = sqlite3.connect(STAR_DB)
    with open(SCHEMA_SQL, "r", encoding="utf-8") as f:
        sql = f.read()
    conn.executescript(sql)
    conn.commit()
    conn.close()
    logging.info("Star schema tables created / ensured.")


def load_raw_df():
    conn = sqlite3.connect(RAW_DB)
    df = pd.read_sql("SELECT * FROM ivf_patients;", conn)
    conn.close()
    logging.info(f"Loaded {len(df)} rows from raw.")
    return df


def basic_cleaning(df: pd.DataFrame) -> pd.DataFrame:
    # drop exact duplicate rows
    before = len(df)
    df = df.drop_duplicates()
    logging.info(f"Dropped {before - len(df)} duplicate rows.")

    # strip strings
    str_cols = df.select_dtypes(include="object").columns
    for c in str_cols:
        df[c] = df[c].astype(str).str.strip()

    # simple missing handling examples (you تقدري تعدليها)
    num_cols = df.select_dtypes(include=["float", "int"]).columns
    for c in num_cols:
        if df[c].isna().any():
            df[c] = df[c].fillna(df[c].median())

    cat_cols = [c for c in str_cols if c not in ["case_id"]]
    for c in cat_cols:
        if df[c].isna().any():
            df[c] = df[c].fillna("Unknown")

    return df


# ---------- DIM Loaders ----------

def load_dim_female(df: pd.DataFrame, conn: sqlite3.Connection):
    cols = ["female_id", "female_age", "female_bmi", "amh_level", "fsh_level", "afc"]
    dim = df[cols].drop_duplicates().copy()
    dim.to_sql("dim_female", conn, if_exists="replace", index=False)
    logging.info(f"dim_female rows: {len(dim)}")


def load_dim_male(df, conn):
    cols = [
        "male_id",
        "male_age",
        "male_factor",
        "semen_count_mill_per_ml",
        "motility_percent",
        "morphology_percent",
    ]
    dim = df[cols].drop_duplicates().copy()
    dim.to_sql("dim_male", conn, if_exists="replace", index=False)
    logging.info(f"dim_male rows: {len(dim)}")


def load_dim_protocol(df, conn):
    # لو مفيش recommended_protocol في الـ raw استخدمي doctor_recommendation كـ تقريب
    dim = df[
        [
            "protocol_id",
            "protocol_type",
            "stimulation_days",
            "total_fsh_dose",
            "trigger_type",
        ]
    ].drop_duplicates().copy()
    if "recommended_protocol" in df.columns:
        dim["recommended_protocol"] = df["recommended_protocol"]
    else:
        dim["recommended_protocol"] = df["doctor_recommendation"]
    dim = dim.drop_duplicates(subset=["protocol_id"])
    dim.to_sql("dim_protocol", conn, if_exists="replace", index=False)
    logging.info(f"dim_protocol rows: {len(dim)}")


def load_dim_doctor(df, conn):
    dim = df[["doctor_id", "doctor_recommendation"]].drop_duplicates().copy()
    dim.to_sql("dim_doctor", conn, if_exists="replace", index=False)
    logging.info(f"dim_doctor rows: {len(dim)}")


def load_dim_outcome(df, conn):
    cols = [
        "outcome_id",
        "risk_level",
        "response_type",
        "suggested_waiting_period_days",
        "failure_reason",
    ]
    dim = df[cols].drop_duplicates().copy()
    dim.to_sql("dim_outcome", conn, if_exists="replace", index=False)
    logging.info(f"dim_outcome rows: {len(dim)}")


def build_time_dim(df, conn):
    # نفترض إن et_date موجود في الـ raw
    dates = pd.to_datetime(df["et_date"])
    dim = pd.DataFrame({"full_date": dates.drop_duplicates().sort_values()})
    dim["day"] = dim["full_date"].dt.day
    dim["month"] = dim["full_date"].dt.month
    dim["month_name"] = dim["full_date"].dt.month_name()
    dim["quarter"] = dim["full_date"].dt.quarter
    dim["year"] = dim["full_date"].dt.year
    dim["week"] = dim["full_date"].dt.isocalendar().week.astype(int)

    # نسيب time_id auto
    dim.to_sql("dim_time", conn, if_exists="replace", index=False)
    logging.info(f"dim_time rows: {len(dim)}")


def load_dim_embryo(df, conn):
    # مثال بسيط – عدلي حسب الـ data dictionary
    if "embryo_id" not in df.columns:
        logging.warning("No embryo_id in raw – dim_embryo will be empty.")
        return

    dim = df[["embryo_id"]].drop_duplicates().copy()
    dim["fresh_et_stage"] = "Unknown"
    dim["grading"] = "Unknown"

    if "good_embryos" in df.columns and "embryos_transferred" in df.columns:
        # class_a_rate ~ نسبة الأجنة الجيدة من المنقولة
        num = df["good_embryos"].fillna(0)
        den = df["embryos_transferred"].replace(0, pd.NA)
        dim["class_a_rate"] = (num / den).fillna(0)
    else:
        dim["class_a_rate"] = 0.0

    dim = dim.drop_duplicates(subset=["embryo_id"])
    dim.to_sql("dim_embryo", conn, if_exists="replace", index=False)
    logging.info(f"dim_embryo rows: {len(dim)}")


# ---------- FACT Loaders ----------

def load_fact_ivf_cycle(df, conn):
    # نحتاج map من full_date → time_id
    time_df = pd.read_sql("SELECT time_id, full_date FROM dim_time;", conn)
    time_df["full_date"] = pd.to_datetime(time_df["full_date"])
    tmap = dict(zip(time_df["full_date"].dt.strftime("%Y-%m-%d"), time_df["time_id"]))

    fact = df.copy()
    fact["cycle_start_time_id"] = pd.to_datetime(fact["et_date"]).dt.strftime("%Y-%m-%d").map(tmap)

    cols = [
        "case_id",
        "female_id",
        "male_id",
        "protocol_id",
        "doctor_id",
        "outcome_id",
        "cycle_start_time_id",
        "e2_on_trigger",
        "endometrium_thickness",
        "follicles_18mm",
        "retrieved_oocytes",
        "m2_count",
        "gv_count",
        "injected_m2",
        "fertilized_oocytes",
        "fertilization_rate",
        "cleavage_d3",
        "blastocyst_d5",
        "good_embryos",
    ]
    fact = fact[cols].drop_duplicates(subset=["case_id"])
    fact.to_sql("fact_ivf_cycle", conn, if_exists="replace", index=False)
    logging.info(f"fact_ivf_cycle rows: {len(fact)}")


def load_fact_transfer(df, conn):
    # نستخدم تاني نفس time_dim
    time_df = pd.read_sql("SELECT time_id, full_date FROM dim_time;", conn)
    time_df["full_date"] = pd.to_datetime(time_df["full_date"])
    tmap = dict(zip(time_df["full_date"].dt.strftime("%Y-%m-%d"), time_df["time_id"]))

    fact = df.copy()
    fact["transfer_time_id"] = pd.to_datetime(fact["et_date"]).dt.strftime("%Y-%m-%d").map(tmap)

    cols = [
        "cycle_id",  # لو مش موجود استبدليها بـ case_id وبعدين نعمل join مع fact_ivf_cycle
        "doctor_id",
        "transfer_time_id",
        "embryos_transferred",
        "pregnancy_test_result",
        "clinical_pregnancy",
        "live_birth",
        "outcome_id",
        "success_probability_score",
    ]
    existing_cols = [c for c in cols if c in fact.columns]
    fact = fact[existing_cols].copy()

    # مؤقتاً نسيب cycle_sk يتولد Auto عن طريق الجدول
    fact.to_sql("fact_transfer", conn, if_exists="replace", index=False)
    logging.info(f"fact_transfer rows: {len(fact)}")


def run_full_etl():
    logging.info("===== ETL START =====")
    run_schema_sql()

    df = load_raw_df()
    df = basic_cleaning(df)

    conn = sqlite3.connect(STAR_DB)

    try:
        load_dim_female(df, conn)
        load_dim_male(df, conn)
        load_dim_protocol(df, conn)
        load_dim_doctor(df, conn)
        load_dim_outcome(df, conn)
        build_time_dim(df, conn)
        load_dim_embryo(df, conn)
        load_fact_ivf_cycle(df, conn)
        load_fact_transfer(df, conn)

        logging.info("ETL finished successfully.")
    except Exception as e:
        logging.exception(f"ETL failed: {e}")
        raise
    finally:
        conn.close()
        logging.info("===== ETL END =====")


if __name__ == "__main__":
    run_full_etl()
