import pandas as pd
import sqlite3
import glob
import os
import logging
import uuid

# نستورد السكيمـا لكن مش بنشغلها هنا
from schema_setup import setup_schema  

DB_DEFAULT = "ivf_star_schema.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("etl_pipeline")


# =====================================================================
# Step 1 — Load Parquet or CSV
# =====================================================================
def read_input_files(path_or_pattern: str) -> pd.DataFrame:
    """
    Reads multiple CSV or Parquet files based on a path or wildcard pattern.
    """
    logger.info(f"Reading data from: {path_or_pattern}")

    files = glob.glob(path_or_pattern)
    if not files:
        raise FileNotFoundError("No files found for the given path.")

    dfs = []
    for f in files:
        try:
            if f.endswith(".parquet"):
                df = pd.read_parquet(f)
            elif f.endswith(".csv"):
                df = pd.read_csv(f)
            else:
                logger.warning(f"Unsupported format: {f}")
                continue

            logger.info(f"Loaded {len(df)} rows from {f}")
            dfs.append(df)
        except Exception as e:
            logger.error(f"Failed loading file {f}: {e}")

    if not dfs:
        raise ValueError("No valid dataframes loaded.")

    final_df = pd.concat(dfs, ignore_index=True)
    final_df.columns = [c.strip() for c in final_df.columns]
    return final_df



# =====================================================================
# Step 2 — Data cleaning
# =====================================================================
def sanitize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean dataframe (strip strings, convert numeric columns, remove empty rows)
    """
    df = df.copy()
    df = df.dropna(how="all")

    # Clean text columns
    for col in df.select_dtypes(include="object"):
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)

    # Convert numeric columns
    numeric_cols = [
        'female_age','female_bmi','amh_level','fsh_level','afc','male_age',
        'semen_count_mill_per_ml','motility_percent','morphology_percent',
        'stimulation_days','total_fsh_dose','suggested_waiting_period_days',
        'e2_on_trigger','endometrium_thickness','follicles_18mm','retrieved_oocytes',
        'mii_count','mi_count','gv_count','injected_mii','fertilized_oocytes',
        'fertilization_rate','cleavage_d3','blastocyst_d5','good_embryos',
        'class_a_rate','embryos_transferred','success_probability_score'
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# =====================================================================
# Step 3 — Insert/Upsert into Dimension Tables
# =====================================================================
def insert_dimensions(df: pd.DataFrame, conn: sqlite3.Connection):
    """
    Inserts unique rows into dim tables using INSERT OR IGNORE
    and returns mapping dictionaries for ID resolution.
    """
    logger.info("Loading dimensions...")

    dims = {
        "dim_female": (['female_age', 'female_bmi', 'amh_level', 'fsh_level', 'afc'], "female_id"),
        "dim_male": (['male_age','male_factor','semen_count_mill_per_ml','motility_percent','morphology_percent'], "male_id"),
        "dim_protocol": (['protocol_type','stimulation_days','total_fsh_dose','trigger_type','recommended_protocol'], "protocol_id"),
        "dim_outcome": (['risk_level','response_type','suggested_waiting_period_days','failure_reason','doctor_recommendation'], "outcome_id"),
    }

    dim_maps = {}

    for table, (cols, id_col) in dims.items():
        if not set(cols).issubset(df.columns):
            logger.warning(f"Skipping dim {table} (missing columns)")
            continue

        sub_df = df[cols].drop_duplicates().reset_index(drop=True)
        sub_df = sub_df.where(pd.notnull(sub_df), None)

        placeholders = ",".join(["?"] * len(cols))
        col_list = ",".join(cols)
        sql = f"INSERT OR IGNORE INTO {table} ({col_list}) VALUES ({placeholders})"

        conn.executemany(sql, sub_df.to_records(index=False))

        # Load ids back
        dim_table = pd.read_sql(f"SELECT * FROM {table}", conn)
        merged = sub_df.merge(dim_table, on=cols)
        dim_maps[id_col] = dict(
            zip(
                [tuple(r[c] for c in cols) for _, r in merged.iterrows()],
                merged[id_col].tolist()
            )
        )

        logger.info(f"{table}: {len(sub_df)} unique rows")

    conn.commit()
    return dim_maps



# =====================================================================
# Step 4 — Load Fact Table
# =====================================================================
def load_fact(df: pd.DataFrame, conn: sqlite3.Connection, dim_maps: dict):
    logger.info("Loading fact table...")

    # Build fact rows
    fact_rows = []

    for _, row in df.iterrows():
        female_key = tuple(row[c] for c in ['female_age','female_bmi','amh_level','fsh_level','afc'])
        male_key   = tuple(row[c] for c in ['male_age','male_factor','semen_count_mill_per_ml','motility_percent','morphology_percent'])

        protocol_key = None
        if 'protocol_type' in df.columns:
            protocol_key = tuple(row[c] for c in ['protocol_type','stimulation_days','total_fsh_dose','trigger_type','recommended_protocol'])

        outcome_key = None
        if 'risk_level' in df.columns:
            outcome_key = tuple(row[c] for c in ['risk_level','response_type','suggested_waiting_period_days','failure_reason','doctor_recommendation'])

        fact_rows.append({
            "case_id": row["case_id"],
            "female_id": dim_maps["female_id"].get(female_key),
            "male_id":   dim_maps["male_id"].get(male_key),
            "protocol_id": dim_maps.get("protocol_id", {}).get(protocol_key),
            "outcome_id":  dim_maps.get("outcome_id", {}).get(outcome_key),
            "e2_on_trigger": row.get("e2_on_trigger"),
            "endometrium_thickness": row.get("endometrium_thickness"),
            "retrieved_oocytes": row.get("retrieved_oocytes"),
            "fertilization_rate": row.get("fertilization_rate"),
            "pregnancy_test_result": row.get("pregnancy_test_result"),
            "clinical_pregnancy": row.get("clinical_pregnancy"),
            "live_birth": row.get("live_birth"),
            "success_probability_score": row.get("success_probability_score")
        })

    fact_df = pd.DataFrame(fact_rows)

    # UPSERT
    cols = list(fact_df.columns)
    placeholders = ",".join(["?"] * len(cols))
    assignments = ",".join([f"{c}=excluded.{c}" for c in cols if c != "case_id"])

    sql = f"""
        INSERT INTO fact_ivf_cycle ({",".join(cols)})
        VALUES ({placeholders})
        ON CONFLICT(case_id) DO UPDATE SET {assignments}
    """

    conn.executemany(sql, fact_df.to_records(index=False))
    conn.commit()

    logger.info(f"Inserted/updated {len(fact_df)} fact rows.")
    return len(fact_df)



# =====================================================================
# Step 5 — Main ETL Flow
# =====================================================================
def run_daily_etl(input_pattern: str, db_path: str = DB_DEFAULT):
    logger.info("=== Starting Daily ETL ===")

    df = read_input_files(input_pattern)
    df = sanitize(df)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    dim_maps = insert_dimensions(df, conn)
    fact_rows = load_fact(df, conn, dim_maps)

    conn.close()
    logger.info("=== ETL Completed Successfully ===")
    return fact_rows


if __name__ == "__main__":
    # Example:
    # run_daily_etl("data/raw/*.csv")
    pass
