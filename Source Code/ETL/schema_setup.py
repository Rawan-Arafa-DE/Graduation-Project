import sqlite3
import logging

logger = logging.getLogger("schema_setup")

def setup_schema(conn: sqlite3.Connection):
    """
    Create star schema tables (dimensions + fact) ONE TIME ONLY.
    Adds UNIQUE constraints + indexes.
    """
    logger.info("Ensuring star schema (tables, unique constraints, indexes)")

    sql_script = """
    PRAGMA foreign_keys = ON;

    -- ==========================
    -- Female Dimension
    -- ==========================
    CREATE TABLE IF NOT EXISTS dim_female (
        female_id INTEGER PRIMARY KEY AUTOINCREMENT,
        female_age INTEGER NOT NULL,
        female_bmi REAL,
        amh_level REAL,
        fsh_level REAL,
        afc INTEGER,
        UNIQUE(female_age, female_bmi, amh_level, fsh_level, afc)
    );

    -- ==========================
    -- Male Dimension
    -- ==========================
    CREATE TABLE IF NOT EXISTS dim_male (
        male_id INTEGER PRIMARY KEY AUTOINCREMENT,
        male_age INTEGER NOT NULL,
        male_factor TEXT,
        semen_count_mill_per_ml REAL,
        motility_percent REAL,
        morphology_percent REAL,
        UNIQUE(male_age, male_factor, semen_count_mill_per_ml, motility_percent, morphology_percent)
    );

    -- ==========================
    -- Protocol Dimension
    -- ==========================
    CREATE TABLE IF NOT EXISTS dim_protocol (
        protocol_id INTEGER PRIMARY KEY AUTOINCREMENT,
        protocol_type TEXT NOT NULL,
        stimulation_days INTEGER,
        total_fsh_dose INTEGER,
        trigger_type TEXT,
        recommended_protocol TEXT,
        UNIQUE(protocol_type, stimulation_days, total_fsh_dose, trigger_type, recommended_protocol)
    );

    -- ==========================
    -- Outcome Dimension
    -- ==========================
    CREATE TABLE IF NOT EXISTS dim_outcome (
        outcome_id INTEGER PRIMARY KEY AUTOINCREMENT,
        risk_level TEXT,
        response_type TEXT,
        suggested_waiting_period_days INTEGER,
        failure_reason TEXT,
        doctor_recommendation TEXT,
        UNIQUE(risk_level, response_type, suggested_waiting_period_days, failure_reason, doctor_recommendation)
    );

    -- ==========================
    -- Fact Table
    -- ==========================
    CREATE TABLE IF NOT EXISTS fact_ivf_cycle (
        cycle_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id TEXT NOT NULL UNIQUE,
        female_id INTEGER NOT NULL,
        male_id INTEGER NOT NULL,
        protocol_id INTEGER,
        outcome_id INTEGER,
        e2_on_trigger REAL,
        endometrium_thickness REAL,
        follicles_18mm INTEGER,
        retrieved_oocytes INTEGER,
        mii_count INTEGER,
        mi_count INTEGER,
        gv_count INTEGER,
        injected_mii INTEGER,
        fertilized_oocytes INTEGER,
        fertilization_rate REAL,
        cleavage_d3 INTEGER,
        blastocyst_d5 INTEGER,
        good_embryos INTEGER,
        class_a_rate REAL,
        fresh_et_stage TEXT,
        embryos_transferred INTEGER,
        grading TEXT,
        pregnancy_test_result TEXT,
        clinical_pregnancy TEXT,
        live_birth TEXT,
        success_probability_score REAL,
        FOREIGN KEY (female_id) REFERENCES dim_female(female_id),
        FOREIGN KEY (male_id) REFERENCES dim_male(male_id),
        FOREIGN KEY (protocol_id) REFERENCES dim_protocol(protocol_id),
        FOREIGN KEY (outcome_id) REFERENCES dim_outcome(outcome_id)
    );

    CREATE INDEX IF NOT EXISTS idx_fact_female ON fact_ivf_cycle(female_id);
    CREATE INDEX IF NOT EXISTS idx_fact_male ON fact_ivf_cycle(male_id);
    CREATE INDEX IF NOT EXISTS idx_fact_protocol ON fact_ivf_cycle(protocol_id);
    CREATE INDEX IF NOT EXISTS idx_fact_outcome ON fact_ivf_cycle(outcome_id);
    """

    conn.executescript(sql_script)
    conn.commit()
    logger.info("Schema ensured successfully.")
