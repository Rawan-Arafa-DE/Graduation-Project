-- ============================
-- DROP Tables (full refresh)
-- ============================
DROP TABLE IF EXISTS dim_female;
DROP TABLE IF EXISTS dim_male;
DROP TABLE IF EXISTS dim_protocol;
DROP TABLE IF EXISTS dim_doctor;
DROP TABLE IF EXISTS dim_outcome;
DROP TABLE IF EXISTS dim_embryo;
DROP TABLE IF EXISTS dim_time;
DROP TABLE IF EXISTS fact_ivf_cycle;
DROP TABLE IF EXISTS fact_transfer;
DROP TABLE IF EXISTS fact_transfer_embryo;

-- ============================
-- DIMENSION TABLES
-- ============================

CREATE TABLE dim_female (
    female_id TEXT PRIMARY KEY,
    female_age INTEGER,
    female_bmi REAL,
    amh_level REAL,
    fsh_level REAL,
    afc INTEGER
);

CREATE TABLE dim_male (
    male_id TEXT PRIMARY KEY,
    male_age INTEGER,
    male_factor TEXT,
    semen_count_mill_per_ml REAL,
    motility_percent REAL,
    morphology_percent REAL
);

CREATE TABLE dim_protocol (
    protocol_id TEXT PRIMARY KEY,
    protocol_type TEXT,
    stimulation_days INTEGER,
    total_fsh_dose INTEGER,
    trigger_type TEXT,
    recommended_protocol TEXT,
    UNIQUE (protocol_type, stimulation_days, total_fsh_dose, trigger_type, recommended_protocol)
);

CREATE TABLE dim_doctor (
    doctor_id TEXT PRIMARY KEY,
    doctor_recommendation TEXT,
    UNIQUE (doctor_recommendation)
);

CREATE TABLE dim_outcome (
    outcome_id TEXT PRIMARY KEY,
    risk_level TEXT,
    response_type TEXT,
    suggested_waiting_period_days INTEGER,
    failure_reason TEXT,
    UNIQUE (risk_level, response_type, suggested_waiting_period_days, failure_reason)
);

CREATE TABLE dim_embryo (
    embryo_id TEXT PRIMARY KEY,
    fresh_et_stage TEXT,
    grading TEXT,
    class_a_rate REAL,
    UNIQUE (fresh_et_stage, grading, class_a_rate)
);

CREATE TABLE dim_time (
    time_id INTEGER PRIMARY KEY AUTOINCREMENT,
    full_date TEXT UNIQUE,
    day INTEGER,
    month INTEGER,
    month_name TEXT,
    quarter INTEGER,
    year INTEGER,
    week INTEGER
);

-- ============================
-- FACT TABLES
-- ============================

CREATE TABLE fact_ivf_cycle (
    cycle_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id TEXT UNIQUE,
    female_id TEXT,
    male_id TEXT,
    protocol_id TEXT,
    doctor_id TEXT,
    outcome_id TEXT,
    cycle_start_time_id INTEGER,

    e2_on_trigger REAL,
    endometrium_thickness REAL,
    follicles_18mm INTEGER,
    retrieved_oocytes INTEGER,
    m2_count INTEGER,
    gv_count INTEGER,
    injected_m2 INTEGER,
    fertilized_oocytes INTEGER,

    fertilization_rate REAL,
    cleavage_d3 INTEGER,
    blastocyst_d5 INTEGER,
    good_embryos INTEGER,

    FOREIGN KEY (female_id) REFERENCES dim_female(female_id),
    FOREIGN KEY (male_id) REFERENCES dim_male(male_id),
    FOREIGN KEY (protocol_id) REFERENCES dim_protocol(protocol_id),
    FOREIGN KEY (doctor_id) REFERENCES dim_doctor(doctor_id),
    FOREIGN KEY (outcome_id) REFERENCES dim_outcome(outcome_id),
    FOREIGN KEY (cycle_start_time_id) REFERENCES dim_time(time_id)
);

CREATE TABLE fact_transfer (
    transfer_sk INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id TEXT,
    transfer_time_fk INTEGER,
    doctor_id TEXT,
    embryos_transferred INTEGER,
    pregnancy_test_result TEXT,
    clinical_pregnancy INTEGER,
    live_birth INTEGER,
    outcome_id TEXT,
    success_probability_score REAL,

    FOREIGN KEY (transfer_time_fk) REFERENCES dim_time(time_id),
    FOREIGN KEY (doctor_id) REFERENCES dim_doctor(doctor_id),
    FOREIGN KEY (outcome_id) REFERENCES dim_outcome(outcome_id)
);

CREATE TABLE fact_transfer_embryo (
    transfer_sk INTEGER,
    embryo_id TEXT,

    FOREIGN KEY (transfer_sk) REFERENCES fact_transfer(transfer_sk),
    FOREIGN KEY (embryo_id) REFERENCES dim_embryo(embryo_id)
    PRIMARY KEY (transfer_sk, embryo_id)

);
