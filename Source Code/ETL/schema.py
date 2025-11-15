import pandas as pd
import sqlite3

def check_source_database():
    print("check if there is any problems in the database")
    
    try:
        conn = sqlite3.connect('ivf_database.db')
        
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
        if 'ivf_patients' not in tables['name'].values:
            print("Isn't Exist")
            return False
        
        # rows number
        count = pd.read_sql("SELECT COUNT(*) as count FROM ivf_patients", conn)
        print(f"Found {count['count'][0]} rows in ivf_patients")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

def setup_schema():
    """إنشاء الـ Schema مع تفعيل Foreign Keys"""
    print("Star Schema")
    
    conn = sqlite3.connect('ivf_star_schema.db')
    
    # Activate foreign key in SqlLite
    conn.execute("PRAGMA foreign_keys = ON")
    
    sql_script = """
    -- Dim_Female
    CREATE TABLE IF NOT EXISTS dim_female (
        female_id INTEGER PRIMARY KEY AUTOINCREMENT,
        female_age INTEGER NOT NULL,
        female_bmi REAL,
        amh_level REAL,
        fsh_level REAL,
        afc INTEGER
    );

    -- Dim_Male
    CREATE TABLE IF NOT EXISTS dim_male (
        male_id INTEGER PRIMARY KEY AUTOINCREMENT,
        male_age INTEGER NOT NULL,
        male_factor TEXT,
        semen_count_mill_per_ml REAL,
        motility_percent REAL,
        morphology_percent REAL
    );

    -- Dim_Protocol
    CREATE TABLE IF NOT EXISTS dim_protocol (
        protocol_id INTEGER PRIMARY KEY AUTOINCREMENT,
        protocol_type TEXT NOT NULL,
        stimulation_days INTEGER,
        total_fsh_dose INTEGER,
        trigger_type TEXT,
        recommended_protocol TEXT
    );

    -- Dim_Outcome
    CREATE TABLE IF NOT EXISTS dim_outcome (
        outcome_id INTEGER PRIMARY KEY AUTOINCREMENT,
        risk_level TEXT,
        response_type TEXT,
        suggested_waiting_period_days INTEGER,
        failure_reason TEXT,
        doctor_recommendation TEXT
    );

    -- Fact_IVFCycle
    CREATE TABLE IF NOT EXISTS fact_ivf_cycle (
        cycle_sk INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id TEXT NOT NULL,
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
    """
    
    conn.executescript(sql_script)
    conn.close()
    print("Tables created successfully")

def load_data_safely():
    print("Load the data from ivf_database")
    try:
        source_conn = sqlite3.connect('ivf_database.db')
        df = pd.read_sql('SELECT * FROM ivf_patients', source_conn)
        source_conn.close()
        
        print(f"{len(df)} rows from ivf_database.db")
        
        print("connecting to new database (ivf_star_schema.db)")

        target_conn = sqlite3.connect('ivf_star_schema.db')
        target_conn.execute("PRAGMA foreign_keys = ON")
        
        # load dim tables
        load_dimension_tables(df, target_conn)
        
        # load fact
        load_fact_table(df, target_conn)
        
        target_conn.close()
        print("Data loaded succesfully")
        
    except Exception as e:
        print(f"Error {e}")
        import traceback
        traceback.print_exc()

def load_dimension_tables(df, conn):
    print("Loading Dims...")
    
    # Dim_Female
    female_cols = ['female_age', 'female_bmi', 'amh_level', 'fsh_level', 'afc']
    dim_female = df[female_cols].drop_duplicates().reset_index(drop=True)
    dim_female['female_id'] = dim_female.index + 1
    dim_female.to_sql('dim_female', conn, if_exists='append', index=False)
    print(f" Dim_Female: {len(dim_female)} ")
    
    # Dim_Male
    male_cols = ['male_age', 'male_factor', 'semen_count_mill_per_ml', 'motility_percent', 'morphology_percent']
    dim_male = df[male_cols].drop_duplicates().reset_index(drop=True)
    dim_male['male_id'] = dim_male.index + 1
    dim_male.to_sql('dim_male', conn, if_exists='append', index=False)
    print(f" Dim_Male: {len(dim_male)} ")
    
    # Dim_Protocol
    protocol_cols = ['protocol_type', 'stimulation_days', 'total_fsh_dose', 'trigger_type', 'recommended_protocol']
    dim_protocol = df[protocol_cols].drop_duplicates().reset_index(drop=True)
    dim_protocol['protocol_id'] = dim_protocol.index + 1
    dim_protocol.to_sql('dim_protocol', conn, if_exists='append', index=False)
    print(f"Dim_Protocol: {len(dim_protocol)} ")
    
    # Dim_Outcome
    outcome_cols = ['risk_level', 'response_type', 'suggested_waiting_period_days', 'failure_reason', 'doctor_recommendation']
    dim_outcome = df[outcome_cols].drop_duplicates().reset_index(drop=True)
    dim_outcome['outcome_id'] = dim_outcome.index + 1
    dim_outcome.to_sql('dim_outcome', conn, if_exists='append', index=False)
    print(f" Dim_Outcome: {len(dim_outcome)} ")

def load_fact_table(df, conn):
    print("Loading Fact Table...")
    #Get ids 
    female_df = pd.read_sql('SELECT * FROM dim_female', conn)
    male_df = pd.read_sql('SELECT * FROM dim_male', conn)
    protocol_df = pd.read_sql('SELECT * FROM dim_protocol', conn)
    outcome_df = pd.read_sql('SELECT * FROM dim_outcome', conn)
    
    # Merge data
    merged_df = df.merge(female_df, on=['female_age', 'female_bmi', 'amh_level', 'fsh_level', 'afc'])
    merged_df = merged_df.merge(male_df, on=['male_age', 'male_factor', 'semen_count_mill_per_ml', 'motility_percent', 'morphology_percent'])
    merged_df = merged_df.merge(protocol_df, on=['protocol_type', 'stimulation_days', 'total_fsh_dose', 'trigger_type', 'recommended_protocol'])
    merged_df = merged_df.merge(outcome_df, on=['risk_level', 'response_type', 'suggested_waiting_period_days', 'failure_reason', 'doctor_recommendation'])
    
    
    fact_data = []
    for _, row in merged_df.iterrows():
        fact_data.append({
            'case_id': row['case_id'],
            'female_id': row['female_id'],
            'male_id': row['male_id'],
            'protocol_id': row['protocol_id'],
            'outcome_id': row['outcome_id'],
            'e2_on_trigger': row['e2_on_trigger'],
            'endometrium_thickness': row['endometrium_thickness'],
            'follicles_18mm': row['follicles_18mm'],
            'retrieved_oocytes': row['retrieved_oocytes'],
            'mii_count': row['mii_count'],
            'mi_count': row['mi_count'],
            'gv_count': row['gv_count'],
            'injected_mii': row['injected_mii'],
            'fertilized_oocytes': row['fertilized_oocytes'],
            'fertilization_rate': row['fertilization_rate'],
            'cleavage_d3': row['cleavage_d3'],
            'blastocyst_d5': row['blastocyst_d5'],
            'good_embryos': row['good_embryos'],
            'class_a_rate': row['class_a_rate'],
            'fresh_et_stage': row['fresh_et_stage'],
            'embryos_transferred': row['embryos_transferred'],
            'grading': row['grading'],
            'pregnancy_test_result': row['pregnancy_test_result'],
            'clinical_pregnancy': row['clinical_pregnancy'],
            'live_birth': row['live_birth'],
            'success_probability_score': row['success_probability_score']
        })
    
    # تحميل الجدول الرئيسي
    fact_df = pd.DataFrame(fact_data)
    fact_df.to_sql('fact_ivf_cycle', conn, if_exists='append', index=False)
    print(f"Fact_IVF_Cycle: {len(fact_df)} rows")

def verify_star_schema():
    """التحقق من الـ Star Schema النهائي"""
    print("\nChecking Star Schema...")
    
    conn = sqlite3.connect('ivf_star_schema.db')
    
    try:
        # عرض جميع الجداول
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
        print("Tables:")
        for table in tables['name']:
            count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", conn)
            print(f"   • {table}: {count['count'][0]} rows")
        
        # ✅ الكود المصحح - اسم الجدول الصح
        print("\nSample from Fact_IVF_Cycle with Dimensions:")
        sample = pd.read_sql('''
            SELECT 
                f.cycle_sk, f.case_id,
                fem.female_age, fem.female_bmi,
                m.male_age, m.male_factor,
                p.protocol_type,
                f.pregnancy_test_result, f.clinical_pregnancy
            FROM fact_ivf_cycle f
            JOIN dim_female fem ON f.female_id = fem.female_id
            JOIN dim_male m ON f.male_id = m.male_id
            JOIN dim_protocol p ON f.protocol_id = p.protocol_id
            LIMIT 5
        ''', conn)
        print(sample.to_string(index=False))
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    # أولاً: التحقق من قاعدة البيانات المصدر
    if check_source_database():
        # ثانياً: إنشاء الـ Schema وتحميل البيانات
        setup_schema()
        load_data_safely()
        # ثالثاً: التحقق من النتيجة
        verify_star_schema()
    else:
        print("Cannot continue due to source database issues")