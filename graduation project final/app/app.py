import os
from flask import Flask, render_template,  request, redirect, url_for
from datetime import datetime
import json
import sqlite3


BASE_DIR = os.path.dirname(os.path.abspath(__file__))    # .../app/
PROJECT_ROOT = os.path.dirname(BASE_DIR)                 # .../graduation promax/

# Templates & static (already used in your app)
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

# Data folders (for JSON + ETL later)
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
RAW_JSON_DIR = os.path.join(DATA_DIR, "raw_json")
STAGING_DIR = os.path.join(DATA_DIR, "staging")          # (for staging.db later)
FINAL_DB_DIR = os.path.join(DATA_DIR, "final")           # (for ivf_star_schema.db)

# Create directories if they don't exist
os.makedirs(RAW_JSON_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(FINAL_DB_DIR, exist_ok=True)

# ====== PATH للـ warehouse_final ======
WAREHOUSE_DB = os.path.join(DATA_DIR, "warehouse_final", "ivf_star_schema.db")

# ====  INITIALIZE FLASK  ====
app = Flask(__name__,
            template_folder=TEMPLATES_DIR,
            static_folder=STATIC_DIR)

# ====  ROUTES  ====
@app.route('/')
def home():
    return render_template('index.html')   # Login

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')  # Dashboard

@app.route('/patients')
def patients():
    DB_PATH = r"E:\work\DEPI\graduation promax\data\warehouse_final\ivf_star_schema.db"

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            f.female_id AS patient_id
        FROM dim_female f
        LIMIT 5;     -- 👈 ONLY SHOW 5 PATIENTS
    """)
    rows = cursor.fetchall()
    conn.close()

    data = []
    for row in rows:
        data.append({
            "patient_id": row[0],
            "last_visit": "-",
            "status": "Unknown",
            "next_appointment":"-",
        })

    return render_template('patients.html', data=data)


@app.route('/ivf_cycles')
def ivf_cycles():
    return render_template('ivf_cycles.html')

@app.route('/reports')
def reports():
    return render_template('reports.html')

@app.route('/settings')
def settings():
    return render_template('settings.html')

@app.route('/admin_page')
def admin():
    return render_template('admin_page.html')

@app.route('/add_patient', methods=['GET', 'POST'])
def add_patient():
    if request.method == 'POST':
        # 1) Collect data from form
        patient_data = {
            # ===== Basic Info =====
            "patient_id": request.form.get("patient_id"),  # Female ID
            "case_id": request.form.get("case_id"),
            "doctor_id": request.form.get("doctor_id"),
            "male_id": request.form.get("male_id"),        # <-- ADDED

            # ===== Female Data =====
            "female_age": request.form.get("female_age"),
            "female_bmi": request.form.get("female_bmi"),
            "amh_level": request.form.get("amh_level"),
            "fsh_level": request.form.get("fsh_level"),
            "afc": request.form.get("afc"),

            # ===== Male Data =====
            "male_age": request.form.get("male_age"),
            "male_factor": request.form.get("male_factor"),
            "semen_count_mill_per_ml": request.form.get("semen_count_mill_per_ml"),
            "motility_percent": request.form.get("motility_percent"),
            "morphology_percent": request.form.get("morphology_percent"),

            # ===== IVF Cycle Data =====
            "protocol_id": request.form.get("protocol_id"),
            "e2_on_trigger": request.form.get("e2_on_trigger"),
            "endometrium_thickness": request.form.get("endometrium_thickness"),
            "follicles_18mm": request.form.get("follicles_18mm"),
            "retrieved_oocytes": request.form.get("retrieved_oocytes"),
            "injected_mii": request.form.get("injected_mii"),
            "fertilized_oocytes": request.form.get("fertilized_oocytes"),
            "blastocyst_d5": request.form.get("blastocyst_d5"),
            "good_embryos": request.form.get("good_embryos"),

            # ===== Outcome =====
            "risk_level": request.form.get("risk_level"),
            "response_type": request.form.get("response_type"),
            "failure_reason": request.form.get("failure_reason"),

            # ===== Auto Timestamp =====
            "saved_at": datetime.now().isoformat()
        }

        # 2) Save as JSON in data/raw_json
        filename = f"{patient_data['patient_id']}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        filepath = os.path.join(RAW_JSON_DIR, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(patient_data, f, indent=4, ensure_ascii=False)

        # 3) Redirect to Patients Page
        return redirect(url_for('patients'))

    # GET → Render Form
    return render_template('add_patient.html')


@app.route('/add_ivf_cycle', methods=['GET', 'POST'])
def add_ivf_cycle():
    if request.method == 'POST':
        # نجمع البيانات من الفورم:
        cycle_data = {
            "patient_id": request.form.get("patient_id"),
            "case_id": request.form.get("case_id"),
            "protocol_id": request.form.get("protocol_id"),
            "cycle_start_time": request.form.get("cycle_start_time"),
            "e2_on_trigger": request.form.get("e2_on_trigger"),
            "endometrium_thickness": request.form.get("endometrium_thickness"),
            "retrieved_oocytes": request.form.get("retrieved_oocytes"),
            "injected_mii": request.form.get("injected_mii"),
            "fertilized_oocytes": request.form.get("fertilized_oocytes"),
            "blastocyst_d5": request.form.get("blastocyst_d5"),
            "good_embryos": request.form.get("good_embryos"),

            "saved_at": datetime.now().isoformat()
        }

        # نحفظه في RAW_JSON_DIR فقط (مفيش staging هنا)
        filename = f"{cycle_data['case_id']}_CYCLE_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        filepath = os.path.join(RAW_JSON_DIR, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(cycle_data, f, indent=4, ensure_ascii=False)

        return redirect(url_for('patients'))

    return render_template('add_ivf_cycle.html')



@app.route('/patient/<female_id>')
def patient_profile(female_id):
    conn = sqlite3.connect(WAREHOUSE_DB)
    cursor = conn.cursor()

    # ------------------------
    # 🟣 1) Basic Patient Info
    # ------------------------
    cursor.execute("""
        SELECT f.cycle_sk, f.case_id, df.female_id, df.female_age, df.female_bmi,
               dm.male_id, dm.male_age, dm.male_factor,
               d.doctor_recommendation, o.risk_level, o.response_type
        FROM fact_ivf_cycle f
        LEFT JOIN dim_female df ON f.female_id = df.female_id
        LEFT JOIN dim_male dm ON f.male_id = dm.male_id
        LEFT JOIN dim_doctor d ON f.doctor_id = d.doctor_id
        LEFT JOIN dim_outcome o ON f.outcome_id = o.outcome_id
        WHERE df.female_id = ?
        ORDER BY f.cycle_sk DESC
        LIMIT 1
    """, (female_id,))
    basic_info = cursor.fetchone()

    if not basic_info:
        return f"No patient found for female_id {female_id}"

    case_id = basic_info[1]   # مهم عشان نستخدمه تحت

    # ------------------------
    # 🔵 2) ONLY 1 Latest Cycle
    # ------------------------
    cursor.execute("""
        SELECT 
            t.full_date,
            f.retrieved_oocytes,
            f.blastocyst_d5,
            o.risk_level
        FROM fact_ivf_cycle f
        LEFT JOIN dim_time t   ON f.cycle_start_time_id = t.time_id
        LEFT JOIN dim_outcome o ON f.outcome_id = o.outcome_id
        WHERE f.female_id = ?
        ORDER BY f.cycle_sk DESC
        LIMIT 1
    """, (female_id,))
    latest_cycle = cursor.fetchone()

    # ------------------------
    # 🟠 3) Latest Transfer (use case_id)
    # ------------------------
    cursor.execute("""
        SELECT t.full_date, ft.embryos_transferred, ft.pregnancy_test_result,
               ft.live_birth, o.response_type
        FROM fact_transfer ft
        LEFT JOIN dim_time t ON ft.transfer_time_fk = t.time_id
        LEFT JOIN dim_outcome o ON ft.outcome_id = o.outcome_id
        WHERE ft.case_id = ?
        ORDER BY ft.transfer_sk DESC
        LIMIT 1
    """, (case_id,))
    transfer = cursor.fetchone()

    conn.close()

    # ------------------------
    # 📦 Send to HTML
    # ------------------------
    data = {
        "basic": {
            "case_id": basic_info[1],
            "female_id": basic_info[2],
            "female_age": basic_info[3],
            "female_bmi": basic_info[4],
            "male_id": basic_info[5],
            "male_age": basic_info[6],
            "male_factor": basic_info[7],
            "doctor_recommendation": basic_info[8],
            "risk_level": basic_info[9],
            "response_type": basic_info[10],
        },
        "latest_cycle": {
            "date": latest_cycle[0] if latest_cycle else None,
            "retrieved": latest_cycle[1] if latest_cycle else None,
            "blast_d5": latest_cycle[2] if latest_cycle else None,
            "status": latest_cycle[3] if latest_cycle else None,
        } if latest_cycle else None,
        "transfer": {
            "date": transfer[0] if transfer else None,
            "embryos_transferred": transfer[1] if transfer else None,
            "preg_test": transfer[2] if transfer else None,
            "live_birth": transfer[3] if transfer else None,
            "outcome": transfer[4] if transfer else None,
        } if transfer else None
    }

    return render_template('patient_profile.html', data=data)

# ====  RUN APP  ====
if __name__ == '__main__':
    app.run(debug=False)


