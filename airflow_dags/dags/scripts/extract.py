import pandas as pd

def extract_data():
    input_path = '/opt/airflow/dags/elysium_large_synthetic_ivf_10000.csv'  
    output_path = '/opt/airflow/dags/temp_data/extracted_data.csv'       
    
    df = pd.read_csv(input_path)
    df.to_csv(output_path, index=False)
    print(f"Data extracted and saved to {output_path}")

if __name__ == "__main__":
    extract_data()
