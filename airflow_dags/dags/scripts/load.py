import pandas as pd
import os

def load_data():
    input_path = '/opt/airflow/dags/temp_data/transformed_data.csv'
    output_folder = '/opt/airflow/dags/output'
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    output_path = os.path.join(output_folder, 'clean_data.csv')
    
    df = pd.read_csv(input_path)
    df.to_csv(output_path, index=False)
    print(f"Data loaded to {output_path}")

if __name__ == "__main__":
    load_data()
