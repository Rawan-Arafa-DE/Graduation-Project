import pandas as pd

def transform_data():
    input_path = '/opt/airflow/dags/temp_data/extracted_data.csv'
    output_path = '/opt/airflow/dags/temp_data/transformed_data.csv'
    
    df = pd.read_csv(input_path)
    df.dropna(inplace=True)  
    
    df.to_csv(output_path, index=False)
    print(f"Data transformed and saved to {output_path}")

if __name__ == "__main__":
    transform_data()

