
import pandas as pd
import sqlite3
import os

print("Start")

try:
   
    csv_path = r"C:\Users\Hp\OneDrive\Desktop\GradProject(Depi)\Graduation-Project\Data\elysium_large_synthetic_ivf_10000 (1).csv"
    print(f"Downloading from: {csv_path}")
    
    df = pd.read_csv(csv_path)
    print(f" {len(df)} rows downloaded")
    
    
    conn = sqlite3.connect('ivf_database.db')
    print("Database created successfully")
    
    
    df.to_sql('ivf_patients', conn, if_exists='replace', index=False)
    print(f" {len(df)} Data transfered to database successfully")
    
    
    result = pd.read_sql('SELECT COUNT(*) as total FROM ivf_patients', conn)
    print(f"for making sure: {result['total'][0]}")
    
    conn.close()
    
except FileNotFoundError:
    print("File isn;t exist")
    print("Data files avaliable")
    os.system("ls -la Data/")
except Exception as e:
    print(f"Error {e}")