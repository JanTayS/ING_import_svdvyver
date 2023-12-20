import os
import re
import pandas as pd
import shutil
from sqlalchemy import create_engine

# Directory containing the CSV files
todo_folder = os.path.join(os.getcwd(), "todo")
done_folder = os.path.join(os.getcwd(), "done")
print(todo_folder)

# Database connection parameters
server = '196.147.195.12'  # e.g., 'localhost\sqlexpress'
database = 'Financial'
username = 'dbuser'
password = 'BuurMan$322'
# For Windows authentication, use "Trusted_Connection=yes;" in the connection string

# Create a database engine
# For pyodbc
engine = create_engine(f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=SQL+Server')
# For pymssql
# engine = create_engine(f'mssql+pymssql://{username}:{password}@{server}/{database}')

# List all .csv files in the directory
csv_files = [file for file in os.listdir(todo_folder) if file.endswith('.csv')]


def extract_ing_id(text):
    match = re.search(r'ING-id:\s*([0-9X]+)', text)
    if match:
        return match.group(1)
    return None


# Process each CSV file
for csv_file in csv_files:
    file_path = os.path.join(todo_folder, csv_file)
    df = pd.read_csv(file_path, delimiter=';')

    # Filter rows that contain 'ING-id:' in 'Mededeling begunstigde'
    filter_mask = df['Mededeling begunstigde'].str.contains('ING-id:', na=False)
    ing_id_rows = df[filter_mask].copy()

    # Extract ING IDs and store them in a new column
    ing_id_rows['ING_ID'] = ing_id_rows['Mededeling begunstigde'].apply(extract_ing_id)

    # Create an empty DataFrame to store the final results
    final_df = pd.DataFrame()

    # Search for occurrences of each ID and keep those rows
    for id in ing_id_rows['ING_ID'].unique():
        id_mask = df.apply(lambda row: row.astype(str).str.contains(id).any(), axis=1)
        id_rows = df[id_mask].copy()  # Use copy to create a separate DataFrame
        id_rows.loc[:, 'ING_ID'] = id  # Add the ID as a new column using .loc
        final_df = pd.concat([final_df, id_rows], ignore_index=True)

    # Drop the 'Mededeling begunstigde' column
    final_df.drop(columns=['Mededeling begunstigde'], inplace=True)

    # Export the DataFrame to SQL
    table_name = 'ING_import'  # Set your table name
    final_df.to_sql(table_name, con=engine, if_exists='append', index=False)

    # Move the processed CSV file to the 'done' folder
    # shutil.move(file_path, os.path.join(done_folder, csv_file))