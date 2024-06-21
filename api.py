from flask import Flask, jsonify
import os
import pandas as pd
from fuzzywuzzy import process
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

# Define constants for path and column names
CSV_PATH = "C:\\Users\\abhis\\Desktop\\CloudBuilders\\myauto_iq\\sample_lead_data"  
COLUMN_NAMES = ['Dealer_ID', 'LeadStatusType_Name', 'LeadType_Name','LeadType_ID','CustomerCreated_UTC','LeadCreated_UTC','LeadSource_Name','SalesPerson_FirstName','SalesPerson_LastName','First_Name','Last_Name','Middle_Name','Email','Email_Alt','Vehicle_Year','Vehicle_Make','Vehicle_Model','Vehicle_VIN','Vehicle_StockNumber','SoldDate_UTC','DoNotCall','DoNotEmail','DoNotMail']  

MAPPER_FOLDER_PATH = "C:\\Users\\abhis\\Desktop\\CloudBuilders\\myauto_iq\\mapper"
CUSTOM_COLUMN_MAPPER = ['Vehicle_Make', 'Vehicle_Model', 'MFG_TYPE', 'MFG_MAKE', 'MFG_POS','MFG_CNT','F11','F12','F13','New_Auto_Class']

# Common columns to merge on
common_columns = ['Vehicle_Make', 'Vehicle_Model']

# Global variables to store dataframes
merged_df = None
mapper_df = None

# Function to collect CSV files from a specified path
def collect_csv_files(path):
    csv_files = [file for file in os.listdir(path) if file.endswith('.csv')]
    return csv_files

# Function to rename columns in a dataframe
def rename_columns(df, column_names):
    df.columns = column_names
    return df

# Function to merge CSV files into a single dataframe
def merge_csv_files(path, column_names):
    csv_files = collect_csv_files(path)
    dfs = []
    for file in csv_files:
        file_path = os.path.join(path, file)
        df = pd.read_csv(file_path)
        df = rename_columns(df, column_names)
        #df = drop_nan(df)
        dfs.append(df)
    merged_df = pd.concat(dfs, ignore_index=True)
    return merged_df

# Function to treat missing values
def fill_missing_values(df, method):
    if method == 'ffill':
        df_filled = df.ffill()
    elif method == 'bfill':
        df_filled = df.bfill()
    else:
        raise ValueError("Unsupported fill method. Please use 'ffill' or 'bfill'.")
    
    return df_filled

# Function to drop rows and columns with NaN values
def drop_nan(df):
    df = df.dropna(axis=0, how='any')  # Drop rows with any NaN values
    df = df.dropna(axis=1, how='any')  # Drop columns with any NaN values
    return df

# Function to merge dataframes
def fuzzy_logic(df1, df2, on_column, threshold=90):
    if df1 is None or df2 is None:
        raise ValueError("One or both dataframes are not initialized.")
    
    # Create a copy of the dataframes to avoid modifying the original ones
    df1_copy = df1.copy()
    df2_copy = df2.copy()

    for col in on_column:
        df1_copy[col + '_match'] = df1_copy[col].apply(lambda x: process.extractOne(x, df2_copy[col], score_cutoff=threshold))
        df1_copy[col + '_match'] = df1_copy[col + '_match'].apply(lambda x: x[0] if x else None)

    # Merge on the matched columns
    for col in on_column:
        df2_copy = df2_copy.rename(columns={col: col + '_match'})

    merged_data = pd.merge(df1_copy, df2_copy, on=[col + '_match' for col in on_column], how='inner')

    # Drop the additional match columns
    for col in on_column:
        merged_data = merged_data.drop(columns=[col + '_match'])

    return merged_data

# Function to merge global dataframes with fuzzy matching
def map_data():
    global merged_df,mapper_df
    try:
        # Check if dataframes are not None
        if merged_df is None:
            return jsonify({'error': 'Dataframe 1 is not processed yet.'}), 400
        if mapper_df is None:
            return jsonify({'error': 'Dataframe 2 is not processed yet.'}), 400
        mapped_df = fuzzy_logic(merged_df, mapper_df, common_columns)
        # Save merged dataframe to CSV file
        mapped_df.to_csv('final_output.csv', index=False)

        return mapped_df
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# API endpoint to process CSV files
@app.route('/process_leads', methods=['GET'])
def process_leads():
    global merged_df
    try:
        merged_df = merge_csv_files(CSV_PATH, COLUMN_NAMES)
        merged_df = fill_missing_values(merged_df, 'ffill')  
        # Convert dataframe to JSON for API response
        response = merged_df.to_json(orient='records')
        return response
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# API endpoint to process mapper csv
@app.route('/process_mapper', methods=['GET'])
def process_mapper():
    global mapper_df
    try:
        mapper_df = merge_csv_files(MAPPER_FOLDER_PATH,CUSTOM_COLUMN_MAPPER)
        mapper_df = fill_missing_values(mapper_df, 'bfill')
        # Convert dataframe to JSON for API response
        response = mapper_df.to_json(orient='records')
        return response
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# API endpoint to map the dataframes
@app.route('/map', methods=['GET'])
def map():
    global merged_df,mapper_df
    try:
        final_df = map_data()
        response = final_df.to_json(orient='records')
        return response
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# API endpoint to call all the other APIs
@app.route('/process_all', methods=['GET'])
def process_all():
    try:
        with ThreadPoolExecutor() as executor:
            res1 = executor.submit(process_leads)
            res2 = executor.submit(process_mapper)
            res1.result()
            res2.result()

        res3 = map()

        return res3, 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    
if __name__ == '__main__':
    app.run(debug=True)
