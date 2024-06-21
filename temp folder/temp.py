from flask import Flask, jsonify
import pandas as pd
from fuzzywuzzy import process

app = Flask(__name__)

# Global variables to store dataframes
global_df1 = None
global_df2 = None

# Mock data for illustration
raw_data1 = {
    'Vehicle_Make': ['Toyota', 'Honda', 'Ford'],
    'Vehicle_Model': ['Corolla', 'Civic', 'F-150'],
    'Year': [2010, 2012, 2015]
}

raw_data2 = {
    'Vehicle_Make': ['Toyta', 'Hunda', 'Chevrolet'],
    'Vehicle_Model': ['Corola', 'Civc', 'Malibu'],
    'Owner': ['Alice', 'Bob', 'Charlie']
}

# Endpoint to process data and store in global_df1
@app.route('/api/process_data1', methods=['GET'])
def process_data1():
    global global_df1
    try:
        # Process data (mock data processing here)
        global_df1 = pd.DataFrame(raw_data1)
        return jsonify({'message': 'Dataframe 1 processed and stored globally'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Endpoint to process data and store in global_df2
@app.route('/api/process_data2', methods=['GET'])
def process_data2():
    global global_df2
    try:
        # Process data (mock data processing here)
        global_df2 = pd.DataFrame(raw_data2)
        return jsonify({'message': 'Dataframe 2 processed and stored globally'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Function to perform fuzzy matching on the common columns
def fuzzy_merge(df1, df2, on_columns, threshold=90):
    if df1 is None or df2 is None:
        raise ValueError("One or both dataframes are not initialized.")
    
    # Create a copy of the dataframes to avoid modifying the original ones
    df1_copy = df1.copy()
    df2_copy = df2.copy()

    for col in on_columns:
        df1_copy[col + '_match'] = df1_copy[col].apply(lambda x: process.extractOne(x, df2_copy[col], score_cutoff=threshold))
        df1_copy[col + '_match'] = df1_copy[col + '_match'].apply(lambda x: x[0] if x else None)

    # Merge on the matched columns
    for col in on_columns:
        df2_copy = df2_copy.rename(columns={col: col + '_match'})

    merged_df = pd.merge(df1_copy, df2_copy, on=[col + '_match' for col in on_columns], how='inner')

    # Drop the additional match columns
    for col in on_columns:
        merged_df = merged_df.drop(columns=[col + '_match'])

    return merged_df

# Endpoint to merge global dataframes with fuzzy matching
@app.route('/api/merge_data', methods=['GET'])
def merge_data():
    global global_df1, global_df2
    try:
        # Check if dataframes are not None
        if global_df1 is None:
            return jsonify({'error': 'Dataframe 1 is not processed yet.'}), 400
        if global_df2 is None:
            return jsonify({'error': 'Dataframe 2 is not processed yet.'}), 400

        # Specify the common columns to merge on
        common_columns = ['Vehicle_Make', 'Vehicle_Model']  # Replace with the actual column names

        # Perform the fuzzy merge operation on the global dataframes
        merged_df = fuzzy_merge(global_df1, global_df2, common_columns)

        # Save merged dataframe to CSV file
        merged_df.to_csv('merged_data.csv', index=False)

        # Convert merged dataframe to JSON
        response = merged_df.to_json(orient='records')
        return response
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Endpoint to call the processing and merging endpoints sequentially
@app.route('/api/process_and_merge', methods=['GET'])
def process_and_merge():
    with app.test_client() as client:
        # Call the first processing endpoint
        response1 = client.get('/api/process_data1')
        if response1.status_code != 200:
            return response1

        # Call the second processing endpoint
        response2 = client.get('/api/process_data2')
        if response2.status_code != 200:
            return response2

        # Call the merge endpoint
        response3 = client.get('/api/merge_data')
        return response3

if __name__ == '__main__':
    app.run(debug=True)
