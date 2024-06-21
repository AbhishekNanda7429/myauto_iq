from flask import Flask, jsonify, request
import pandas as pd
from fuzzywuzzy import process
from concurrent.futures import ThreadPoolExecutor
from celery_method.celery_config import make_celery

app = Flask(__name__)

# Flask configuration for Celery
app.config.update(
    CELERY_BROKER_URL='redis://localhost:6379/0',
    CELERY_RESULT_BACKEND='redis://localhost:6379/0'
)

celery = make_celery(app)

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

# Function to process data
def process_data(raw_data):
    return pd.DataFrame(raw_data)

# Endpoint to process data and store in global_df1
@app.route('/api/process_data1', methods=['GET'])
def process_data1():
    global global_df1
    try:
        # Process data
        global_df1 = process_data(raw_data1)
        return jsonify({'message': 'Dataframe 1 processed and stored globally'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Endpoint to process data and store in global_df2
@app.route('/api/process_data2', methods=['GET'])
def process_data2():
    global global_df2
    try:
        # Process data
        global_df2 = process_data(raw_data2)
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

# Function to merge global dataframes with fuzzy matching
def merge_data():
    global global_df1, global_df2
    try:
        # Check if dataframes are not None
        if global_df1 is None:
            raise ValueError('Dataframe 1 is not processed yet.')
        if global_df2 is None:
            raise ValueError('Dataframe 2 is not processed yet.')

        # Specify the common columns to merge on
        common_columns = ['Vehicle_Make', 'Vehicle_Model']  # Replace with the actual column names

        # Perform the fuzzy merge operation on the global dataframes
        merged_df = fuzzy_merge(global_df1, global_df2, common_columns)

        # Save merged dataframe to CSV file
        merged_df.to_csv('merged_data.csv', index=False)

        return merged_df
    except Exception as e:
        raise e

# Celery task to merge data
@celery.task()
def merge_data_task():
    return merge_data().to_json(orient='records')

# Endpoint to merge global dataframes and map columns
@app.route('/api/merge_data', methods=['GET'])
def merge_data_endpoint():
    try:
        merged_df = merge_data()
        response = merged_df.to_json(orient='records')
        return response
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Endpoint to call the processing and merging endpoints sequentially
@app.route('/api/process_and_merge', methods=['GET'])
def process_and_merge():
    try:
        with ThreadPoolExecutor() as executor:
            # Call the first processing endpoint
            future1 = executor.submit(process_data1)
            # Call the second processing endpoint
            future2 = executor.submit(process_data2)
            # Wait for both processing tasks to complete
            future1.result()
            future2.result()
        
        # Call the merge endpoint using Celery
        task = merge_data_task.apply_async()
        result = task.get()  # You may want to return the task ID and let the client check the result later
        
        return result
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
