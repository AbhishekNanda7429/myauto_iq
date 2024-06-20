from flask import Flask, jsonify
import pandas as pd

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
    'Vehicle_Make': ['Toyota', 'Honda', 'Chevrolet'],
    'Vehicle_Model': ['Corolla', 'Civic', 'Malibu'],
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

# Function to merge dataframes on common columns
def merge_dataframes(df1, df2, on_columns):
    if df1 is None or df2 is None:
        raise ValueError("One or both dataframes are not initialized.")
    merged_df = pd.merge(df1, df2, on=on_columns, how='inner')
    return merged_df

# Endpoint to merge global dataframes and map columns
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

        # Perform the merge operation on the global dataframes
        merged_df = merge_dataframes(global_df1, global_df2, common_columns)

        # Convert merged dataframe to JSON
        response = merged_df.to_json(orient='records')
        return response
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
