from flask import Flask, jsonify
import os
import pandas as pd
from flask import Flask, send_file

app = Flask(__name__)

# Route to get all users
@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    return jsonify("health check OK!!")

@app.route('/merge_csv', methods=['GET']) #merge all the input csv files without column check
def merge_csv():
    # Set the directory path
    directory_path = "C:\\Users\\abhis\\Desktop\\CloudBuilders\\myauto_iq\\sample_lead_data"

    # Initialize an empty list to store the data frames
    dfs = []

# Loop through all files in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory_path, filename)
            df = pd.read_csv(file_path)
            dfs.append(df)

    # Concatenate all data frames into a single data frame
    merged_df = pd.concat(dfs, ignore_index=True)

    # Save the merged data frame to a new CSV file
    output_file = 'newdata.csv'
    merged_df.to_csv(output_file, index=False)

    # Return the merged CSV file as a download
    #return send_file(output_file, as_attachment=True)

    # Return a success message
    return jsonify({"message": "CSV files merged successfully."})

@app.route('/merge_csv_1', methods=['GET']) #merge all the input csv after column check
def merge_csv_check():
    # Set the directory path
    directory_path = "C:\\Users\\abhis\\Desktop\\CloudBuilders\\myauto_iq\\sample_lead_data"

    # Initialize an empty list to store the data frames
    dfs = []

    # Get the column names from the first CSV file
    first_file = None
    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
            first_file = os.path.join(directory_path, filename)
            break

    if first_file:
        first_df = pd.read_csv(first_file)
        expected_columns = list(first_df.columns)

        # Loop through all files in the directory
        for filename in os.listdir(directory_path):
            if filename.endswith('.csv'):
                file_path = os.path.join(directory_path, filename)
                df = pd.read_csv(file_path)

                # Check if the column names match
                if list(df.columns) != expected_columns:
                    return jsonify({"error": "Column names do not match across all CSV files."}), 400

                dfs.append(df)

        # Concatenate all data frames into a single data frame
        merged_df = pd.concat(dfs, ignore_index=True)

        # Save the merged data frame to a new CSV file
        output_file = 'newdata.csv'
        merged_df.to_csv(output_file, index=False)

        # Return a success message
        return jsonify({"message": "CSV files merged successfully."})
    else:
        return jsonify({"error": "No CSV files found in the specified directory."}), 404


if __name__ == '__main__':
    app.run(debug=True)



#------------------------------------------------------------------------

    # C:\Users\abhis\Desktop\CloudBuilders\myauto_iq\sample_lead_data
    # sample_lead_data