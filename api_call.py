from flask import Flask, jsonify
import os
import pandas as pd
from flask import Flask, send_file

app = Flask(__name__)

# Route to get all users
@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    return jsonify("health check OK!!")

@app.route('/merge_csv', methods=['GET'])
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


if __name__ == '__main__':
    app.run(debug=True)



#------------------------------------------------------------------------

    # C:\Users\abhis\Desktop\CloudBuilders\myauto_iq\sample_lead_data
    # sample_lead_data