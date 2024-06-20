from flask import Flask, request, jsonify
import os
import pandas as pd

app = Flask(__name__)

# Predefined folder path and custom column names
LEAD_FOLDER_PATH = "C:\\Users\\abhis\\Desktop\\CloudBuilders\\myauto_iq\\sample_lead_data"
CUSTOM_COLUMN_NAMES = ['Dealer_ID', 'LeadStatusType_Name', 'LeadType_Name','LeadType_ID','CustomerCreated_UTC','LeadCreated_UTC','LeadSource_Name','SalesPerson_FirstName','SalesPerson_LastName','First_Name','Last_Name','Middle_Name','Email','Email_Alt','Vehicle_Year','Vehicle_Make','Vehicle_Model','Vehicle_VIN','Vehicle_StockNumber','SoldDate_UTC','DoNotCall','DoNotEmail','DoNotMail']

MAPPER_FOLDER_PATH = "C:\\Users\\abhis\\Desktop\\CloudBuilders\\myauto_iq\\mapper"
CUSTOM_COLUMN_MAPPER = ['Vehicle_Make', 'Vehicle_Model', 'MFG_TYPE', 'MFG_MAKE', 'MFG_POS','MFG_CNT','F11','F12','F13','New_Auto_Class']

#OUTPUT_CSV_PATH = "C:\\Users\\abhis\\Desktop\\CloudBuilders\\myauto_iq\\sample_lead_data"

def collect_csv_files(folder_path):
    """
    Collects all CSV files in the specified folder.
    
    :param folder_path: Path to the folder containing CSV files
    :return: List of file paths to CSV files
    """
    csv_files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]
    return csv_files

def rename_columns(file_paths, custom_column_names):
    """
    Renames the columns of each CSV file with custom column names.
    
    :param file_paths: List of paths to CSV files
    :param custom_column_names: List of custom column names
    :return: List of DataFrames with renamed columns
    """
    dataframes = []
    for file_path in file_paths:
        df = pd.read_csv(file_path)
        if len(df.columns) == len(custom_column_names):
            df.columns = custom_column_names
        else:
            raise ValueError(f"Number of columns in {file_path} does not match the number of custom column names.")
        dataframes.append(df)
    return dataframes

def merge_dataframes(dataframes):
    """
    Merges a list of DataFrames into a single DataFrame.
    
    :param dataframes: List of DataFrames
    :return: A single merged DataFrame
    """
    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df

def fill_missing_values(df, method):
    if method == 'ffill':
        df_filled = df.ffill()
    elif method == 'bfill':
        df_filled = df.bfill()
    else:
        raise ValueError("Unsupported fill method. Please use 'ffill' or 'bfill'.")
    
    return df_filled

@app.route('/process_leads', methods=['POST'])
def process_csv():
    """
    Flask endpoint to process CSV files, rename columns, merge them into a single DataFrame,
    and send a success message.

    """
    try:
    #Process the sample lead

        # Collect all CSV files
        csv_files = collect_csv_files(LEAD_FOLDER_PATH)
        
        # Rename columns of all CSV files
        renamed_dataframes = rename_columns(csv_files, CUSTOM_COLUMN_NAMES)
        
        # Merge all dataframes into a single dataframe
        merged_df = merge_dataframes(renamed_dataframes)

        # Convert data to pandas DataFrames
        df1 = pd.DataFrame(merged_df)

        # Missing value treatment
        converted_df_1 = fill_missing_values(df1, 'ffill')

        # Get first few rows of the DF & convert it to json format
        #data1 = merged_df.head().to_json(orient ='records')

    #Process the mapper
        # Collect the mapper csv
        mapper_file = collect_csv_files(MAPPER_FOLDER_PATH)

        # Rename columns of the csv file
        mapper_df = rename_columns(mapper_file, CUSTOM_COLUMN_MAPPER)

        # Convert data to pandas DataFrames
        df2 = pd.DataFrame(mapper_df)

        # Missing value treatment
        converted_df_2 = fill_missing_values(df2, 'ffill')

        # Get first few rows of the DF & convert it to json format
        #data2 = mapper_df.head().to_json(orient ='records')

        #Send a success message 
        return jsonify({'message':"Data prcoessed!!",
                        #'merged_df': data1, 
                        #'mapper_df': data2
                        }), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    app.run(debug=True)
