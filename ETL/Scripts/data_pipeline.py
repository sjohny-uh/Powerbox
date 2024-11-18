import os
import pandas as pd
import sqlite3
import zipfile
import datetime


#Step O : Check if file exists 
def check_if_file_processed(directory, file_name):
   
    if not os.path.isdir(directory):
        raise ValueError(f"The path '{directory}' is not a valid directory.")
    
    # Extract the base name from the input file
    #base_name = os.path.splitext(file_name)[0]
    zip_file_name = f"{file_name}.zip"
    
    # Check if the .zip file exists in the directory
    if zip_file_name in os.listdir(directory):
        raise ValueError(f"The file '{zip_file_name}' already exists in the directory and has been processed.")
    
    print(f"No .zip file for '{file_name}' found. Proceeding...")

# Step 1: Ingest Data
def ingest_data(file_path):
    
    directory = os.path.dirname(file_path)
    file_name = os.path.basename(file_path)
    check_if_file_processed(directory, file_name)
    
    #Rename the file by adding the date
    today=datetime.datetime.now().strftime("%Y%m%d")
    file_path_date= file_name.split('.')[0]+"_"+today+'.'+file_name.split('.')[1]
    os.chdir(directory)
    os.rename(file_name,file_path_date)
        
    if file_path_date.endswith('.csv'):
        df = pd.read_csv(file_path_date)
    elif file_path_date.endswith('.xlsx'):
        df = pd.read_excel(file_path_date)
    else:
        raise ValueError("Unsupported file format. Please provide a CSV or Excel file.")
    return df,file_path_date
    
# Step 1.1: Validate the file - schema check
def validate_dataframe_columns(dataframe, csv_file_path):

    # Load the CSV file and get the expected column names
    expected_columns = pd.read_csv(csv_file_path).columns.tolist()
    
    # Get the columns of the DataFrame
    actual_columns = dataframe.columns.tolist()
    
    # Check for mismatched columns
    missing_in_dataframe = set(expected_columns) - set(actual_columns)
    extra_in_dataframe = set(actual_columns) - set(expected_columns)
    
    if missing_in_dataframe or extra_in_dataframe:
        error_message = "Column mismatch detected:\n"
        if missing_in_dataframe:
            error_message += f"Missing in DataFrame: {missing_in_dataframe}\n"
        if extra_in_dataframe:
            error_message += f"Extra in DataFrame: {extra_in_dataframe}\n"
        raise ValueError(error_message)
    else:
        print("Columns are valid and match the expected structure.")


# Step 2: Drop columns with missingness > 50%
def drop_high_missingness(df, threshold=0.5):
    missing_percent = df.isnull().mean()
    columns_to_drop = missing_percent[missing_percent > threshold].index
    df = df.drop(columns=columns_to_drop)
    return df
    
# Step 3: Remove outliers using Interquartile Range (IQR)
def remove_outliers(df):
    for column in df.select_dtypes(include=['float64', 'int64']).columns:
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    return df

# Step 4: Check for data inconsistencies
def check_inconsistencies(df):
    # Remove duplicates
    df = df.drop_duplicates()

    # Remove invalid values (e.g., negative values for energy-related columns)
    energy_columns = ['Solar Panels Energy Output (W)', 'Power Consumption (kW)',
                      'Energy Stored in Batteries (kWh)', 'System Load (kW)', 
                      'Battery Capacity (Wh)', 'Inverter Capacity (kW)']
    
    for column in energy_columns:
        if column in df.columns:
            df = df[df[column] >= 0]  # Ensure no negative values for these columns

    return df

# Step 5: Fill remaining missing values
def fill_missing_values(df):
    for column in df.columns:
        if df[column].dtype in ['float64', 'int64']:
            df[column] = df[column].fillna(df[column].median())
        else:
            df[column] = df[column].fillna(df[column].mode()[0])  # Fill categorical with mode
    return df

# Step 6: Load Data into SQLite and Save to CSV
def load_and_save_data(df, db_name, table_name, csv_name, folder):
    # Create folder if it doesn't exist
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    # Load Data into SQLite
    db_path = os.path.join(folder, db_name)
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()

    # Save cleaned data to CSV
    csv_path = os.path.join(folder, csv_name)
    df.to_csv(csv_path, index=False)

    print(f"Data successfully saved to SQLite database at {db_path}")
    print(f"Cleaned data CSV saved at {csv_path}")
    
# Step 7: Archive the input file
def archive_file(input_file_path, output_archive_path=None):
  
    if not os.path.exists(input_file_path):
        raise FileNotFoundError(f"The input file '{input_file_path}' does not exist.")
    
    # Determine the output archive path
    if output_archive_path is None:
        output_archive_path = f"{input_file_path}.zip"
    
    # Create the zip archive
    with zipfile.ZipFile(output_archive_path, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.write(input_file_path, os.path.basename(input_file_path))
    
    print(f"File archived successfully: {output_archive_path}")
    return output_archive_path

# Full Pipeline Function
def data_pipeline(file_path, db_name, table_name, csv_name, schema_file, folder):

    # Step 1: Ingest data
    df,file_path_date = ingest_data(file_path)
    
    # Step 1.1: Validate the file 
    validate_dataframe_columns(df, schema_file)
    
    # Step 2: Drop columns with high missingness
    df = drop_high_missingness(df)
    
    # Step 3: Remove outliers
    df = remove_outliers(df)
    
    # Step 4: Check for inconsistencies
    df = check_inconsistencies(df)
    
    # Step 5: Fill remaining missing values
    df = fill_missing_values(df)
    
    # Step 6: Load data into SQLite and save CSV
    load_and_save_data(df, db_name, table_name, csv_name, folder)
    
    # Step 7: Archive the input file
    archive_file(file_path_date)
    
    print("Data pipeline completed successfully!")
    

file_path = './powerbox_dataset_prototype.csv'  # Or 'solar_energy_data.xlsx'
db_name = 'solar_system.db'
table_name = 'cleaned_solar_data'
csv_name = 'cleaned_solar_data.csv'
folder = '/Powerbox/Clean_data/'
schema_file = 'powerbox_schema.csv'

data_pipeline(file_path, db_name, table_name, csv_name,schema_file, folder)
