import datetime
import pandas as pd
from bs4 import BeautifulSoup
import requests
import numpy as np
from sqlalchemy import create_engine

def log_progress(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{timestamp} : {message}\n"

    with open('code_log.txt', 'a') as log_file:
        log_file.write(log_entry)

def extract():
    # URL of the webpage
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"

    # Send a GET request to the URL
    response = requests.get(url)

    # Parse the HTML content of the page
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the required table under the heading 'By market capitalization'
    table = soup.find('span', {'id': 'By_market_capitalization'}).find_next('table')
    print(table)
    # Extract data from the table to a Pandas DataFrame
    data = pd.read_html(str(table), header=0)[0]

    # Convert 'Market cap (US$ billion)' column to strings and remove the last character '\n'
    data['Market cap (US$ billion)'] = data['Market cap (US$ billion)'].astype(str).str.rstrip('\n')

    # Typecast the 'Market cap (US$ billion)' values to float
    data['Market cap (US$ billion)'] = data['Market cap (US$ billion)'].astype(float)

    return data

def transform(data):
    # Read exchange rate CSV file from the local directory and convert to dictionary
    exchange_rate_path = "C:\\testing\exchange_rate.csv"
    exchange_rate_df = pd.read_csv(exchange_rate_path, index_col=0)
    exchange_rate = exchange_rate_df.squeeze().to_dict()

    # Add new columns for each currency in the exchange rate file
    for currency, rate in exchange_rate.items():
        column_name = f'MC_{currency}_Billion'
        data[column_name] = np.round(data['Market cap (US$ billion)'] * rate, 2)

    return data

def save_to_csv(file_path, data):
    data.to_csv(file_path, index=False)

# Initial log entry
log_progress("Preliminaries complete. Initiating ETL process")

# Call the extract() function and print the returning data frame
extracted_data = extract()
print(extracted_data)

# Log entry
log_progress("Data extraction complete. Initiating Transformation process")

# Call the transform() function and print the returning data frame
transformed_data = transform(extracted_data)
print("here")
print(transformed_data)

# Log entry
log_progress("Data transformation complete. Initiating Loading process")

# Save transformed data to CSV
csv_file_path = "./transformed_data.csv"
save_to_csv(csv_file_path, transformed_data)
def load_to_sql_server(server, database, transformed_data, table_name):
    # Create a SQL Server connection string
    connection_string = f"mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"

    # Create a SQLAlchemy engine
    engine = create_engine(connection_string)

    # Load the transformed data into SQL Server
    transformed_data.to_sql(table_name, con=engine, index=False, if_exists='replace')

    print(f'Data has been successfully loaded into the table: {table_name}')
your_data = pd.read_csv("transformed_data.csv")  # Replace "your_data.csv" with your actual data file
transformed_data = transform(your_data)


# SQL Server details
SERVER_NAME = 'LAPTOP-EP89R35M\SQLEXPRESS'
DATABASE_NAME = 'xyz'
TABLE_NAME = 'transformed_data'  # Replace with your desired table name

# Call the function to load data into SQL Server
load_to_sql_server(SERVER_NAME, DATABASE_NAME, transformed_data,TABLE_NAME)
                   

                   