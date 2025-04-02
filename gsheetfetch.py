import os
import logging
import sqlite3
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from datetime import datetime

# Define Google Drive API scope
SCOPES = ['https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets']

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("script.log"), logging.StreamHandler()]
)

# Authenticate with Google APIs
def authenticate():
    """Authenticate with Google APIs."""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json')
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            script_dir = os.path.dirname(os.path.realpath(__file__))
            credentials_path = os.path.join(script_dir, 'credentials.json')
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    logging.info("Authentication successful.")
    return creds

# Function to fetch data from Google Sheets
def fetch_google_sheet_data(sheet_id, range_name):
    """Fetch data from a specific range in a Google Sheet."""
    creds = authenticate()
    service = build('sheets', 'v4', credentials=creds)
    result = service.spreadsheets().values().get(spreadsheetId=sheet_id, range=range_name).execute()
    values = result.get('values', [])
    logging.info(f"Fetched {len(values)} rows from range '{range_name}' in Google Sheet.")
    return values

# Function to process and transform data
def process_data(data):
    """
    Process the fetched data:
    - Skip the header row.
    - Extract columns A and C.
    - Convert column C dates from dd/mm/yyyy to yyyy-mm-dd hh:mm:ss.
    - Replace empty or missing values with NULL.
    """
    processed_data = []
    for row in data[1:]:  # Skip the header row
        col_a = row[0] if len(row) > 0 and row[0].strip() else None  # Column A
        col_c = row[2] if len(row) > 2 and row[2].strip() else None  # Column C
        
        # Convert date in column C to yyyy-mm-dd hh:mm:ss
        if col_c:
            try:
                date_obj = datetime.strptime(col_c, "%d/%m/%Y")
                formatted_date = date_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                logging.warning(f"Invalid date format in column C: {col_c}")
                formatted_date = None
        else:
            formatted_date = None
        
        # Append processed row, replacing None with "NULL" for SQL compatibility
        processed_data.append([col_a if col_a else "NULL", formatted_date if formatted_date else "NULL"])
    
    logging.info(f"Processed {len(processed_data)} rows.")
    return processed_data

# Main function to execute the script
def main():
    try:
        # Define Google Sheet ID and range
        sheet_id = "15LuTTWYN_ETK0srFueuqXAbrRy53QhKr5x3F5zov08Y"
        range_name = "Clean Record!A2:C"  # Start from row 2, include columns A and C
        
        # Fetch data from the Google Sheet
        raw_data = fetch_google_sheet_data(sheet_id, range_name)
        
        # Process the data
        processed_data = process_data(raw_data)
        
        # Print the processed data (for verification)
        print("Processed Data:")
        for row in processed_data:
            print(row)
        
        # Optionally, save the processed data to an SQLite database
        db_name = "processed_data.db"
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        logging.info(f"Created database: {db_name}")
        
        # Create a table to store the processed data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS clean_record (
                column_a TEXT,
                column_c TEXT
            )
        ''')
        logging.info("Created table 'clean_record' in database.")
        
        # Insert processed data into the table
        cursor.executemany('''
            INSERT INTO clean_record (column_a, column_c)
            VALUES (?, ?)
        ''', processed_data)
        conn.commit()
        logging.info(f"Inserted {len(processed_data)} rows into table 'clean_record'.")
        
        conn.close()
        logging.info("Script execution completed successfully.")
        print("Script execution completed successfully. Check the log file for details.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()