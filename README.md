Data Pipeline: Google Sheets to PostgreSQL

This repository contains a collection of Python scripts designed to extract data 
from a Google Sheet, analyze it, identify missing records in a PostgreSQL database, 
and push the missing data back into the database. The pipeline integrates with 
Google Sheets API, PostgreSQL, and Flyway for migrations.

Table of Contents
1. Overview
2. Prerequisites
3. Installation
4. Configuration
5. Scripts Overview
6. How It Works
7. Usage
8. Troubleshooting
9. Contributing
10. License

Overview
The goal of this project is to automate the process of extracting data from a 
Google Sheet, analyzing it for discrepancies, and synchronizing it with a PostgreSQL 
database. The pipeline ensures that missing or new records are identified and 
inserted into the database.

Key features:
- Extract data from Google Sheets using the Google Sheets API.
- Process and transform the data for compatibility with PostgreSQL.
- Compare the extracted data with existing records in the PostgreSQL database.
- Identify and insert missing records into the database.

Prerequisites
Before running the scripts, ensure you have the following installed:
1. Python 3.8+: The scripts are written in Python and require version 3.8 or higher.
2. PostgreSQL: A running PostgreSQL instance with the target database created.
3. Google Cloud Project: A Google Cloud project with the Google Sheets API enabled.
4. Dependencies: Install the required Python libraries using pip.

Installation
1. Clone the repository:
   git clone https://github.com/your-username/google-sheets-to-postgresql.git
   cd google-sheets-to-postgresql

2. Install dependencies:
   pip install -r requirements.txt

Configuration
Google Sheets API
- Create a Google Cloud project and enable the Google Sheets API.
- Download the credentials.json file and place it in the root directory of the project.
- Run the script once to generate the token.json file for authentication.

PostgreSQL Database
- Update the POSTGRES_CONFIG dictionary in the scripts with your PostgreSQL credentials:
  POSTGRES_CONFIG = {
      "dbname": "your_database_name",
      "user": "your_username",
      "password": "your_password",
      "host": "localhost",  # or your database host
      "port": "5432"        # default PostgreSQL port
  }

Environment Variables
You can also use environment variables for sensitive information like database 
credentials and API keys. Example:
export DB_NAME=your_database_name
export DB_USER=your_username
export DB_PASSWORD=your_password
export GOOGLE_SHEET_ID=your_google_sheet_id

Scripts Overview
gsheet.py
- Purpose: Fetches and processes data from a Google Sheet.
- Functionality:
  - Authenticates with the Google Sheets API.
  - Fetches data from a specific range in the Google Sheet.
  - Processes the data (e.g., converts dates, handles missing values).
  - Saves the processed data to an SQLite database (processed_data.db).

compare.py
- Purpose: Identifies missing records by comparing two datasets.
- Functionality:
  - Compares serial_number values in processed_data.db with those in local_data.db.
  - Uses multithreading to speed up the comparison process.
  - Stores missing records in missing_data.db.

dbfetch.py
- Purpose: Identifies missing records without multithreading.
- Functionality:
  - Similar to compare.py, but simpler and without multithreading.
  - Stores missing records in missing_data.db.

migrationtopsql.py
- Purpose: Migrates missing data from SQLite to PostgreSQL.
- Functionality:
  - Connects to the SQLite database (missing_data.db) and PostgreSQL database.
  - Transfers missing records from SQLite to PostgreSQL.
  - Resets the id sequence in the PostgreSQL table to ensure proper auto-increment behavior.

How It Works
1. Data Extraction:
   - gsheet.py fetches and processes data from a Google Sheet and stores it in 
     processed_data.db.

2. Data Analysis:
   - compare.py or dbfetch.py compares the data in processed_data.db with 
     local_data.db to identify missing records.
   - Missing records are stored in missing_data.db.

3. Data Migration:
   - migrationtopsql.py migrates the missing records from missing_data.db to the 
     PostgreSQL database.

Usage
Step 1: Fetch Data from Google Sheets
Run the gsheet.py script to fetch and process data:
python gsheet.py

Step 2: Identify Missing Records
Run either compare.py or dbfetch.py to identify missing records:
python compare.py
# OR
python dbfetch.py

Step 3: Push Missing Data to PostgreSQL
Run the migrationtopsql.py script to migrate missing data to PostgreSQL:
python migrationtopsql.py


