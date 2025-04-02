import sqlite3
import logging
import os

# Configure logging
logging.basicConfig(
    filename="data_check.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_table_name(db_path):
    """Retrieve the first table name from the given database."""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
        table = cursor.fetchone()
        return table[0] if table else None

def create_missing_db_structure(source_db, missing_db, table_name):
    """Creates a new database with the same structure as the given table."""
    with sqlite3.connect(source_db) as src_conn, sqlite3.connect(missing_db) as dest_conn:
        src_cursor = src_conn.cursor()
        dest_cursor = dest_conn.cursor()
        
        # Get table structure from source database
        src_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_structure = src_cursor.fetchone()
        
        if table_structure and table_structure[0]:
            dest_cursor.execute(table_structure[0])
            logging.info(f"✅ Created missing_data.db with the same table structure as {table_name}.")
        else:
            logging.error(f"❌ Could not retrieve table structure from {table_name}.")

def check_and_store_missing_data():
    local_db = "local_data.db"
    processed_db = "processed_data.db"
    missing_db = "missing_data.db"

    # Get actual table names
    local_table = get_table_name(local_db)
    processed_table = get_table_name(processed_db)
    
    if not local_table or not processed_table:
        logging.error("❌ Could not determine table names from one or both databases.")
        return

    # Ensure missing_data.db exists
    if not os.path.exists(missing_db):
        create_missing_db_structure(local_db, missing_db, local_table)

    # Connect to databases
    local_conn = sqlite3.connect(local_db)
    processed_conn = sqlite3.connect(processed_db)
    missing_conn = sqlite3.connect(missing_db)

    local_cursor = local_conn.cursor()
    processed_cursor = processed_conn.cursor()
    missing_cursor = missing_conn.cursor()

    try:
        # Fetch all values from processed_data.column_a
        processed_cursor.execute(f"SELECT column_a, column_c FROM {processed_table}")
        processed_rows = processed_cursor.fetchall()
        
        missing_entries = []
        for serial_number, manufacture_date in processed_rows:
            # Check if serial_number exists in local_data.serial_number
            local_cursor.execute(f"SELECT COUNT(*) FROM {local_table} WHERE serial_number = ?", (serial_number,))
            exists = local_cursor.fetchone()[0]
            
            if exists == 0:
                # Add to missing data list
                missing_entries.append((serial_number, manufacture_date))

        # Insert missing entries into missing_data.db
        if missing_entries:
            missing_cursor.executemany(
                f"INSERT INTO {local_table} (serial_number, manufacture_date) VALUES (?, ?)", missing_entries
            )
            missing_conn.commit()
            logging.info(f"✅ Added {len(missing_entries)} missing entries to missing_data.db.")
        else:
            logging.info("✅ No missing entries found.")
    except sqlite3.Error as e:
        logging.error(f"❌ SQLite error occurred: {e}")
    finally:
        # Close connections
        local_conn.close()
        processed_conn.close()
        missing_conn.close()
        logging.info("✅ Database connections closed.")

if __name__ == "__main__":
    logging.info("🚀 Script started: Checking for missing data")
    check_and_store_missing_data()
    logging.info("🏁 Script completed.")
