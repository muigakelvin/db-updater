import sqlite3
import logging
import os
import concurrent.futures

# Configure logging to both file and console
logging.basicConfig(
    filename="data_check.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_message(level, message):
    """Log messages to both file and console."""
    print(message)  # Print to console
    if level == "info":
        logging.info(message)
    elif level == "error":
        logging.error(message)

def get_table_name(db_path):
    """Retrieve the first table name from the given database."""
    log_message("info", f"🔍 Connecting to {db_path} to find table name...")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
        table = cursor.fetchone()
        if table:
            log_message("info", f"✅ Found table: {table[0]} in {db_path}")
        else:
            log_message("error", f"❌ No table found in {db_path}")
        return table[0] if table else None

def create_missing_db_structure(source_db, missing_db, table_name):
    """Creates a new database with the same structure as the given table."""
    log_message("info", f"🔍 Checking table structure in {source_db}...")
    with sqlite3.connect(source_db) as src_conn, sqlite3.connect(missing_db) as dest_conn:
        src_cursor = src_conn.cursor()
        dest_cursor = dest_conn.cursor()
        
        src_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_structure = src_cursor.fetchone()
        
        if table_structure and table_structure[0]:
            dest_cursor.execute(table_structure[0])
            log_message("info", f"✅ Created {missing_db} with the same table structure as {table_name}.")
        else:
            log_message("error", f"❌ Could not retrieve table structure from {table_name}.")

def check_serial_numbers(chunk, local_db, local_table, chunk_id):
    """Checks which serial numbers are missing using multithreading."""
    log_message("info", f"🔍 Checking chunk {chunk_id} with {len(chunk)} serial numbers in {local_db}...")
    missing_serials = []
    
    with sqlite3.connect(local_db) as conn:
        cursor = conn.cursor()
        for serial_number in chunk:
            log_message("info", f"🔎 Checking if serial_number {serial_number} exists in {local_table}...")
            cursor.execute(f"SELECT COUNT(*) FROM {local_table} WHERE serial_number = ?", (serial_number,))
            exists = cursor.fetchone()[0]
            if exists == 0:
                log_message("info", f"⚠️ Missing serial_number: {serial_number}")
                missing_serials.append(serial_number)
    
    log_message("info", f"✅ Chunk {chunk_id} checked: {len(missing_serials)} missing serial numbers found.")
    return missing_serials

def check_and_store_missing_data():
    log_message("info", "🚀 Script started: Checking for missing data")

    local_db = "local_data.db"
    processed_db = "processed_data.db"
    missing_db = "missing_data.db"

    # Get actual table names
    local_table = get_table_name(local_db)
    processed_table = get_table_name(processed_db)
    
    if not local_table or not processed_table:
        log_message("error", "❌ Could not determine table names from one or both databases.")
        return

    log_message("info", f"✅ Found tables: {local_table} (local_data) and {processed_table} (processed_data)")

    # Ensure missing_data.db exists
    if not os.path.exists(missing_db):
        log_message("info", f"🔧 Creating {missing_db}...")
        create_missing_db_structure(local_db, missing_db, local_table)

    # Connect to databases
    processed_conn = sqlite3.connect(processed_db)
    processed_cursor = processed_conn.cursor()

    try:
        log_message("info", f"🔍 Fetching data from {processed_table}...")
        processed_cursor.execute(f"SELECT column_a, column_c FROM {processed_table}")
        processed_rows = processed_cursor.fetchall()
        log_message("info", f"✅ Retrieved {len(processed_rows)} rows from {processed_table}.")

        if not processed_rows:
            log_message("info", "✅ No data found in processed_data, exiting.")
            return

        serial_numbers = [row[0] for row in processed_rows]
        manufacture_dates = {row[0]: row[1] for row in processed_rows}

        # Use multithreading to speed up checking missing serials
        log_message("info", "🚀 Checking for missing serial numbers in parallel...")
        missing_serials = []
        chunk_size = max(1, len(serial_numbers) // (os.cpu_count() or 4))  # Avoid division by zero

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for i, start in enumerate(range(0, len(serial_numbers), chunk_size)):
                chunk = serial_numbers[start:start + chunk_size]
                log_message("info", f"⚡ Dispatching chunk {i} to worker thread...")
                futures.append(executor.submit(check_serial_numbers, chunk, local_db, local_table, i))

            for future in concurrent.futures.as_completed(futures):
                missing_serials.extend(future.result())

        if missing_serials:
            log_message("info", f"🚀 Inserting {len(missing_serials)} missing entries into {missing_db}...")
            with sqlite3.connect(missing_db) as missing_conn:
                missing_cursor = missing_conn.cursor()
                for serial in missing_serials:
                    log_message("info", f"📌 Inserting missing serial_number {serial} into {missing_db}...")
                    missing_cursor.execute(
                        f"INSERT INTO {local_table} (serial_number, manufacture_date) VALUES (?, ?)",
                        (serial, manufacture_dates[serial])
                    )
                missing_conn.commit()

            log_message("info", f"✅ Added {len(missing_serials)} missing entries to {missing_db}.")
        else:
            log_message("info", "✅ No missing entries found.")

    except sqlite3.Error as e:
        log_message("error", f"❌ SQLite error occurred: {e}")
    finally:
        # Close connections
        processed_conn.close()
        log_message("info", "✅ Database connections closed.")
        log_message("info", "🏁 Script completed.")

if __name__ == "__main__":
    check_and_store_missing_data()
