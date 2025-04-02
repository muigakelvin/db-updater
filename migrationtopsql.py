import sqlite3
import psycopg2
from psycopg2 import sql

# Configuration for SQLite database
SQLITE_DB_PATH = "missing_data.db"  # Path to your SQLite database file

# Configuration for PostgreSQL database
POSTGRES_CONFIG = {
    "dbname": "et_service_local_db",
    "user": "akili_et_service",
    "password": "local_db_password",
    "host": "localhost",
    "port": "5434",
}

# Table name in PostgreSQL
POSTGRES_TABLE = "cookstoves"

def connect_to_sqlite():
    """Connect to the SQLite database."""
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        print(f"[INFO] Connected to SQLite database at {SQLITE_DB_PATH}")
        return conn
    except Exception as e:
        print(f"[ERROR] Failed to connect to SQLite database: {e}")
        raise

def connect_to_postgres():
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        print("[INFO] Connected to PostgreSQL database.")
        return conn
    except Exception as e:
        print(f"[ERROR] Failed to connect to PostgreSQL database: {e}")
        raise

def get_table_name_from_sqlite(conn):
    """Retrieve the name of the single table in the SQLite database."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        if len(tables) != 1:
            raise ValueError(f"Expected 1 table in SQLite database, but found {len(tables)} tables.")

        table_name = tables[0][0]
        print(f"[INFO] Found table in SQLite database: {table_name}")
        return table_name
    except Exception as e:
        print(f"[ERROR] Failed to retrieve table name from SQLite database: {e}")
        raise

def fetch_data_from_sqlite(conn, table_name):
    """Fetch data from the SQLite database."""
    try:
        cursor = conn.cursor()
        query = f"SELECT serial_number, manufacture_date FROM {table_name};"
        cursor.execute(query)
        rows = cursor.fetchall()
        print(f"[INFO] Fetched {len(rows)} rows from SQLite database.")
        return rows
    except Exception as e:
        print(f"[ERROR] Failed to fetch data from SQLite database: {e}")
        raise

def get_postgres_table_columns(conn):
    """Retrieve the column names of the PostgreSQL table."""
    try:
        cursor = conn.cursor()
        cursor.execute(
            sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_name = %s"),
            [POSTGRES_TABLE]
        )
        columns = [row[0] for row in cursor.fetchall()]
        print(f"[INFO] Retrieved columns from PostgreSQL table '{POSTGRES_TABLE}': {columns}")
        return columns
    except Exception as e:
        print(f"[ERROR] Failed to retrieve columns from PostgreSQL table: {e}")
        raise

def reset_sequence(conn):
    """Reset the sequence for the 'id' column in the PostgreSQL table."""
    try:
        cursor = conn.cursor()
        cursor.execute(sql.SQL("SELECT MAX(id) FROM {}").format(sql.Identifier(POSTGRES_TABLE)))
        max_id = cursor.fetchone()[0] or 0  # Default to 0 if no records exist
        
        sequence_name = f"{POSTGRES_TABLE}_id_seq"
        cursor.execute(sql.SQL("SELECT setval(%s, %s, true);"), [sequence_name, max_id + 1])
        print(f"[INFO] Reset sequence '{sequence_name}' to {max_id + 1}")
        conn.commit()
    except Exception as e:
        print(f"[ERROR] Failed to reset sequence: {e}")
        conn.rollback()
        raise

def insert_data_into_postgres(conn, rows, postgres_columns):
    """Insert data into the PostgreSQL table based on its schema."""
    try:
        cursor = conn.cursor()
        inserted_count = 0
        available_columns = [col for col in ["serial_number", "manufacture_date"] if col in postgres_columns]
        placeholders = ", ".join(["%s"] * len(available_columns))
        columns_sql = sql.SQL(", ").join(map(sql.Identifier, available_columns))

        query = sql.SQL("""
            INSERT INTO {} ({})
            VALUES ({})
            ON CONFLICT (serial_number) DO NOTHING;
        """).format(sql.Identifier(POSTGRES_TABLE), columns_sql, sql.SQL(placeholders))

        for row in rows:
            values = [row[i] for i, col in enumerate(["serial_number", "manufacture_date"]) if col in available_columns]
            cursor.execute(query, values)
            inserted_count += cursor.rowcount

        conn.commit()
        print(f"[INFO] Inserted {inserted_count} rows into PostgreSQL table '{POSTGRES_TABLE}'.")
        return inserted_count
    except Exception as e:
        print(f"[ERROR] Failed to insert data into PostgreSQL table: {e}")
        conn.rollback()
        raise

def main():
    print("[START] Starting the script to transfer data from SQLite to PostgreSQL...")
    sqlite_conn = connect_to_sqlite()
    sqlite_table_name = get_table_name_from_sqlite(sqlite_conn)
    rows = fetch_data_from_sqlite(sqlite_conn, sqlite_table_name)
    postgres_conn = connect_to_postgres()
    postgres_columns = get_postgres_table_columns(postgres_conn)
    
    print("[INFO] Resetting sequence before insertion.")
    reset_sequence(postgres_conn)
    inserted_count = insert_data_into_postgres(postgres_conn, rows, postgres_columns)
    
    print("[INFO] Resetting sequence after insertion.")
    reset_sequence(postgres_conn)
    
    sqlite_conn.close()
    postgres_conn.close()
    print("[END] Script completed successfully.")

if __name__ == "__main__":
    main()