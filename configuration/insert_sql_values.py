import psycopg2


# Database connection settings
DB_HOST = "localhost"        # e.g. "localhost" or a server address
DB_PORT = "5432"             # default PostgreSQL port
DB_NAME = "colouringcitiesdb" # replace with  DB name
DB_USER ="colouring_cities"   # replace with your username
DB_PASS = "12345"    # replace with your password


SQL_FILE = "generate_footprints.sql" # Path to your SQL script

def run_sql_file(filename):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = True  # so DROP/CREATE/INSERT work outside transactions
        cur = conn.cursor()

        print(f"Reading SQL file: {filename}")

        # Read SQL script from file
        with open(filename, "r", encoding="utf-8") as f:
            sql_script = f.read()

        print("Executing SQL script...")
        cur.execute(sql_script)
        print("SQL script executed successfully!")

        cur.close()
        conn.close()

    except Exception as e:
        print(" Error:", e)

if __name__ == "__main__":
    run_sql_file(SQL_FILE)
