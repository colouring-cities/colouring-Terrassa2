import pandas as pd
import psycopg2

# --- CONFIG ---
CSV_FILE = 'merged_buildings_full.csv'
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'colouringcitiesdb',
    'user': 'colouring_cities',
    'password': '12345'
}

# --- READ CSV ---
df = pd.read_csv(CSV_FILE)

# --- CLEAN AND MAP COLUMNS ---
df['building_reference'] = df['building_reference'].astype(str).str.strip()
df['UseDescription'] = df['UseDescription'].astype(str).str.strip()
df['second_type'] = df['second_type'].astype(str).str.strip().fillna('Unknown')

# Wrap ref_toid in quotes to match DB
df['ref_toid'] = df['building_reference'].apply(lambda x: f'"{x}"')

# Map CSV columns to DB columns
df['current_landuse_order'] = df['UseDescription']                        # FK column
# Convert second_type to PostgreSQL array literal string
df['current_landuse_group'] = df['second_type'].apply(lambda x: '{' + x + '}')
df['date_year'] = df['YearBuilt1']
df['size_height_apex'] = df['HEIGHT']

update_df = df[['ref_toid', 'current_landuse_order', 'current_landuse_group', 'date_year', 'size_height_apex']]

# --- CONNECT TO DB ---
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# --- DEBUG: check matching rows ---
cur.execute("SELECT COUNT(*) FROM buildings WHERE ref_toid = ANY(%s);", (list(update_df['ref_toid']),))
print("Matching buildings in DB:", cur.fetchone()[0])

# --- UPDATE QUERY ---
update_sql = """
UPDATE buildings
SET current_landuse_order = %s,
    current_landuse_group = %s::text[],
    date_year = %s,
    size_height_apex = %s
WHERE ref_toid = %s
"""

# --- LOOP OVER ROWS AND UPDATE ---
for _, row in update_df.iterrows():
    cur.execute(update_sql, (
        row['current_landuse_order'],
        row['current_landuse_group'],   # as PostgreSQL array literal
        row['date_year'],
        row['size_height_apex'],
        row['ref_toid']
    ))

# Commit changes and close connection
conn.commit()
cur.close()
conn.close()

print("Buildings table updated successfully!")

