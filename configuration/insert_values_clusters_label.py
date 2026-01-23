import pandas as pd
import psycopg2

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

# --- CLEAN & MAP COLUMNS ---
df['building_reference'] = df['building_reference'].astype(str).str.strip()
df['label'] = df['label'].astype(str).str.strip()  # column that holds the 'is_domestic' label

# Wrap in quotes to match DB ref_toid format
df['ref_toid'] = df['building_reference'].apply(lambda x: f'"{x}"')

# Select relevant columns
update_df = df[['ref_toid', 'label']]

# --- CONNECT TO DATABASE ---
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# --- DEBUG: count matching rows ---
cur.execute("SELECT COUNT(*) FROM buildings WHERE ref_toid = ANY(%s);", (list(update_df['ref_toid']),))
print("Matching buildings in DB:", cur.fetchone()[0])

# --- UPDATE SQL ---
update_sql = """
UPDATE buildings
SET is_domestic = %s
WHERE ref_toid = %s
"""

# --- LOOP AND UPDATE ---
for _, row in update_df.iterrows():
    cur.execute(update_sql, (
        row['label'],       # new value for is_domestic
        row['ref_toid']     # building reference
    ))

# --- COMMIT AND CLOSE ---
conn.commit()
cur.close()
conn.close()

print("Column 'is_domestic' updated successfully in buildings table!")

