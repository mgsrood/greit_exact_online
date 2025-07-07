import pandas as pd
import re
from utils.env_config import setup_environment
from utils.database_connection import connect_to_database
from utils.config import ConfigManager
import pyodbc

# --- Instellingen ---
CSV_PATH = '/Users/maxrood/werk/greit/klanten/finn_it/greit_exact_online/sql_script/Map1.csv'  # Hardcoded pad naar je CSV-bestand
TABLE_NAME = 'Excel'  # Doeltabel
KLANT_NAAM = 'SecureVest B.V.'
APPLICATIE = 'AFAS'

# --- Stap 1: Lees de CSV in ---
df = pd.read_csv(CSV_PATH, delimiter=';', encoding='utf-8')

# Kolomnamen strippen (spaties voor/na weg) en vervangen dubbele spaties door enkele
clean_columns = [re.sub(r'\s+', ' ', col.strip()) for col in df.columns]
df.columns = clean_columns

# Zet alle waarden om naar string en vervang NaN door lege string
for col in df.columns:
    df[col] = df[col].astype(str).where(df[col].notna(), '')

# --- Stap 2: Haal connectiestring op via ConfigManager ---
db_config = setup_environment()
config_manager = ConfigManager(
    connection_string=db_config["connection_string"],
    auth_method=db_config["auth_method"],
    tenant_id=db_config.get("tenant_id"),
    client_id=db_config.get("client_id"),
    client_secret=db_config.get("client_secret"),
    script_name=db_config.get("script_name")
)

connection_dict = config_manager.get_connection_strings()

# Zoek de juiste connectiestring
conn_str = None
for klant, (connection_string, type, applicatie) in connection_dict.items():
    if klant == KLANT_NAAM and applicatie == APPLICATIE:
        conn_str = connection_string
        break

if not conn_str:
    raise Exception(f"Geen connectiestring gevonden voor klant '{KLANT_NAAM}' en applicatie '{APPLICATIE}'")

conn = connect_to_database(
    conn_str,
    db_config.get('auth_method', 'SQL'),
    db_config.get('tenant_id'),
    db_config.get('client_id'),
    db_config.get('client_secret')
)

# --- Stap 3: Maak de tabel aan (drop als bestaat) ---
cursor = conn.cursor()

# Drop bestaande tabel indien aanwezig
try:
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    conn.commit()
except Exception as e:
    print(f"Kon bestaande tabel niet droppen: {e}")

# Bouw CREATE TABLE statement dynamisch
col_types = []
for col in df.columns:
    # Alles als NVARCHAR(255) voor maximale compatibiliteit
    col_types.append(f"[{col}] NVARCHAR(255)")
col_defs = ', '.join(col_types)
create_stmt = f"CREATE TABLE {TABLE_NAME} ({col_defs})"
cursor.execute(create_stmt)
conn.commit()

# --- Stap 4: Upload de data ---
placeholders = ','.join(['?'] * len(df.columns))
insert_stmt = f"INSERT INTO {TABLE_NAME} ({', '.join('['+c+']' for c in df.columns)}) VALUES ({placeholders})"

for row in df.itertuples(index=False, name=None):
    cursor.execute(insert_stmt, row)
conn.commit()

print(f"CSV succesvol geüpload naar tabel {TABLE_NAME} voor klant '{KLANT_NAAM}' en applicatie '{APPLICATIE}'!")

cursor.close()
conn.close() 