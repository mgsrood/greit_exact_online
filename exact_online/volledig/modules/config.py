from datetime import datetime, timedelta
from modules.database import connect_to_database

def fetch_script_id(cursor):
    # Voer de query uit om het hoogste ScriptID op te halen
    query = 'SELECT MAX(ScriptID) FROM Logging'
    cursor.execute(query)
    
    # Verkrijg het resultaat
    highest_script_id = cursor.fetchone()[0]

    return highest_script_id

def fetch_all_connection_strings(cursor):
    # Voer de query uit om alle connectiestrings op te halen
    query = 'SELECT * FROM Klanten'
    cursor.execute(query)
    
    # Verkrijg alle rijen uit de resultaten
    rows = cursor.fetchall()
    
    # Extract de connectiestrings uit de resultaten
    connection_dict = {row[1]: (row[2], row[3]) for row in rows}  
    return connection_dict

def fetch_configurations(cursor):
    query = 'SELECT * FROM Config'
    cursor.execute(query)

    # Verkrijg alle rijen uit de resultaten
    rows = cursor.fetchall()

    # Maak een configuratie dictionary
    config_dict = {row[1]: row[2] for row in rows}

    return config_dict

def fetch_division_codes(cursor):
    query = 'SELECT * FROM Divisions'
    cursor.execute(query)

    # Verkrijg alle rijen uit de resultaten
    rows = cursor.fetchall()

    # Maak een configuratie dictionary
    division_dict = {row[2]: row[1] for row in rows}

    return division_dict

def fetch_table_configurations(cursor):
    query = 'SELECT * FROM TabellenConfig'
    cursor.execute(query)

    # Verkrijg alle rijen uit de resultaten
    rows = cursor.fetchall()

    # Maak een configuratie dictionary
    table_config_dict = {row[1]: row[2] for row in rows}

    return table_config_dict

def save_laatste_sync(connection_string, laatste_sync):
    # Verbinding maken met database voor ophalen laatste sync
    sync_conn = connect_to_database(connection_string)
    if sync_conn:
        cursor = sync_conn.cursor()

        # Query en uitvoering
        query = 'UPDATE Config SET Waarde = ? WHERE Config = ?'
        config = 'Laatste_sync'
        try:
            cursor.execute(query, (laatste_sync, config))
            sync_conn.commit()  # Maak de wijziging permanent
            print("Laatste sync succesvol bijgewerkt.")
        except Exception as e:
            print(f"Fout bij uitvoeren van de query: {e}")
            sync_conn.rollback()  # Rollback in geval van een fout
        finally:
            sync_conn.close() 

def save_reporting_year(connection_string):
    # Verbinding maken met database voor ophalen reporting year
    year_conn = connect_to_database(connection_string)
    if year_conn:
        cursor = year_conn.cursor()

        # Query en uitvoering
        query = 'UPDATE Config SET Waarde = ? WHERE Config = ?'
        config = 'ReportingYear'
        current_year = datetime.now().year
        last_year = current_year - 1

        try:
            cursor.execute(query, (last_year, config))
            year_conn.commit()  # Maak de wijziging permanent
            print("Reporting Year succesvol bijgewerkt.")
        except Exception as e:
            print(f"Fout bij uitvoeren van de query: {e}")
            year_conn.rollback()  # Rollback in geval van een fout
        finally:
            year_conn.close() 