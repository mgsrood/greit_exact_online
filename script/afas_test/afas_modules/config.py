from datetime import datetime
import pyodbc
import time

def connect_to_database(connection_string):
    # Retries en delays
    max_retries = 3
    retry_delay = 5
    
    # Pogingen doen om connectie met database te maken
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(connection_string)
            return conn
        except Exception as e:
            print(f"Fout bij poging {attempt + 1} om verbinding te maken: {e}")
            if attempt < max_retries - 1:  # Wacht alleen als er nog pogingen over zijn
                time.sleep(retry_delay)
    
    # Als het na alle pogingen niet lukt, return None
    print("Kan geen verbinding maken met de database na meerdere pogingen.")
    return None


def log(logging_connection_string, klantnaam, actie, script_id, script, administratiecode=None, tabel=None):
    # Actuele datum en tijd ophalen
    datumtijd = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    # Probeer verbinding te maken met de database
    logging_conn = connect_to_database(logging_connection_string)

    # Foutmelding geven indien connectie niet gemaakt kan worden
    if logging_conn is None:
        print("Kan geen verbinding maken met de logging database na 3 pogingen.")
        return  # Stop de functie als er geen verbinding is

    try:
        # Verbinding maken met database
        cursor = logging_conn.cursor()

        # Query om waarden toe te voegen aan de Logging tabel met parameterbinding
        insert_query = """
        INSERT INTO Logging (Klantnaam, Actie, Datumtijd, Administratiecode, Tabel, Script, ScriptID)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        # Voer de INSERT-query uit met parameterbinding
        cursor.execute(insert_query, (klantnaam, actie, datumtijd, administratiecode, tabel, script, script_id))
        logging_conn.commit() 

    except Exception as e:
        print(f"Fout bij het toevoegen van waarden: {e}")

    finally:
        # Sluit connectie als die is gemaakt
        if logging_conn:
            logging_conn.close()

def fetch_script_id(cursor):
    # Voer de query uit om het hoogste ScriptID op te halen
    query = 'SELECT MAX(ScriptID) FROM Logging'
    cursor.execute(query)
    
    # Verkrijg het resultaat
    highest_script_id = cursor.fetchone()[0]

    return highest_script_id

def determine_script_id(finn_it_connection_string, klant, script):
    try:
        database_conn = connect_to_database(finn_it_connection_string)
    except Exception as e:
        print(f"Verbinding met database mislukt, foutmelding: {e}")
    if database_conn:
        print(f"Verbinding met database geslaagd")
        cursor = database_conn.cursor()
        latest_script_id = fetch_script_id(cursor)
        print(f"ScriptID: {latest_script_id}")
        database_conn.close()

    if latest_script_id:
        script_id = latest_script_id + 1
    else:
        script_id = 1
        
    log(finn_it_connection_string, klant, f"Script gestart", script_id, script)
    
    return script_id

def fetch_all_connection_strings(cursor):
    # Voer de query uit om alle connectiestrings op te halen
    query = 'SELECT * FROM Klanten'
    cursor.execute(query)
    
    # Verkrijg alle rijen uit de resultaten
    rows = cursor.fetchall()
    
    # Extract de connectiestrings uit de resultaten, inclusief het softwarepakket
    connection_dict = {row[1]: (row[2], row[3], row[4]) for row in rows}
    return connection_dict

def create_connection_dict(finn_it_connection_string, klantnaam, script, script_id):
    max_retries = 3
    retry_delay = 5
    
    try:
        database_conn = connect_to_database(finn_it_connection_string)
    except Exception as e:
        log(finn_it_connection_string, klantnaam, f"Verbinding met database mislukt, foutmelding: {e}", script_id, script)
    if database_conn:
        log(finn_it_connection_string, klantnaam, f"Verbinding met database opnieuw geslaagd", script_id, script)
        cursor = database_conn.cursor()
        connection_dict = None
        for attempt in range(max_retries):
            try:
                connection_dict = fetch_all_connection_strings(cursor)
                if connection_dict:
                    break
            except Exception as e:
                time.sleep(retry_delay)
        database_conn.close()
        if connection_dict:
            log(finn_it_connection_string, klantnaam, f"Ophalen connectiestrings gestart", script_id, script)

        else:
            print(f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen")
            log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen", script_id, script)
            
    else:
        print(f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen")
        log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen", script_id, script)
    
    log(finn_it_connection_string, klantnaam, "Configuratie dictionary opgehaald", script_id, script)
    
    return connection_dict

def fetch_configurations(cursor):
    query = 'SELECT * FROM Config'
    cursor.execute(query)

    # Verkrijg alle rijen uit de resultaten
    rows = cursor.fetchall()

    # Maak een configuratie dictionary
    config_dict = {row[1]: row[2] for row in rows}

    return config_dict

def extract_config_variables(connection_string, finn_it_connection_string, klantnaam, script, script_id):
    
    log(finn_it_connection_string, klantnaam, f"Ophalen configuratie gegevens gestart", script_id, script)
    
    # Aantal retries instellen
    max_retries = 3
    retry_delay = 5
    
    # Connectie opzetten
    config_conn = connect_to_database(connection_string)
    
    # Ophalen configruaties
    if config_conn:
        cursor = config_conn.cursor()
        config_dict = None
        
        # Ophalen configuratie dictionary
        for attempt in range(max_retries):
            try:
                config_dict = fetch_configurations(cursor)
                if config_dict:
                    break
            except Exception as e:
                time.sleep(retry_delay)
        
        # Extraheren van laatste_sync en reporting_year
        if config_dict:
            api_string = config_dict['APIString']
            token = config_dict['Token']
            config_conn.close()
    
            # Succes en start log
            log(finn_it_connection_string, klantnaam, f"Ophalen configuratie gegevens gelukt", script_id, script)
            
            return api_string, token
        else:
            # Foutmelding log
            print(f"FOUTMELDING | Ophalen configuratie gegevens mislukt", script_id, script)
            log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Ophalen configuratie gegevens mislukt", script_id, script)
            
            return None

def fetch_table_configurations(cursor):
    query = 'SELECT * FROM TabellenConfig'
    cursor.execute(query)

    # Verkrijg alle rijen uit de resultaten
    rows = cursor.fetchall()

    # Maak een configuratie dictionary
    table_config_dict = {row[1]: row[2] for row in rows}

    return table_config_dict

def create_table_config_dict(connection_string, finn_it_connection_string, klantnaam, script, script_id):
    
    log(finn_it_connection_string, klantnaam, f"Ophalen tabel configuratie gegevens gestart", script_id, script)
    
    # Aantal retries instellen
    max_retries = 3
    retry_delay = 5
    
    # Connectie opzetten
    config_conn = connect_to_database(connection_string)
    
    # Ophalen tabel configuratie gegevens
    if config_conn:    
        cursor = config_conn.cursor()
        table_config_dict = None
        
        # Ophalen tabel configuratie dictionary
        for attempt in range(max_retries):
            try:
                table_config_dict = fetch_table_configurations(cursor)
                if table_config_dict:
                    log(finn_it_connection_string, klantnaam, f"Ophalen tabel configuratie gegevens gelukt", script_id, script)
                    return table_config_dict
            except Exception as e:
                time.sleep(retry_delay)
        config_conn.close()
        
        # Check of configuratiegegevens zijn opgehaald
        if not table_config_dict:
            log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Ophalen tabel configuratie gegevens mislukt", script_id, script)
            return None
                
    else:
        # Foutmelding log voor mislukte verbinding
        print(f"FOUTMELDING | Verbinding met configuratie database mislukt", script_id, script)
        log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Verbinding met configuratie database mislukt", script_id, script)
        return None

def fetch_division_codes(cursor):
    query = 'SELECT * FROM Divisions'
    cursor.execute(query)

    # Verkrijg alle rijen uit de resultaten
    rows = cursor.fetchall()

    # Maak een configuratie dictionary
    division_dict = {row[2]: row[1] for row in rows}

    return division_dict

def create_division_dict(finn_it_connection_string, klantnaam, connection_string, script, script_id):
    
    log(finn_it_connection_string, klantnaam, f"Ophalen divisiecodes gestart", script_id, script)
    
    # Aantal retries instellen
    max_retries = 3
    retry_delay = 5

    # Connectie opzetten voor divisiecodes
    division_conn = connect_to_database(connection_string)
    
    # Ophalen divisiecodes
    if division_conn:
        cursor = division_conn.cursor()
        division_dict = None
        
        # Ophalen divisiecode dictionary
        for attempt in range(max_retries):
            try:
                division_dict = fetch_division_codes(cursor)
                if division_dict:
                    # Sluit de verbinding en return de gegevens als succesvol
                    division_conn.close()
                    log(finn_it_connection_string, klantnaam, f"Ophalen divisiecodes gelukt", script_id, script)
                    return division_dict
            except Exception as e:
                time.sleep(retry_delay)

        # Sluit de verbinding als het ophalen van divisiecodes mislukt
        division_conn.close()

        # Foutmelding log voor mislukte divisiecode-ophaling
        log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Ophalen divisiecodes mislukt", script_id, script)
    else:
        # Foutmelding log voor mislukte verbinding
        print(f"FOUTMELDING | Verbinding met divisie database mislukt", script_id, script)
        log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Verbinding met divisie database mislukt", script_id, script)
        
    return None


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