from afas_modules.config import save_laatste_sync
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
            
def sync_log(klantnaam, nieuwe_laatste_sync, errors_occurred, connection_string, finn_it_connection_string, script_id, script, omgeving_id, table):
    # Succes en start log
    print(f"Sync succesvol afgerond voor klant: {klantnaam}")
    log(finn_it_connection_string, klantnaam, f"Sync succesvol afgerond", script_id, script, omgeving_id, table)

    if not errors_occurred:
        log(finn_it_connection_string, klantnaam, f"Creëren nieuwe laatste sync en reporting year", script_id, script, omgeving_id, table)

        try:    
            # Update laatste sync en reporting year
            save_laatste_sync(connection_string, nieuwe_laatste_sync)

            # Succes log
            print(f"Laatste sync en reporting year succesvol geüpdate voor klant: {klantnaam}")
            log(finn_it_connection_string, klantnaam, f"Laatste sync en reporting year succesvol geüpdate", script_id, script, omgeving_id, table)

        except Exception as e:
            # Foutmelding log en print
            print(f"Fout bij het toevoegen naar database: {e}")
            log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij updaten laatste_sync: {str(e)}", script_id, script, omgeving_id, table)
    else:
        # log dat er fouten zij opgetreden en dat de laatste sync niet is geupdate
        print(f"Er zijn fouten opgetreden voor klant: {klantnaam}, laatste_sync wordt niet geüpdate")
        log(finn_it_connection_string, klantnaam, f"Er zijn fouten opgetreden, laatste_sync wordt niet geüpdate", script_id, script, omgeving_id, table)

    # Succes log
    print(f"Endpoints succesvol afgerond voor klant: {klantnaam}")
    log(finn_it_connection_string, klantnaam, f"Script succesvol afgerond", script_id, script, omgeving_id, table)
    
def end_log(start_time, finn_it_connection_string, script, script_id, omgeving_id, table):
    # Totale tijdsduur script
    end_time = time.time()
    total_duration = end_time - start_time  # Tijd in seconden

    # Omzetten naar HH:MM:SS
    hours, remainder = divmod(total_duration, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_duration = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

    # Succes log
    print(f"Script volledig afgerond in {formatted_duration}")
    log(finn_it_connection_string, "Finn It", f"Script volledig afgerond in {formatted_duration}", script_id, script, omgeving_id, table)