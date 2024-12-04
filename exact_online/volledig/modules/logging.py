from datetime import datetime
from modules.database import connect_to_database
import time

def logging(logging_connection_string, klantnaam, actie, script_id, script, administratiecode=None, tabel=None):
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

        # Query om waarden toe te voegen aan de log tabel met parameterbinding
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