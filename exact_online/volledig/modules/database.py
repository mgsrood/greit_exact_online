import pyodbc
import time
from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine, event, text
import urllib

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

def write_to_database(df, tabel, connection_string):
    db_params = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={db_params}")

    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True

    with engine.connect() as connection:   
        try:
            # Andere modes blijven ongewijzigd
            df.to_sql(tabel, engine, index=False, if_exists="append", schema="dbo")
        except Exception as e:
            print(f"Fout bij het toevoegen naar de database: {e}")

    print(f"DataFrame succesvol toegevoegd/bijgewerkt in de tabel: {tabel}")

def clear_table(connection_string, table, mode, start_date, division_code):
    try:
        # Maak verbinding met de database
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        
        rows_deleted = 0
        if mode == 'orders':
            # Verwijder rijen waar Aangemaakt >= start_date en AdministratieCode = division_code
            cursor.execute(f"DELETE FROM {table} WHERE O_Orderdatum >= ? AND O_AdministratieCode = ?", start_date, division_code)
            rows_deleted = cursor.rowcount
        elif mode == 'facturen':
            # Verwijder rijen waar Aangemaakt >= start_date en AdministratieCode = division_code
            cursor.execute(f"DELETE FROM {table} WHERE F_Factuurdatum >= ? AND F_AdministratieCode = ?", start_date, division_code)
            rows_deleted = cursor.rowcount
        elif mode == 'mutaties':
            # Selecteer jaar uit start_date
            jaar = start_date[:4]
            cursor.execute(f"DELETE FROM {table} WHERE Boekjaar >= ? AND AdministratieCode = ?", jaar, division_code)
            rows_deleted = cursor.rowcount
        elif mode == 'none':
            # Doe niets
            print(f"Geen actie ondernomen voor tabel {table}.")
            actie = f"Geen actie ondernomen voor tabel {table}."
        else:
            actie = f"Ongeldige mode: {mode}"
            print(actie)
            return actie
        
        # Commit de transactie
        connection.commit()
        print(f"Actie '{mode}' succesvol uitgevoerd voor tabel {table}. Rijen verwijderd: {rows_deleted}")
        actie = f"Actie '{mode}' succesvol uitgevoerd voor tabel {table}. Rijen verwijderd: {rows_deleted}"
    except pyodbc.Error as e:
        print(f"Fout bij het uitvoeren van de actie '{mode}' voor tabel {table}: {e}")
        actie = f"Fout bij het uitvoeren van de actie '{mode}' voor tabel {table}: {e}"
    finally:
        # Sluit de cursor en verbinding
        cursor.close()
        connection.close()
    
    return actie, rows_deleted