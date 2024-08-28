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

def write_to_database(df, tabel, connection_string, unique_column, division_column):
    table_conn = connect_to_database(connection_string)
    if table_conn:
        cursor = table_conn.cursor()

        # Controleer of de tabel bestaat
        cursor.execute(f"IF OBJECT_ID('{tabel}', 'U') IS NULL BEGIN PRINT 'Tabel niet gevonden.'; RETURN; END")

        # Voorbereiden van de MERGE statement
        merge_query = f"""
        MERGE {tabel} AS target
        USING (
            SELECT {', '.join([f'? AS {col}' for col in df.columns])}
        ) AS source
        ON (target.{unique_column} = source.{unique_column} AND target.{division_column} = source.{division_column})
        WHEN MATCHED THEN 
            UPDATE SET {', '.join([f"target.{col} = source.{col}" for col in df.columns if col not in [unique_column, division_column]])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(df.columns)})
            VALUES ({', '.join(['source.' + col for col in df.columns])});
        """

        # Voeg de DataFrame rijen toe aan de bestaande tabel of update ze
        for row in df.itertuples(index=False):
            cursor.execute(merge_query, row)
    
        table_conn.commit()
        cursor.close()
        table_conn.close()
        print(f"DataFrame succesvol toegevoegd/ bijgewerkt in de tabel: {tabel}")

def clear_table(connection_string, table, mode, reporting_year, division_code):
    try:
        # Maak verbinding met de database
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        
        if mode == 'truncate':
            # Probeer de tabel leeg te maken met TRUNCATE TABLE
            try:
                cursor.execute(f"DELETE FROM {table} WHERE AdministratieCode = ?", division_code)
            except pyodbc.Error as e:
                print(f"DELETE FROM {table} WHERE AdministratieCode = {division_code} failed: {e}")
        elif mode == 'reporting_year':
            # Verwijder rijen waar ReportingYear >= reporting_year en AdministratieCode = division_code
            cursor.execute(f"DELETE FROM {table} WHERE ReportingYear >= ? AND AdministratieCode = ?", reporting_year, division_code)
        elif mode == 'none':
            # Doe niets
            print(f"Geen actie ondernomen voor tabel {table}.")
        
        # Commit de transactie
        connection.commit()
        print(f"Actie '{mode}' succesvol uitgevoerd voor tabel {table}.")
        actie = f"Actie '{mode}' succesvol uitgevoerd voor tabel {table}."
    except pyodbc.Error as e:
        print(f"Fout bij het uitvoeren van de actie '{mode}' voor tabel {table}: {e}")
    finally:
        # Sluit de cursor en verbinding
        cursor.close()
        connection.close()
    
    return actie