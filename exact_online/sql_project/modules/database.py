import pyodbc
import time
from datetime import datetime

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

def write_to_database(df, tabel, connection_string, unique_column, division_column, mode, laatste_sync):
    conn = connect_to_database(connection_string)
    if conn:
        cursor = conn.cursor()

        # Stap 1: Creëer een tijdelijke tabel met dezelfde structuur als de doel-tabel
        temp_table_name = "#TempTable"
        create_temp_table_query = f"""
        CREATE TABLE {temp_table_name} (
            {', '.join([f"{col} NVARCHAR(MAX)" for col in df.columns])}
        );
        """
        cursor.execute(create_temp_table_query)

        # Stap 2: Voeg data in de tijdelijke tabel in
        insert_temp_query = f"INSERT INTO {temp_table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['?' for _ in df.columns])})"
        cursor.executemany(insert_temp_query, df.itertuples(index=False))
        conn.commit()

        # Stap 3: Voer de MERGE of INSERT uit vanuit de tijdelijke tabel naar de doel-tabel
        if mode == 'none':
            if unique_column and division_column:
                merge_query = f"""
                MERGE {tabel} AS target
                USING (SELECT * FROM {temp_table_name}) AS source
                ON (target.{unique_column} = source.{unique_column} AND target.{division_column} = source.{division_column})
                WHEN MATCHED THEN 
                    UPDATE SET {', '.join([f"target.{col} = source.{col}" for col in df.columns if col not in [unique_column, division_column]])}
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(df.columns)})
                    VALUES ({', '.join(['source.' + col for col in df.columns])});
                """
                cursor.execute(merge_query)
            else:
                raise ValueError("unique_column en division_column moeten worden opgegeven voor de 'none' mode.")
        else:
            # Eenvoudige INSERT bij andere modes
            insert_query = f"INSERT INTO {tabel} ({', '.join(df.columns)}) SELECT * FROM {temp_table_name};"
            cursor.execute(insert_query)

        # Stap 4: Opruimen - Verwijder de tijdelijke tabel
        cursor.execute(f"DROP TABLE {temp_table_name}")
        
        # Commit en sluit de verbinding
        conn.commit()
        cursor.close()
        conn.close()
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