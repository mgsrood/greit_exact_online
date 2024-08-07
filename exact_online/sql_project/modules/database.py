import pyodbc

def connect_to_database(connection_string):
    try:
        conn = pyodbc.connect(connection_string)
        return conn
    except Exception as e:
        print(f"Fout bij verbinding: {e}")
        return None

def write_to_database(df, tabel, connection_string):
    table_conn = connect_to_database(connection_string)
    if table_conn:
        cursor = table_conn.cursor()

        # Controleer of de tabel bestaat
        cursor.execute(f"IF OBJECT_ID('{tabel}', 'U') IS NULL BEGIN PRINT 'Tabel niet gevonden.'; RETURN; END")

        # Invoerquery voorbereiden
        insert_query = f"INSERT INTO {tabel} ({', '.join(df.columns)}) VALUES ({', '.join(['?' for _ in df.columns])})"

        # Voeg de DataFrame rijen toe aan de bestaande tabel
        for row in df.itertuples(index=False):
            cursor.execute(insert_query, row)
    
        table_conn.commit()
        cursor.close()
        table_conn.close()
        print(f"DataFrame succesvol toegevoegd aan de tabel: {tabel}")

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
    except pyodbc.Error as e:
        print(f"Fout bij het uitvoeren van de actie '{mode}' voor tabel {table}: {e}")
    finally:
        # Sluit de cursor en verbinding
        cursor.close()
        connection.close()