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