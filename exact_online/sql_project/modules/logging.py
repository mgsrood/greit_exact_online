from datetime import datetime

def logging(cursor, conn, klantnaam, actie):
    datumtijd = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    # Query om waarden toe te voegen aan de Logging tabel met parameterbinding
    insert_query = """
    INSERT INTO Logging (Klantnaam, Actie, Datumtijd)
    VALUES (?, ?, ?)
    """

    # Voer de INSERT-query uit met parameterbinding
    try:
        cursor.execute(insert_query, (klantnaam, actie, datumtijd))
        conn.commit() 
    except Exception as e:
        print(f"Fout bij het toevoegen van waarden: {e}")
    
    