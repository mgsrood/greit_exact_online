def fetch_all_connection_strings(cursor):
    # Voer de query uit om alle connectiestrings op te halen
    query = 'SELECT * FROM Klanten'
    cursor.execute(query)
    
    # Verkrijg alle rijen uit de resultaten
    rows = cursor.fetchall()
    
    # Extract de connectiestrings uit de resultaten
    connection_dict = {row[1]: row[2] for row in rows}  
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