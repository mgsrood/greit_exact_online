from afas_modules.log import log
import pandas as pd
import requests

def get_request(api_string, token, endpoint):
    
    # Set up headers for authentication
    headers = {
        "Authorization": f"AfasToken {token}",
        "Content-Type": "application/json"
    }
    
    # Define other parameters
    url = f"{api_string}{endpoint}"
    
    take = -1  
    skip = -1 
    all_data = [] 
    
    page_count = 0  
    total_rows = 0  
    
    params = {
        "skip": skip,
        "take": take
    }
    
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        
        # Rijen uit data halen
        rows = data.get("rows", [])
        
        if not rows:
            return None

        # Rijen toevoegen aan data lijst, totaal aantal rijen opslaan
        all_data.extend(rows)  
        total_rows += len(rows)
        
    else:
        print(f"Failed to retrieve data from AFAS. Status code: {response.status_code}")
        return None

    # Convert the collected data into a DataFrame
    df = pd.DataFrame(all_data)

    # Print the total number of rows retrieved
    print(f"Total rows retrieved: {total_rows}")

    return df

def execute_get_request(api_string, token, connector, finn_it_connection_string, tabel, klantnaam, script_id, script):

    print("Start GET Request")
    
    # Uitvoeren GET Request
    df = get_request(api_string, token, connector)

    if df is None:
        # Foutmelding log
        print(f"FOUTMELDING | Fout bij het ophalen van data voor tabel: {tabel} | {klantnaam}")
        log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het ophalen van data", script_id, script)
        return None, True  # True voor errors_occurred

    elif df.empty:
        # Geen data opgehaald, maar geen error
        print(f"Geen data opgehaald voor tabel: {tabel} | {klantnaam}")
        log(finn_it_connection_string, klantnaam, f"Geen data opgehaald", script_id, script)
        return None, False  # False, geen fout maar geen data

    else:
        # Succes log
        print(f"Ophalen DataFrame gelukt voor tabel: {tabel} | {klantnaam}")
        log(finn_it_connection_string, klantnaam, f"Ophalen DataFrame gelukt", script_id, script)
        return df, False  # False, geen fout en data succesvol opgehaald