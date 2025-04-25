from ex_modules.tokens import get_new_tokens, save_refresh_token, save_access_token
from ex_modules.database import connect_to_database
from ex_modules.config import fetch_configurations
from ex_modules.log import log
import pandas as pd
import requests
import time

def current_division_call(token_struct, url, connection_string, finn_it_connection_string, klantnaam, script_id, script):
    print(f"Connection string: {connection_string}")
    print("Start current division call")
    # Endpoint definiëren
    endpoint = "current/Me?$select=CurrentDivision"

    # Full URL definiëren
    full_url = url + endpoint
    
    # Start log
    print(f"Start Current Division Request voor tabel")
    log(token_struct, finn_it_connection_string, klantnaam, f"Start Current Division Request", script_id, script)
    log(token_struct, finn_it_connection_string, klantnaam, f"Ophalen configuratiegegevens", script_id, script)
    
    # Ophalen configuratie gegevens
    try:
        config_conn = connect_to_database(connection_string, token_struct)
        print("Config conn succesvol opgehaald")
    except Exception as e:
        print(f"Fout bij het connecten met de database: {e}")
        return None
    
    if config_conn:
        cursor = config_conn.cursor()
        config_dict = None
        for attempt in range(3):
            try:
                config_dict = fetch_configurations(cursor)
                if config_dict:
                    break
            except Exception as e:
                time.sleep(5)

        if config_dict is None:
            # Fout log
            print("Fout bij het ophalen van de configuratiegegevens.")
            log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het ophalen van de configuratiegegevens", script_id, script)
            return None
        else:
            # Succes log
            log(token_struct, finn_it_connection_string, klantnaam, f"Configuratiegegevens succesvol opgehaald", script_id, script)
    else:
        # Foutmelding log
        print(f"Fout bij het connecten met de database.")
        log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het connecten met de database", script_id, script)
        return None
    
    # Variabelen definiëren
    client_secret = config_dict['client_secret']
    client_id = config_dict['client_id']
    access_token = config_dict['access_token']
    refresh_token = config_dict['refresh_token']
    
    # Request variabelen
    payload = {}
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    # Retry loop voor GET request
    for attempt in range(3):
        response = requests.request("GET", full_url, headers=headers, data=payload)
        
        # Controleer of de request succesvol was
        if response.status_code == 200:
            response_json = response.json()
            current_division = response_json['d']['results'][0]['CurrentDivision']
            return current_division
    
        elif response.status_code == 503:
            print(f"Server is tijdelijk onbeschikbaar, poging {attempt + 1} van 3. Wacht 5 minuten...")
            time.sleep(300)  # Wacht 5 seconden voordat je opnieuw probeert

        elif response.status_code == 401:
            print(f"Fout bij het ophalen van gegevens: {response.status_code} - {response.text}")
            log(token_struct, finn_it_connection_string, klantnaam, f"Ophalen nieuwe access- en refresh token", script_id, script)
                
            # Ophalen nieuwe tokens
            oude_refresh_token = refresh_token
            client_id = client_id
            client_secret = client_secret
            new_access_token, new_refresh_token = get_new_tokens(oude_refresh_token, client_id, client_secret, finn_it_connection_string, klantnaam)
            print(f"New refresh token: {new_refresh_token}")
            print(f"New access token: {new_access_token}")
            if new_refresh_token:
                save_refresh_token(token_struct, connection_string, new_refresh_token)
                refresh_token = new_refresh_token

                # Succes log
                log(token_struct, finn_it_connection_string, klantnaam, f"Nieuwe refresh token successvol opgehaald en opgeslagen", script_id, script)

                # Opslaan nieuwe access token
                if new_access_token:
                    # Voer hier je verzoek uit met de nieuwe access token
                    save_access_token(token_struct, connection_string, new_access_token)
                    access_token = new_access_token

                    # Succes log
                    print(f"Nieuwe access token successvol opgehaald en opgeslagen")
                    log(token_struct, finn_it_connection_string, klantnaam, f"Nieuwe access token successvol opgehaald en opgeslagen", script_id, script)
                    
                    headers = {
                        'Authorization': f'Bearer {access_token}',
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                    
                    response = requests.request("GET", full_url, headers=headers, data=payload)
        
                    # Controleer of de request succesvol was
                    if response.status_code == 200:
                        response_json = response.json()
                        current_division = response_json['d']['results'][0]['CurrentDivision']
                        return current_division
                    
                    else:
                        print(f"Error {response.status_code}: {response.text}")
                        log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Exact Online API foutmelding | Status code: {response.status_code}", script_id, script)
                        return None
            
                # Foutmelding log
                log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Nieuwe access token niet kunnen ophalen", script_id, script)
                return None
            
            else:
                # Foutmelding log
                log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Nieuwe refresh token niet kunnen ophalen", script_id, script)
                return None
        
        else:
            print(f"Error {response.status_code}: {response.text}")
            log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Exact Online API foutmelding | Status code: {response.status_code}", script_id, script)
            return None

def divisions_call(token_struct, url, connection_string, current_division_code, finn_it_connection_string, klantnaam, script_id, script):
    # Start log
    print(f"Start Divisions Request")
    log(token_struct, finn_it_connection_string, klantnaam, f"Start Divisions Request", script_id, script)
    log(token_struct, finn_it_connection_string, klantnaam, f"Ophalen configuratiegegevens", script_id, script)
    
    # Ophalen configuratie gegevens
    config_conn = connect_to_database(connection_string, token_struct)

    if config_conn:
        cursor = config_conn.cursor()
        config_dict = None
        for attempt in range(3):
            try:
                config_dict = fetch_configurations(cursor)
                if config_dict:
                    break
            except Exception as e:
                time.sleep(5)

        if config_dict is None:
            # Fout log
            print("Fout bij het ophalen van de configuratiegegevens.")
            log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het ophalen van de configuratiegegevens", script_id, script)
            return None
        else:
            # Succes log
            log(token_struct, finn_it_connection_string, klantnaam, f"Configuratiegegevens succesvol opgehaald", script_id, script)
    else:
        # Foutmelding log
        print(f"Fout bij het connecten met de database.")
        log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het connecten met de database", script_id, script)
        return None

    # Variabelen definiëren
    client_secret = config_dict['client_secret']
    client_id = config_dict['client_id']
    access_token = config_dict['access_token']
    refresh_token = config_dict['refresh_token']
    
    endpoint = f"{current_division_code}/system/Divisions"
    full_url = url + endpoint
    data = []
    total_rows = 0 
    
    while full_url:
        payload = {}
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Cookie': f'.AspNetCore.Antiforgery.cdV5uW_Ejgc=CfDJ8CTJVFXxHxJHqt315CGWnt6RmoANCHzuwWq9U7Hje9I3wCAI4LZuudgNgWB6dYyMvEmg32OtzGlkiXWwnahptcAkcALB6KJT_gEvyE6MVNsWYGaWCvjmIDAtTJaRIoAFFsgnc8-ZLrEq13YMkITaGlg; .AspNetCore.Culture=c%3Dnl%7Cuic%3Dnl; ARRAffinity=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ARRAffinitySameSite=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ASP.NET_SessionId=3esxcu3yqpsi31zurmgciobc; ExactOnlineClient=7LjlWUfX5lQbrehxxcOgynPtG4hwzKvhbdRrllvl9LzDBusePiI7bj3hvRnEiEC15zVDQrffSDkobPKR/bxTiiFmlkNu4odr6q55xN5eyQQd6dQzIORCNib+1rPe5uyf55F2LyZLmpDtrMt1lYIyYU+Y8+fr/PyNF2VDkL5wcpQ=; ExactServer{{2bd296c3-cdda-4a9b-9dec-0aad425078e1}}=Division={current_division_code}'
        }
        
        # Retry loop voor GET request
        for attempt in range(3):
            print(f"Poging {attempt + 1} van 3")
            response = requests.request("GET", full_url, headers=headers, data=payload)

            # Controleer of de request succesvol was
            if response.status_code == 200:
                response_json = response.json()

                # Voeg de resultaten toe aan de data-lijst
                data.extend(response_json.get('d', {}).get('results', []))
                total_rows += len(response_json.get('d', {}).get('results', []))
                
                # Controleer of er een volgende pagina is
                next_link = response_json.get('d', {}).get('__next', None)
                if next_link:
                    print(f"Volgende pagina gevonden: {next_link}")
                    full_url = next_link  # Stel de volgende URL in voor de volgende request
                else:
                    print("Geen volgende pagina gevonden.")
                    full_url = None  # Geen volgende pagina, stop de loop
                break
        
            elif response.status_code == 503:
                print(f"Server is tijdelijk onbeschikbaar, poging {attempt + 1} van 3. Wacht 5 minuten...")
                time.sleep(300)  # Wacht 5 seconden voordat je opnieuw probeert

            elif response.status_code == 401:
                print(f"Fout bij het ophalen van gegevens: {response.status_code} - {response.text}")
                log(token_struct, finn_it_connection_string, klantnaam, f"Ophalen nieuwe access- en refresh token", script_id, script)
                    
                # Ophalen nieuwe tokens
                oude_refresh_token = refresh_token
                client_id = client_id
                client_secret = client_secret
                new_access_token, new_refresh_token = get_new_tokens(oude_refresh_token, client_id, client_secret, finn_it_connection_string, klantnaam)

                if new_refresh_token:
                    save_refresh_token(token_struct, connection_string, new_refresh_token)
                    refresh_token = new_refresh_token

                    # Succes log
                    log(token_struct, finn_it_connection_string, klantnaam, f"Nieuwe refresh token successvol opgehaald en opgeslagen", script_id, script)

                    # Opslaan nieuwe access token
                    if new_access_token:
                        # Voer hier je verzoek uit met de nieuwe access token
                        save_access_token(token_struct, connection_string, new_access_token)
                        access_token = new_access_token

                        # Succes log
                        print(f"Nieuwe access token successvol opgehaald en opgeslagen")
                        log(token_struct, finn_it_connection_string, klantnaam, f"Nieuwe access token successvol opgehaald en opgeslagen", script_id, script)
                        
                        headers = {
                            'Authorization': f'Bearer {access_token}',
                            'Content-Type': 'application/json',
                            'Accept': 'application/json'
                        }
                        
                        response = requests.request("GET", full_url, headers=headers, data=payload)                    
                        
                        if response.status_code == 200:
                            response_json = response.json()

                            # Voeg de resultaten toe aan de data-lijst
                            data.extend(response_json.get('d', {}).get('results', []))
                            total_rows += len(response_json.get('d', {}).get('results', []))
                            
                            # Controleer of er een volgende pagina is
                            next_link = response_json.get('d', {}).get('__next', None)
                            if next_link:
                                print(f"Volgende pagina gevonden: {next_link}")
                                full_url = next_link  # Stel de volgende URL in voor de volgende request
                            else:
                                print("Geen volgende pagina gevonden.")
                                full_url = None  # Geen volgende pagina, stop de loop
                            break
                        
                        else:
                            print(f"Error {response.status_code}: {response.text}")
                            log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Exact Online API foutmelding | Status code: {response.status_code}", script_id, script)
                            return None
                
                    # Foutmelding log
                    log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Nieuwe access token niet kunnen ophalen", script_id, script)
                    return None
                
                else:
                    # Foutmelding log
                    log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Nieuwe refresh token niet kunnen ophalen", script_id, script)
                    return None
            
            else:
                print(f"Error {response.status_code}: {response.text}")
                log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Exact Online API foutmelding | Status code: {response.status_code}", script_id, script)
                return None
    
    # DataFrame creëren
    if data:
        df = pd.DataFrame(data)
        division_df = pd.DataFrame()
    else:
        df = pd.DataFrame()
    
    for index, row in df.iterrows():
        # None overslaan
        if row["Class_02"] is None:
            continue
        
        # Description ophalen uit Class_02
        class_data = row["Class_02"]
        description = class_data.get('Description')

        # Division ophalen
        if description == "Herbergiers PowerBI":
            # Description als Entiteit, Code als CustomerCode, Description als CustomerName, CompanySizeDesciption als CompanySizeDescription, State als State, City als City, Hid als HID toevoegen
            division_df = pd.concat([division_df, pd.DataFrame({"Entiteit": [description], "CustomerCode": [row["Code"]], "CustomerName": [row["Description"]], "CompanySizeDescription": [row["CompanySizeDescription"]], "State": [row["State"]], "City": [row["City"]], "HID": [row["Hid"]]})])
    
    return division_df

def get_request(token_struct, division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel, script_id, script):
    full_url = f"{url}{endpoint}"
    data = []
    total_rows = 0 

    # Start log
    print(f"Start GET Request voor tabel: {tabel} | {division_code} ({division_code})")
    log(token_struct, finn_it_connection_string, klantnaam, f"Start GET Request", script_id, script, division_code, tabel)
    log(token_struct, finn_it_connection_string, klantnaam, f"Ophalen configuratiegegevens", script_id, script, division_code, tabel)

    # Ophalen configuratie gegevens
    config_conn = connect_to_database(connection_string, token_struct)

    if config_conn:
        cursor = config_conn.cursor()
        config_dict = None
        for attempt in range(3):
            try:
                config_dict = fetch_configurations(cursor)
                if config_dict:
                    break
            except Exception as e:
                time.sleep(5)

        if config_dict is None:
            # Fout log
            print("Fout bij het ophalen van de configuratiegegevens.")
            log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het ophalen van de configuratiegegevens", script_id, script, division_code, tabel)
            return None
        else:
            # Succes log
            log(token_struct, finn_it_connection_string, klantnaam, f"Configuratiegegevens succesvol opgehaald", script_id, script, division_code, tabel)
    else:
        # Foutmelding log
        print(f"Fout bij het connecten met de database: {tabel} | {division_code} ({division_code}).")
        log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het connecten met de database", script_id, script, division_code, tabel)
        return None

    # Variabelen definiëren
    client_secret = config_dict['client_secret']
    client_id = config_dict['client_id']
    access_token = config_dict['access_token']
    refresh_token = config_dict['refresh_token']
    
    # Request loop
    while full_url:
        payload = {}
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Cookie': f'.AspNetCore.Antiforgery.cdV5uW_Ejgc=CfDJ8CTJVFXxHxJHqt315CGWnt6RmoANCHzuwWq9U7Hje9I3wCAI4LZuudgNgWB6dYyMvEmg32OtzGlkiXWwnahptcAkcALB6KJT_gEvyE6MVNsWYGaWCvjmIDAtTJaRIoAFFsgnc8-ZLrEq13YMkITaGlg; .AspNetCore.Culture=c%3Dnl%7Cuic%3Dnl; ARRAffinity=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ARRAffinitySameSite=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ASP.NET_SessionId=3esxcu3yqpsi31zurmgciobc; ExactOnlineClient=7LjlWUfX5lQbrehxxcOgynPtG4hwzKvhbdRrllvl9LzDBusePiI7bj3hvRnEiEC15zVDQrffSDkobPKR/bxTiiFmlkNu4odr6q55xN5eyQQd6dQzIORCNib+1rPe5uyf55F2LyZLmpDtrMt1lYIyYU+Y8+fr/PyNF2VDkL5wcpQ=; ExactServer{{2bd296c3-cdda-4a9b-9dec-0aad425078e1}}=Division={division_code}'
        }

        # Retry loop voor GET request
        for attempt in range(3):
            response = requests.request("GET", full_url, headers=headers, data=payload)

            # Controleer of de request succesvol was
            if response.status_code == 200:
                response_json = response.json()
                
                # Verwerk JSON response
                if 'd' in response_json and 'results' in response_json['d']:
                    results = response_json['d']['results']
                    data.extend(results)
                    total_rows += len(results)
                    
                    # Controleer op volgende pagina
                    next_link = response_json['d'].get('__next')
                    full_url = next_link if next_link else None
                else:
                    # Directe JSON response zonder 'd' wrapper
                    data.append(response_json)
                    total_rows += 1
                    full_url = None

                # Wacht 1 seconde voordat je de volgende request doet
                print(f"Opgehaalde rijen tot nu toe: {total_rows}")
                time.sleep(1)
                break  # Stop de retry-lus bij een succesvolle response

            elif response.status_code == 503:
                print(f"Server is tijdelijk onbeschikbaar, poging {attempt + 1} van 3. Wacht 5 minuten...")
                time.sleep(300)  # Wacht 5 seconden voordat je opnieuw probeert

            elif response.status_code == 401:
                print(f"Fout bij het ophalen van gegevens: {response.status_code} - {response.text}")
                # log
                log(token_struct, finn_it_connection_string, klantnaam, f"Ophalen nieuwe access- en refresh token", script_id, script, division_code, tabel)
                
                # Ophalen nieuwe tokens
                oude_refresh_token = refresh_token
                client_id = client_id
                client_secret = client_secret
                new_access_token, new_refresh_token = get_new_tokens(oude_refresh_token, client_id, client_secret, finn_it_connection_string, klantnaam)

                if new_refresh_token:
                    save_refresh_token(token_struct, connection_string, new_refresh_token)
                    refresh_token = new_refresh_token

                    # Succes log
                    log(token_struct, finn_it_connection_string, klantnaam, f"Nieuwe refresh token successvol opgehaald en opgeslagen", script_id, script, division_code, tabel)

                    # Opslaan nieuwe access token
                    if new_access_token:
                        # Voer hier je verzoek uit met de nieuwe access token
                        save_access_token(token_struct, connection_string, new_access_token)
                        access_token = new_access_token

                        # Succes log
                        print(f"Nieuwe access token successvol opgehaald en opgeslagen")
                        log(token_struct, finn_it_connection_string, klantnaam, f"Nieuwe access token successvol opgehaald en opgeslagen", script_id, script, division_code, tabel)

                        headers = {
                            'Authorization': f'Bearer {access_token}',
                            'Content-Type': 'application/json',
                            'Accept': 'application/json'
                        }
                        
                        response = requests.request("GET", full_url, headers=headers, data=payload)  
                        
                        # Verwerk JSON response
                        if 'd' in response_json and 'results' in response_json['d']:
                            results = response_json['d']['results']
                            data.extend(results)
                            total_rows += len(results)
                            
                            # Controleer op volgende pagina
                            next_link = response_json['d'].get('__next')
                            full_url = next_link if next_link else None
                        else:
                            # Directe JSON response zonder 'd' wrapper
                            data.append(response_json)
                            total_rows += 1
                            full_url = None

                        # Wacht 1 seconde voordat je de volgende request doet
                        print(f"Opgehaalde rijen tot nu toe: {total_rows}")
                        time.sleep(1)
                        break  # Stop de retry-lus bij een succesvolle response    
                
                    # Foutmelding log
                    log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Nieuwe access token niet kunnen ophalen", script_id, script, division_code, tabel)
                    return None
                
                else:
                    # Foutmelding log
                    log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Nieuwe refresh token niet kunnen ophalen", script_id, script, division_code, tabel)
                    return None
            
            else:
                print(f"Error {response.status_code}: {response.text}")
                log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Exact Online API foutmelding | Status code: {response.status_code}", script_id, script, division_code, tabel)
                return None

    # Maak een DataFrame van de verzamelde gegevens
    if data:
        df = pd.DataFrame(data)

        # Succes log
        log(token_struct, finn_it_connection_string, klantnaam, f"Data succesvol opgehaald", script_id, script, division_code, tabel)

        return df
    
    else:
        df = pd.DataFrame()

        return df

def execute_get_request(token_struct, division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel, script_id, script, division_name):

    # Uitvoeren GET Request
    df = get_request(token_struct, division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel, script_id, script)

    if df is None:
        # Foutmelding log
        print(f"FOUTMELDING | Fout bij het ophalen van data voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
        log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het ophalen van data", script_id, script, division_code, tabel)
        return None, True  # True voor errors_occurred

    elif df.empty:
        # Geen data opgehaald, maar geen error
        print(f"Geen data opgehaald voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
        log(token_struct, finn_it_connection_string, klantnaam, f"Geen data opgehaald", script_id, script, division_code, tabel)
        return None, False  # False, geen fout maar geen data

    else:
        # Succes log
        log(token_struct, finn_it_connection_string, klantnaam, f"Ophalen DataFrame gelukt", script_id, script, division_code, tabel)
        return df, False  # False, geen fout en data succesvol opgehaald
    
def execute_divisions_call(token_struct, url, connection_string, current_division_code, finn_it_connection_string, klantnaam, script_id, script):
    
    # Uitvoeren GET Request
    df = divisions_call(token_struct, url, connection_string, current_division_code, finn_it_connection_string, klantnaam, script_id, script)

    if df is None:
        # Foutmelding log
        print(f"FOUTMELDING | Fout bij het ophalen van divisions data")
        log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het ophalen van divisions data", script_id, script)
        return None, True  # True voor errors_occurred

    elif df.empty:
        # Geen data opgehaald, maar geen error
        print(f"Geen divisions data opgehaald")
        log(token_struct, finn_it_connection_string, klantnaam, f"Geen divisions data opgehaald", script_id, script)
        return None, False  # False, geen fout maar geen data
    
    else:
        # Succes log
        log(token_struct, finn_it_connection_string, klantnaam, f"Divisions data succesvol opgehaald", script_id, script)
        return df, False  # False, geen fout en data succesvol opgehaald