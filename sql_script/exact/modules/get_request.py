from exact.modules.tokens import get_new_tokens, save_refresh_token, save_access_token
import pandas as pd
import requests
import logging
import time

def current_division_call(config_manager, url, connection_string):
    """
    Haalt de huidige divisie op van Exact Online.
    
    Args:
        config_manager: Instantie van ConfigManager
        url: Basis URL voor Exact Online API
        connection_string: Connectiestring voor de database
    """
    endpoint = "current/Me?$select=CurrentDivision"
    full_url = url + endpoint
    
    logging.info("Start Current Division Request")
    
    try:
        # Configuraties ophalen
        config_dict = config_manager.get_configurations(connection_string)
        if not config_dict:
            logging.error("Configuratiegegevens konden niet worden opgehaald")
            return None
    
        # Variabelen definiëren
        access_token = config_dict['access_token']
        refresh_token = config_dict['refresh_token']
        client_id = config_dict['client_id']
        client_secret = config_dict['client_secret']
    
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        # Retry loop voor GET request
        for attempt in range(3):
            try:
                response = requests.get(full_url, headers=headers)
                response.raise_for_status()
        
                response_json = response.json()
                current_division = response_json['d']['results'][0]['CurrentDivision']
                logging.info(f"Current division succesvol opgehaald: {current_division}")
                return current_division
    
            except requests.exceptions.HTTPError as e:
                if response.status_code == 503:
                    logging.info(f"Server tijdelijk onbeschikbaar, poging {attempt + 1} van 3")
                    time.sleep(300)
                    continue

                elif response.status_code == 401:
                    logging.info("Token verlopen, nieuwe tokens ophalen")
                    new_access_token, new_refresh_token = get_new_tokens(
                        refresh_token=refresh_token,
                        client_id=client_id,
                        client_secret=client_secret,
                        config_manager=config_manager
                    )
                    
                    if not new_access_token or not new_refresh_token:
                        logging.error("Nieuwe tokens konden niet worden opgehaald")
                        return None
                        
                    # Tokens opslaan
                    save_refresh_token(config_manager, connection_string, new_refresh_token)
                    save_access_token(config_manager, connection_string, new_access_token)
                    
                    # Headers updaten en opnieuw proberen
                    headers['Authorization'] = f'Bearer {new_access_token}'
                    continue
                    
                else:
                    logging.error(f"HTTP error: {e}")
                    return None
            
            except Exception as e:
                logging.error(f"Onverwachte fout: {e}")
                return None
            
        logging.error("Maximaal aantal pogingen bereikt")
        return None
        
    except Exception as e:
        logging.error(f"Fout tijdens ophalen current division: {e}")
        return None

def divisions_call(config_manager, url, connection_string, current_division_code):
    """
    Haalt alle divisies op van Exact Online.
    
    Args:
        config_manager: Instantie van ConfigManager
        url: Basis URL voor Exact Online API
        connection_string: Connectiestring voor de database
        current_division_code: Code van de huidige divisie
    """
    logging.info("Start Divisions Request")
    
    try:
        # Configuraties ophalen
        config_dict = config_manager.get_configurations(connection_string)
        if not config_dict:
            logging.error("Configuratiegegevens konden niet worden opgehaald")
            return None

        # Variabelen definiëren
        access_token = config_dict['access_token']
        refresh_token = config_dict['refresh_token']
        client_id = config_dict['client_id']
        client_secret = config_dict['client_secret']
    
        endpoint = f"{current_division_code}/system/Divisions"
        full_url = url + endpoint
        data = []
    
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        while full_url:
            # Retry loop voor GET request
            for attempt in range(3):
                try:
                    response = requests.get(full_url, headers=headers)
                    response.raise_for_status()

                    response_json = response.json()
                    results = response_json.get('d', {}).get('results', [])
                    data.extend(results)
                    
                    # Check voor volgende pagina
                    full_url = response_json.get('d', {}).get('__next')
                    if full_url:
                        logging.info(f"Volgende pagina gevonden: {len(data)} records tot nu toe")
                    
                    time.sleep(1)  # Rate limiting
                    break
                    
                except requests.exceptions.HTTPError as e:
                    if response.status_code == 503:
                        logging.info(f"Server tijdelijk onbeschikbaar, poging {attempt + 1} van 3")
                        time.sleep(300)
                        continue
                        
                    elif response.status_code == 401:
                        logging.info("Token verlopen, nieuwe tokens ophalen")
                        new_access_token, new_refresh_token = get_new_tokens(
                            refresh_token=refresh_token,
                            client_id=client_id,
                            client_secret=client_secret,
                            config_manager=config_manager
                        )
                        
                        if not new_access_token or not new_refresh_token:
                            logging.error("Nieuwe tokens konden niet worden opgehaald")
                            return None
                            
                        # Tokens opslaan
                        save_refresh_token(config_manager, connection_string, new_refresh_token)
                        save_access_token(config_manager, connection_string, new_access_token)
                        
                        # Headers updaten en opnieuw proberen
                        headers['Authorization'] = f'Bearer {new_access_token}'
                        continue
                        
                    else:
                        logging.error(f"HTTP error: {e}")
                        return None
                
                except Exception as e:
                    logging.error(f"Onverwachte fout: {e}")
                    return None
                
        # Verwerk de data
        if not data:
            logging.info("Geen divisies gevonden")
            return pd.DataFrame()
            
        # DateFrames maken
        df = pd.DataFrame(data)
        division_df = pd.DataFrame()

        # Data doorlopen
        for index, row in df.iterrows():
            if row["Class_02"] is None:
                
                # Variabelen definiëren
                entiteit = row['Description']
                division = row["Code"]
                customer_name = None
                company_size_description = row['CompanySizeDescription']
                state = row['State']
                city = row['City']
                hid = row['Hid']
                customer_code = row['CustomerCode']
            
            else:
                # Entiteit ophalen
                class_data = row["Class_02"]
                customer_name = class_data.get('Description')
                
                # Variabelen definiëren
                division = row["Code"]
                entiteit = row['Description']
                company_size_description = row['CompanySizeDescription']
                state = row['State']
                city = row['City']
                hid = row['Hid']
                customer_code = row['CustomerCode']
            
            # Divisie DataFrame vullen
            division_df = pd.concat([division_df, pd.DataFrame({
                "Entiteit": [entiteit],
                "Division": [division],
                "CustomerCode": [customer_code],
                "CustomerName": [customer_name],
                "CompanySizeDescription": [company_size_description],
                "State": [state],
                "City": [city],
                "HID": [hid]
            })])
        
        logging.info(f"Divisions data succesvol verwerkt: {len(division_df)} relevante divisies gevonden")
        return division_df

    except Exception as e:
        logging.error(f"Fout tijdens ophalen divisions: {e}")
        return None

def get_request(config_manager, division_code, url, endpoint, connection_string, tabel):
    """
    Voert een GET request uit naar de Exact Online API.
    
    Args:
        config_manager: Instantie van ConfigManager
        division_code: Code van de divisie
        url: Basis URL voor Exact Online API
        endpoint: API endpoint
        connection_string: Connectiestring voor de database
        tabel: Naam van de tabel waarvoor data wordt opgehaald
    """
    full_url = f"{url}{endpoint}"
    data = []
    
    logging.info(f"Start GET Request voor tabel: {tabel} | {division_code}")
    
    try:
        # Configuraties ophalen
        config_dict = config_manager.get_configurations(connection_string)

        if not config_dict:
            logging.error("Configuratiegegevens konden niet worden opgehaald")
            return None

        # Variabelen definiëren
        access_token = config_dict['access_token']
        refresh_token = config_dict['refresh_token']
        client_id = config_dict['client_id']
        client_secret = config_dict['client_secret']
    
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        while full_url:
            # Retry loop voor GET request
            for attempt in range(3):
                try:
                    response = requests.get(full_url, headers=headers)
                    response.raise_for_status()

                    response_json = response.json()
                
                    # Verwerk JSON response
                    if 'd' in response_json and 'results' in response_json['d']:
                        results = response_json['d']['results']
                        data.extend(results)
                        
                        # Controleer op volgende pagina
                        full_url = response_json['d'].get('__next')
                        if full_url:
                            logging.info(f"Volgende pagina gevonden: {len(data)} records tot nu toe")
                    else:
                        # Directe JSON response zonder 'd' wrapper
                        data.append(response_json)
                        full_url = None

                    time.sleep(1)  # Rate limiting
                    break
                    
                except requests.exceptions.HTTPError as e:
                    if response.status_code == 503:
                        logging.info(f"Server tijdelijk onbeschikbaar, poging {attempt + 1} van 3")
                        time.sleep(300)
                        continue
                        
                    elif response.status_code == 401:
                        logging.info("Token verlopen, nieuwe tokens ophalen")
                        new_access_token, new_refresh_token = get_new_tokens(
                            refresh_token=refresh_token,
                            client_id=client_id,
                            client_secret=client_secret,
                            config_manager=config_manager
                        )
                        
                        if not new_access_token or not new_refresh_token:
                            logging.error("Nieuwe tokens konden niet worden opgehaald")
                            return None
                            
                        # Tokens opslaan
                        save_refresh_token(config_manager, connection_string, new_refresh_token)
                        save_access_token(config_manager, connection_string, new_access_token)
                        
                        # Headers updaten en opnieuw proberen
                        headers['Authorization'] = f'Bearer {new_access_token}'
                        continue
                        
                    else:
                        logging.error(f"HTTP error: {e}")
                        return None
                
                except Exception as e:
                    logging.error(f"Onverwachte fout: {e}")
                    return None
        
        # Verwerk de data
        if not data:
            logging.info(f"Geen data gevonden voor tabel {tabel}")
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        logging.info(f"Data succesvol opgehaald voor tabel {tabel}: {len(df)} records")
        return df
    
    except Exception as e:
        logging.error(f"Fout tijdens ophalen data voor tabel {tabel}: {e}")
        return None

def execute_get_request(config_manager, division_code, url, endpoint, connection_string, tabel, division_name):
    """
    Voert een GET request uit en handelt de resultaten af.
    
    Args:
        config_manager: Instantie van ConfigManager
        division_code: Code van de divisie
        url: Basis URL voor Exact Online API
        endpoint: API endpoint
        connection_string: Connectiestring voor de database
        tabel: Naam van de tabel waarvoor data wordt opgehaald
        division_name: Naam van de divisie
    """
    # Uitvoeren GET Request
    df = get_request(config_manager, division_code, url, endpoint, connection_string, tabel)

    if df is None:
        logging.error(f"Fout bij het ophalen van data voor tabel: {tabel} | {division_name} ({division_code})")
        return None, True  # True voor errors_occurred

    elif df.empty:
        logging.info(f"Geen data opgehaald voor tabel: {tabel} | {division_name} ({division_code})")
        return None, False  # False, geen fout maar geen data

    else:
        logging.info(f"Data succesvol opgehaald voor tabel: {tabel} | {division_name} ({division_code})")
        return df, False  # False, geen fout en data succesvol opgehaald
    
def execute_divisions_call(config_manager, url, connection_string, current_division_code):
    """
    Voert een divisions call uit en handelt de resultaten af.
    
    Args:
        config_manager: Instantie van ConfigManager
        url: Basis URL voor Exact Online API
        connection_string: Connectiestring voor de database
        current_division_code: Code van de huidige divisie
    """
    # Uitvoeren GET Request
    df = divisions_call(config_manager, url, connection_string, current_division_code)

    if df is None:
        logging.error("Fout bij het ophalen van divisions data")
        return None, True  # True voor errors_occurred

    elif df.empty:
        logging.info("Geen divisions data opgehaald")
        return None, False  # False, geen fout maar geen data
    
    else:
        logging.info(f"Divisions data succesvol opgehaald: {len(df)} divisies")
        return df, False  # False, geen fout en data succesvol opgehaald