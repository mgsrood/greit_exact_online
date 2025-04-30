from nmbrs.modules.tokens import get_new_tokens, save_refresh_token, save_access_token
import pandas as pd
import requests
import logging
import time

def get_debtor_list(config_manager, connection_string):
    """
    Haalt de lijst van debiteuren op van Nmbrs via REST API.
    
    Args:
        config_manager: Instantie van ConfigManager
        connection_string: Connectiestring voor de database
        
    Returns:
        pd.DataFrame: DataFrame met debiteuren data
    """
    try:
        # Configuraties ophalen
        config_dict = config_manager.get_configurations(connection_string)
        if not config_dict:
            logging.error("Configuratiegegevens konden niet worden opgehaald")
            return None

        # Variabelen definiëren
        access_token = config_dict['access_token']
        subscription_key = config_dict['subscription_key']
        
        base_url = "https://api.nmbrsapp.com/api/debtors"
        all_data = []
        
        headers = {
            "X-Subscription-Key": f"{subscription_key}",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        
        # Retry loop voor GET request
        for attempt in range(3):
            try:
                current_page = 1
                total_pages = None
                
                while total_pages is None or current_page <= total_pages:
                    url = f"{base_url}?page={current_page}"
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    
                    response_data = response.json()
                    
                    if not response_data or 'data' not in response_data:
                        logging.info("Geen debiteuren gevonden")
                        return pd.DataFrame()
                    
                    # Verwerk de data array uit de response
                    data = response_data['data']
                    all_data.extend(data)
                    
                    # Update paginering informatie
                    if total_pages is None and 'pagination' in response_data:
                        total_pages = response_data['pagination']['totalPages']
                    
                    logging.info(f"Pagina {current_page} van {total_pages} opgehaald: {len(data)} debiteuren")
                    
                    if current_page >= total_pages:
                        break
                        
                    current_page += 1
                    time.sleep(1)  # Rate limiting
                
                # Maak een DataFrame van alle verzamelde data
                df = pd.DataFrame(all_data)
                
                logging.info(f"Totaal aantal debiteuren opgehaald: {len(df)}")
                return df
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 503:
                    logging.info(f"Server tijdelijk onbeschikbaar, poging {attempt + 1} van 3")
                    time.sleep(300)
                    continue
                    
                elif response.status_code == 401:
                    logging.info("Token verlopen, nieuwe tokens ophalen")
                    new_access_token, new_refresh_token = get_new_tokens(
                        refresh_token=config_dict['refresh_token'],
                        client_id=config_dict['client_id'],
                        client_secret=config_dict['client_secret'],
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
        logging.error(f"Fout tijdens ophalen debiteuren: {e}")
        return None
        
def get_companies_per_debtor(config_manager, connection_string, debtor_id):
    """
    Haalt de lijst van bedrijven op voor een specifieke debiteur van Nmbrs via REST API.
    
    Args:
        config_manager: Instantie van ConfigManager
        connection_string: Connectiestring voor de database
        debtor_id: ID van de debiteur
        
    Returns:
        pd.DataFrame: DataFrame met bedrijven data
    """
    try:
        # Configuraties ophalen
        config_dict = config_manager.get_configurations(connection_string)
        if not config_dict:
            logging.error("Configuratiegegevens konden niet worden opgehaald")
            return None

        # Variabelen definiëren
        access_token = config_dict['access_token']
        subscription_key = config_dict['subscription_key']
        
        base_url = f"https://api.nmbrsapp.com/api/debtors/{debtor_id}/companies"
        
        headers = {
            "X-Subscription-Key": f"{subscription_key}",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        
        # Retry loop voor GET request
        for attempt in range(3):
            try:
                response = requests.get(base_url, headers=headers)
                response.raise_for_status()
                
                response_data = response.json()
                
                if not response_data or 'data' not in response_data:
                    logging.info(f"Geen bedrijven gevonden voor debiteur {debtor_id}")
                    return pd.DataFrame()
                
                # Haal de bedrijven data op uit de response
                companies_data = response_data['data']
                
                # Maak een DataFrame van de data
                df = pd.DataFrame(companies_data)
        
                logging.info(f"Aantal bedrijven opgehaald voor debiteur {debtor_id}: {len(df)}")
                return df
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 503:
                    logging.info(f"Server tijdelijk onbeschikbaar, poging {attempt + 1} van 3")
                    time.sleep(300)
                    continue
                    
                elif response.status_code == 401:
                    logging.info("Token verlopen, nieuwe tokens ophalen")
                    new_access_token, new_refresh_token = get_new_tokens(
                        refresh_token=config_dict['refresh_token'],
                        client_id=config_dict['client_id'],
                        client_secret=config_dict['client_secret'],
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
        logging.error(f"Fout tijdens ophalen bedrijven voor debiteur {debtor_id}: {e}")
        return None
        
def get_employee_schedules(config_manager, connection_string, company_id):
    """
    Haalt de FTE data op voor een specifiek bedrijf van Nmbrs via REST API.
    
    Args:
        config_manager: Instantie van ConfigManager
        connection_string: Connectiestring voor de database
        company_id: ID van het bedrijf
        
    Returns:
        pd.DataFrame: DataFrame met FTE data
    """
    try:
        # Configuraties ophalen
        config_dict = config_manager.get_configurations(connection_string)
        if not config_dict:
            logging.error("Configuratiegegevens konden niet worden opgehaald")
            return None

        # Variabelen definiëren
        access_token = config_dict['access_token']
        subscription_key = config_dict['subscription_key']
        
        base_url = f"https://api.nmbrsapp.com/api/companies/{company_id}/employees/schedules"
        all_data = []
        total_schedules = 0
        
        headers = {
            "X-Subscription-Key": f"{subscription_key}",
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}"
        }
        
        # Retry loop voor GET request
        for attempt in range(3):
            try:
                current_page = 1
                total_pages = None
                
                while total_pages is None or current_page <= total_pages:
                    url = f"{base_url}?pageNumber={current_page}&pageSize=20"
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    
                    response_data = response.json()
                    
                    if not response_data or 'data' not in response_data:
                        logging.info(f"Geen FTE gevonden voor bedrijf {company_id}")
                        return pd.DataFrame()
                    
                    # Verwerk de data array uit de response
                    data = response_data['data']
                    all_data.extend(data)
                    
                    # Tel het aantal schema's per werknemer
                    for employee in data:
                        if 'schedules' in employee:
                            total_schedules += len(employee['schedules'])
                    
                    # Update paginering informatie
                    if total_pages is None and 'pagination' in response_data:
                        total_pages = response_data['pagination']['totalPages']
                    
                    logging.info(f"Pagina {current_page} van {total_pages} opgehaald: {len(data)} werknemers")
                    
                    if current_page >= total_pages:
                        break
                        
                    current_page += 1
                    time.sleep(1)  # Rate limiting
                
                # Maak een DataFrame van alle verzamelde data
                df = pd.DataFrame(all_data)
                
                # Voeg CompanyID toe aan de DataFrame
                df['CompanyID'] = company_id
                
                # Klap de schedules uit
                if 'schedules' in df.columns:
                    # Explode de schedules kolom en reset de index om de mapping te behouden
                    df = df.explode('schedules').reset_index(drop=True)
                    
                    # Maak een nieuwe DataFrame van de schedules data
                    schedules_df = pd.json_normalize(df['schedules'])
                    
                    # Reset de index om de mapping te behouden
                    schedules_df = schedules_df.reset_index(drop=True)
                    
                    # Hernoem kolommen om duidelijk te maken dat ze bij het schema horen
                    schedule_columns = {col: f"schedule_{col}" for col in schedules_df.columns}
                    schedules_df = schedules_df.rename(columns=schedule_columns)
                    
                    # Voeg de basis kolommen toe aan elke rij
                    schedules_df['CompanyID'] = df['CompanyID']
                    schedules_df['employeeId'] = df['employeeId']
                    
                    # Verwijder de originele schedules kolom
                    df = schedules_df
                
                # Klap week1 en week2 uit naar aparte kolommen
                for week in ['week1', 'week2']:
                    if week in df.columns:
                        # Maak een nieuwe DataFrame van de week data
                        week_df = pd.json_normalize(df[week])
                        
                        # Hernoem kolommen voor deze week
                        week_columns = {col: f"{week}_{col}" for col in week_df.columns}
                        week_df = week_df.rename(columns=week_columns)
                        
                        # Voeg de nieuwe kolommen toe aan het originele DataFrame
                        df = pd.concat([df, week_df], axis=1)
                        
                        # Verwijder de originele week kolom
                        df = df.drop(columns=[week])
                
                logging.info(f"Totaal aantal FTE opgehaald voor bedrijf {company_id}: {total_schedules}")
                return df
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 503:
                    logging.info(f"Server tijdelijk onbeschikbaar, poging {attempt + 1} van 3")
                    time.sleep(300)
                    continue
                    
                elif response.status_code == 401:
                    logging.info("Token verlopen, nieuwe tokens ophalen")
                    new_access_token, new_refresh_token = get_new_tokens(
                        refresh_token=config_dict['refresh_token'],
                        client_id=config_dict['client_id'],
                        client_secret=config_dict['client_secret'],
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
        logging.error(f"Fout tijdens ophalen FTE voor bedrijf {company_id}: {e}")
        return None
        
        
        