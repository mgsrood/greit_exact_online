from utils.config import ConfigManager
import requests
import logging

def get_new_tokens(refresh_token, client_id, client_secret, config_manager):
    """
    Verkrijg nieuwe tokens van Nmbrs.
    
    Args:
        refresh_token: Huidige refresh token
        client_id: Client ID voor Nmbrs
        client_secret: Client Secret voor Nmbrs
        config_manager: Instantie van ConfigManager
    """
    # Endpoint voor het verkrijgen van een nieuwe access token
    token_url = "https://identityservice.nmbrs.com/connect/token"
    
    # Parameters voor het vernieuwen van het token
    payload = f'grant_type=refresh_token&redirect_uri=https%3A%2F%2Ffinnit.nl%2F&client_id={client_id}&client_secret={client_secret}&refresh_token={refresh_token}'
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Origin": "https://finnit.nl",
        "Referer": "https://finnit.nl/",
        "Connection": "keep-alive"
    }

    try:
        # Request maken om een nieuwe access token te verkrijgen
        response = requests.post(token_url, headers=headers, data=payload)
        response.raise_for_status()
        
        # Het JSON-antwoord parsen om de nieuwe access token en refresh token te krijgen
        tokens = response.json()
        new_access_token = tokens["access_token"]
        new_refresh_token = tokens["refresh_token"]  # Nmbrs geeft altijd een nieuwe refresh token
        
        logging.info("Nieuwe tokens succesvol opgehaald van Nmbrs")
        return new_access_token, new_refresh_token
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Fout bij het vernieuwen van het toegangstoken: {str(e)}")
        if hasattr(response, 'text'):
            logging.error(f"Response tekst: {response.text}")
        return None, None

def save_refresh_token(config_manager, connection_string, new_refresh_token):
    """
    Sla het nieuwe refresh token op in de database.
    
    Args:
        config_manager: Instantie van ConfigManager
        connection_string: De te gebruiken connectiestring
        new_refresh_token: Het nieuwe refresh token om op te slaan
    """
    try:
        with config_manager.get_connection(connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute('UPDATE Config SET Waarde = ? WHERE Config = ?',
                             (new_refresh_token, 'refresh_token'))
                conn.commit()
                logging.info("Refresh token succesvol bijgewerkt")
    except Exception as e:
        logging.error(f"Fout bij bijwerken refresh token: {str(e)}")

def save_access_token(config_manager, connection_string, new_access_token):
    """
    Sla het nieuwe access token op in de database.
    
    Args:
        config_manager: Instantie van ConfigManager
        connection_string: De te gebruiken connectiestring
        new_access_token: Het nieuwe access token om op te slaan
    """
    try:
        with config_manager.get_connection(connection_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute('UPDATE Config SET Waarde = ? WHERE Config = ?',
                             (new_access_token, 'access_token'))
                conn.commit()
                logging.info("Access token succesvol bijgewerkt")
    except Exception as e:
        logging.error(f"Fout bij bijwerken access token: {str(e)}") 