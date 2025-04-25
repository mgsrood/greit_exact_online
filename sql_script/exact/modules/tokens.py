from utils.config import ConfigManager
import requests
import logging

def get_new_tokens(refresh_token, client_id, client_secret, config_manager):
    """
    Verkrijg nieuwe tokens van Exact Online.
    
    Args:
        refresh_token: Huidige refresh token
        client_id: Client ID voor Exact Online
        client_secret: Client Secret voor Exact Online
        config_manager: Instantie van ConfigManager
    """
    # Endpoint voor het verkrijgen van een nieuwe access token
    token_url = "https://start.exactonline.nl/api/oauth2/token"
    
    # Parameters voor het vernieuwen van het token
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret
    }
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"  # Vereiste header voor OAuth 2.0
    }

    try:
        # Request maken om een nieuwe access token te verkrijgen
        response = requests.post(token_url, data=payload, headers=headers)
        response.raise_for_status()  # Raise exception voor niet-200 status codes
        
        # Het JSON-antwoord parsen om de nieuwe access token en refresh token te krijgen
        tokens = response.json()
        new_access_token = tokens["access_token"]
        new_refresh_token = tokens.get("refresh_token", refresh_token)  # Nieuwe refresh token of behoud de oude
        
        logging.info("Nieuwe tokens succesvol opgehaald van Exact Online")
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