import logging
import pyodbc
import struct
import time
import msal

class ConnectorError(Exception):
    pass

class Connector():
    def get_azure_sql_access_token(self, tenant_id, client_id, client_secret):
        
        authority = f"https://login.microsoftonline.com/{tenant_id}"
        app = msal.ConfidentialClientApplication(
            client_id,
            authority=authority,
            client_credential=client_secret
        )
        token_response = app.acquire_token_for_client(scopes=["https://database.windows.net//.default"])
        access_token = token_response.get("access_token")
        if not access_token:
            logging.error("Access token ophalen mislukt.")
            raise ConnectorError("Access token ophalen mislukt.")
        
        token_bytes = bytes(access_token, "UTF-8")
        exptoken = b""
        for i in token_bytes:
            exptoken += bytes({i})
            exptoken += bytes(1)
        token_struct = struct.pack("=i", len(exptoken)) + exptoken

        return token_struct

def connect_to_database(self, connectie_string, auth_method="AzureSQL", tenant_id=None, client_id=None, client_secret=None, max_retries=3, retry_delay=5):
    """
    Maakt verbinding met de SQL database met verschillende authenticatiemethoden.
    
    Args:
        connection_string: De database connectie string
        auth_method: De authenticatiemethode ("AzureSQL" of "AzureSQLMEI")
        tenant_id: Microsoft Entra ID tenant ID (alleen nodig bij AzureSQLMEI)
        client_id: Microsoft Entra ID client ID (alleen nodig bij AzureSQLMEI)
        client_secret: Microsoft Entra ID client secret (alleen nodig bij AzureSQLMEI)
        max_retries: Maximum aantal pogingen voor verbinding
        retry_delay: Wachttijd tussen pogingen in seconden
        
    Returns:
        Database connectie of None bij fout
    """
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    token_struct = None
    
    for attempt in range(max_retries):
        try:
            if auth_method.upper() == "AzureSQLMEI":
                if not all([tenant_id, client_id, client_secret]):
                    logging.error("Bij AzureSQLMEI authenticatie zijn tenant_id, client_id en client_secret verplicht")
                    raise ValueError("Bij AzureSQLMEI authenticatie zijn tenant_id, client_id en client_secret verplicht")
                
                if not token_struct:  # Haal token op bij de eerste poging of als het vervallen is
                    token_struct = self.get_azure_sql_access_token(tenant_id, client_id, client_secret)
                
                conn = pyodbc.connect(connectie_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
            else:  # SQL authenticatie
                conn = pyodbc.connect(connectie_string)
            
            return conn
            
        except pyodbc.OperationalError as e:
            if auth_method.upper() == "AzureSQLMEI" and "Expired" in str(e):
                logging.info("Token is verlopen, vernieuwen...")
                token_struct = self.get_azure_sql_access_token(tenant_id, client_id, client_secret)
                continue  # Probeer opnieuw met het vernieuwde token
            else:
                logging.error(f"Fout bij poging {attempt + 1} om verbinding te maken: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logging.error("Kan geen verbinding maken met de database na meerdere pogingen.")
                    raise ConnectorError("Kan geen verbinding maken met de database na meerdere pogingen.")