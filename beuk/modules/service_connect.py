import pyodbc
import msal
import struct
import time

def get_azure_sql_access_token(tenant_id, client_id, client_secret):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=authority,
        client_credential=client_secret
    )
    token_response = app.acquire_token_for_client(scopes=["https://database.windows.net//.default"])
    access_token = token_response.get("access_token")
    if not access_token:
        raise Exception("Access token ophalen mislukt.")
    
    # Token formatteren
    token_bytes = bytes(access_token, "UTF-8")
    exptoken = b""
    for i in token_bytes:
        exptoken += bytes({i})
        exptoken += bytes(1)
    token_struct = struct.pack("=i", len(exptoken)) + exptoken
    
    return token_struct

def connect_to_database(connection_string, tenant_id, client_id, client_secret, max_retries=3, retry_delay=5):
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    token_struct = None
    
    for attempt in range(max_retries):
        if not token_struct:  # Haal token op bij de eerste poging of als het vervallen is
            token_struct = get_azure_sql_access_token(tenant_id, client_id, client_secret)
        
        try:
            conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
            return conn
        except pyodbc.OperationalError as e:
            # Controleren op verlopen token en vernieuwen
            if "Expired" in str(e):
                print("Token is verlopen, vernieuwen...")
                token_struct = get_azure_sql_access_token(tenant_id, client_id, client_secret)
                continue  # Probeer opnieuw met het vernieuwde token
            else:
                print(f"Fout bij poging {attempt + 1} om verbinding te maken: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    print("Kan geen verbinding maken met de database na meerdere pogingen.")
                    return None