from ex_modules.config import create_connection_dict, determine_script_id
from modules.service_connect import get_azure_sql_access_token, connect_to_database
from modules.env_tool import env_check
from ex_modules.log import log, end_log
from afas_main import afas_main
from ex_main import ex_main
import pyodbc
from azure.identity import ClientSecretCredential
import time
import os
from dotenv import load_dotenv

def main():
    
    load_dotenv()

    # Script configuratie
    klantnaam = "Beuk"
    script = "Wijzigingen"
    start_time = time.time()    

    # Verbindingsinstellingen
    beuk_tenant_id = os.getenv("BEUK_TENANT_ID")
    beuk_client_id = os.getenv("BEUK_CLIENT_ID")
    beuk_client_secret = os.getenv("BEUK_CLIENT_SECRET")
    beuk_server = os.getenv("BEUK_SERVER")
    beuk_database = os.getenv("BEUK_DATABASE")
    beuk_driver = "{ODBC Driver 17 for SQL Server}"
    finn_it_connection_string = (
    f"DRIVER={beuk_driver};"
    f"SERVER={beuk_server};"
    f"DATABASE={beuk_database};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    )
    
    token_struct = get_azure_sql_access_token(beuk_tenant_id, beuk_client_id, beuk_client_secret)
    
    # ScriptID ophalen
    script_id = determine_script_id(token_struct, finn_it_connection_string, klantnaam, script)
    
    # Connection dictionary ophalen
    connection_dict = create_connection_dict(token_struct, finn_it_connection_string, klantnaam, script, script_id)
    
    # Klant loop
    for klantnaam, (connection_string, type, applicatie) in connection_dict.items():
        # Skip de klant als type niet gelijk is aan 1
        if type != 1:
            continue
        
        if applicatie == "AFAS":
            try:
                afas_main(token_struct, connection_string, klantnaam, script, script_id, finn_it_connection_string)
            except Exception as e:
                log(token_struct, finn_it_connection_string, klantnaam, str(e), script_id, script)
        elif applicatie == "Exact":
            try:
                ex_main(token_struct, connection_string, klantnaam, script, script_id, finn_it_connection_string)
            except Exception as e:
                log(token_struct, finn_it_connection_string, klantnaam, str(e), script_id, script)
        else:
            print(f"Onbekende applicatie voor klant: {klantnaam}")
            log(token_struct, finn_it_connection_string, klantnaam, f"Onbekende applicatie", script_id, script)
            continue
    """
    # End log
    end_log(token_struct, start_time, finn_it_connection_string, script, script_id)
    """
    
if __name__ == "__main__":
    main()