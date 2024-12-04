from ex_modules.config import create_connection_dict, determine_script_id
from modules.env_tool import env_check
from ex_modules.log import log, end_log
from afas_main import afas_main
from ex_main import ex_main
import time
import os


def main():
    
    env_check()

    # Script configuratie
    klantnaam = "Finnit"
    script = "Wijzigingen"
    start_time = time.time()    

    # Verbindingsinstellingen
    username = os.getenv('GEBRUIKERSNAAM')
    database = os.getenv('DATABASE')
    password = os.getenv('PASSWORD')
    server = os.getenv('SERVER')
    driver = '{ODBC Driver 17 for SQL Server}'
    finn_it_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    
    # ScriptID ophalen
    script_id = determine_script_id(finn_it_connection_string, klantnaam, script)
    
    # Connection dictionary ophalen
    connection_dict = create_connection_dict(finn_it_connection_string, klantnaam, script, script_id)

    # Klant loop
    for klantnaam, (connection_string, type, applicatie) in connection_dict.items():
        # Skip de klant als type niet gelijk is aan 1
        if type != 1:
            continue
        
        if applicatie == "AFAS":
            try:
                afas_main(connection_string, klantnaam, script, script_id, finn_it_connection_string)
            except Exception as e:
                log(finn_it_connection_string, klantnaam, str(e), script_id, script)
        elif applicatie == "Exact":
            try:
                ex_main(connection_string, klantnaam, script, script_id, finn_it_connection_string)
            except Exception as e:
                log(finn_it_connection_string, klantnaam, str(e), script_id, script)
        else:
            print(f"Onbekende applicatie voor klant: {klantnaam}")
            log(finn_it_connection_string, klantnaam, f"Onbekende applicatie", script_id, script)
            continue

    # End log
    end_log(start_time, finn_it_connection_string, script, script_id)
    
if __name__ == "__main__":
    main()
    
'''        '''