
from afas_modules.config import extract_config_variables, create_table_config_dict, create_environment_dict
from afas_modules.type_mapping import apply_type_conversion, add_environment_id
from afas_modules.database import apply_table_clearing, apply_table_writing
from afas_modules.get_request import execute_get_request
from afas_modules.connectors import get_connectors
from afas_modules.log import log, sync_log
from datetime import datetime
import pandas as pd
import os

def afas_main(connection_string, klantnaam, script, script_id, finn_it_connection_string):

    # Klant configuratie
    errors_occurred = False
    nieuwe_laatste_sync = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # Ophalen configuratie gegevens
    laatste_sync = extract_config_variables(connection_string, finn_it_connection_string, klantnaam, script, script_id)
    if laatste_sync is None:
        return
    
    # Ophalen table configuratie gegevens
    table_config_dict = create_table_config_dict(connection_string, finn_it_connection_string, klantnaam, script, script_id)
    if table_config_dict is None:
        return
    
    # Ophalen omgevings configuratie gegevens
    environment_dict = create_environment_dict(connection_string, finn_it_connection_string, klantnaam, script, script_id)
    if environment_dict is None:
        return
    
    # Endpoint loop
    for klant, (omgeving_id, api_string, token, status) in environment_dict.items():

        # Check of the omgeving aan staat
        if status == 0:
            print(f"Overslaan van GET Requests voor omgeving: {klantnaam}")
            log(finn_it_connection_string, klantnaam, f"Overslaan van GET Requests", script_id, script, omgeving_id)
            continue
        
        for table, status in table_config_dict.items():
            if status == 0:
                print(f"Overslaan van GET Requests voor endpoint: {table} | {klantnaam}")
                log(finn_it_connection_string, klantnaam, f"Overslaan van GET Requests", script_id, script, omgeving_id, table)
                continue
            
            # Connector ophalen
            connectors = get_connectors(laatste_sync)
            if table in connectors:
                connector = connectors[table]

                # Uitvoeren GET Request
                print(f"Start GET Requests voor tabel: {table} | {klantnaam}")
                df, error = execute_get_request(api_string, token, connector, finn_it_connection_string, klantnaam, table, script_id, script, omgeving_id)
                
                if error:
                    errors_occurred = True
                    
                if df is None or df.empty:
                    # Als de DataFrame leeg is, sla deze omgeving/tabel over
                    print(f"Overslaan van verdere verwerking voor tabel {table} omdat er geen data is.")
                    continue
                                
                # Omgeving ID toevoegen
                df = add_environment_id(df, omgeving_id)
                
                if df is None:
                    errors_occurred = True
                    continue
                
                # Type conversie toepassen
                print("Start data type conversie")
                df_converted = apply_type_conversion(df, finn_it_connection_string, klantnaam, script_id, script, table, omgeving_id)
                if df_converted is None:
                    continue
                            
                # Rijen verwijderen
                print("Start rijen verwijderen")
                cleared = apply_table_clearing(connection_string, finn_it_connection_string, klantnaam, script_id, script, table, omgeving_id)
                if cleared is False:
                    continue
                
                # Rijen toevoegen
                print("Start rijen toevoegen")
                written = apply_table_writing(df, connection_string, finn_it_connection_string, klantnaam, script_id, script, table, laatste_sync, omgeving_id)
                if written is False:
                    errors_occurred = True
                    continue
                
        # Succes log
        log(finn_it_connection_string, klantnaam, f"GET Requests succesvol afgerond deze omgeving", script_id, script, omgeving_id)
        
    # Sync log
    sync_log(klantnaam, nieuwe_laatste_sync, errors_occurred, connection_string, finn_it_connection_string, script_id, script, omgeving_id, table)
                        
if __name__ == "__main__":
    afas_main()
    

'''

# Ophalen configuratie gegevens
    config_variables = extract_config_variables(connection_string, finn_it_connection_string, klantnaam, script, script_id)
    if config_variables is not None:
        token = config_variables['Token']
        user_id = config_variables['User_ID']
    else:
        return
    
    # Ophalen table configuratie gegevens
    table_config_dict = create_table_config_dict(connection_string, finn_it_connection_string, klantnaam, script, script_id)
    if table_config_dict is None:
        return
    
     # Endpoint loop
    for tabel, connector in connectors.items():
        for table, status in table_config_dict.items():
            if table == tabel:
                if status == 0:
                    print(f"Overslaan van GET Requests voor endpoint: {tabel} | {klantnaam}")
                    log(finn_it_connection_string, klantnaam, f"Overslaan van GET Requests", script_id, script, division_code, tabel)
                    continue
                
                else:
                # Uitvoeren GET Request
                df = execute_get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel, script_id, script, division_name)'''