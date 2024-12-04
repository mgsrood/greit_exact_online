from modules.config import create_connection_dict, extract_config_variables, create_division_dict, save_laatste_sync, save_reporting_year, determine_script_id, create_table_config_dict
from modules.database import apply_table_writing, apply_table_clearing
from modules.data_transformation import apply_appending_functions
from modules.type_mapping import apply_type_conversion
from modules.table_mapping import apply_column_mapping
from modules.get_request import execute_get_request
from modules.sync_format import define_sync_format
from modules.log import log, sync_log, end_log
from modules.env_tool import env_check
from datetime import datetime
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
    for klantnaam, (connection_string, type) in connection_dict.items():
        # Skip de klant als type niet gelijk is aan 1
        if type != 1:
            continue

        # Klant configuratie
        errors_occurred = False
        nieuwe_laatste_sync = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        # Ophalen configuratie gegevens
        config_variables = extract_config_variables(connection_string, finn_it_connection_string, klantnaam, script, script_id)
        if config_variables is not None:
            laatste_sync = config_variables['Laatste_sync']
            reporting_year = config_variables['ReportingYear']
        else:
            continue
        
        # Ophalen table configuratie gegevens
        table_config_dict = create_table_config_dict(connection_string, finn_it_connection_string, klantnaam, script, script_id)
        if table_config_dict is None:
            continue
        
        # Ophalen divisie code dictioniary
        division_dict = create_division_dict(finn_it_connection_string, klantnaam, connection_string, script, script_id)
        if division_dict is None:
            continue

        # Verbinding maken per divisie code
        for division_name, division_code in division_dict.items():
            
            # Start log en print
            print(f"Begin GET Requests voor divisie: {division_name} ({division_code}) | {klantnaam}")
            log(finn_it_connection_string, klantnaam, f"Start GET Requests voor nieuwe divisie", script_id, script, division_code)

            url = f"https://start.exactonline.nl/api/v1/{division_code}/"
            
            # Bepaal of het een reset of reguliere sync is
            endpoints = define_sync_format(finn_it_connection_string, klantnaam, laatste_sync, reporting_year, script_id, script, division_code)

            # Endpoint loop
            for tabel, endpoint in endpoints.items():
                for table, status in table_config_dict.items():
                    if table == tabel:
                        if status == 0:
                            print(f"Overslaan van GET Requests voor endpoint: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                            log(finn_it_connection_string, klantnaam, f"Overslaan van GET Requests", script_id, script, division_code, tabel)
                            continue
                        
                        else:
                            
                            # Uitvoeren GET Request
                            df, error = execute_get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel, script_id, script, division_name)

                            if error:
                                errors_occurred = True
                                break
                            
                            if df is None:
                                continue

                            # Line appending functies toepassen
                            for tabel in ["Verkoopfacturen", "VerkoopOrders", "Offertes"]:
                                appended_df = apply_appending_functions(df, tabel, klantnaam, finn_it_connection_string, script_id, script, division_code, division_name)
                            if appended_df is None:
                                continue

                            # Kolom mapping toepassen
                            df_transformed = apply_column_mapping(appended_df, finn_it_connection_string, klantnaam, script_id, script, division_code, tabel) 
                            if df_transformed is None:
                                continue
                            
                            # Type conversie toepassen
                            df_converted = apply_type_conversion(df, finn_it_connection_string, klantnaam, script_id, script, division_code, tabel)
                            if df_converted is None:
                                continue
                            
                            # Rijen verwijderen
                            cleared = apply_table_clearing(connection_string, reporting_year, finn_it_connection_string, klantnaam, script_id, script, division_code, tabel)
                            if cleared is False:
                                continue
                            
                            # Rijen toevoegen
                            written = apply_table_writing(df, connection_string, finn_it_connection_string, klantnaam, script_id, script, division_code, tabel, laatste_sync)
                            if written is False:
                                errors_occurred = True
                                continue
                            
            # Succes log
            log(finn_it_connection_string, klantnaam, f"GET Requests succesvol afgerond deze divisie", script_id, script, division_code)

        # Sync log
        sync_log(klantnaam, nieuwe_laatste_sync, errors_occurred, connection_string, finn_it_connection_string, script_id, script)

    # End log
    end_log(start_time, finn_it_connection_string, script, script_id)