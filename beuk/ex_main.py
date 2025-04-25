from ex_modules.database import apply_table_writing, apply_table_clearing, clear_division_table, write_divisions_to_database
from ex_modules.config import extract_config_variables, create_division_dict, create_table_config_dict
from ex_modules.get_request import execute_get_request, current_division_call, execute_divisions_call
from ex_modules.data_transformation import apply_appending_functions
from ex_modules.type_mapping import apply_type_conversion
from ex_modules.table_mapping import apply_column_mapping
from ex_modules.sync_format import define_sync_format
from ex_modules.log import log, sync_log
from datetime import datetime
import pandas as pd

def ex_main(token_struct, connection_string, klantnaam, script, script_id, finn_it_connection_string):
    
    # Klant configuratie
    errors_occurred = False
    nieuwe_laatste_sync = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    # Ophalen configuratie gegevens
    config_variables = extract_config_variables(token_struct, connection_string, finn_it_connection_string, klantnaam, script, script_id)
    if config_variables is not None:
        laatste_sync = config_variables[0]
        reporting_year = config_variables[1]
    else:
        return

    # Ophalen table configuratie gegevens
    table_config_dict = create_table_config_dict(token_struct, connection_string, finn_it_connection_string, klantnaam, script, script_id)
    if table_config_dict is None:
        return

    # Divisie tabel updaten
    try:
        # Huidige divisiecode ophalen
        tabel = "Divisions"
        url = f"https://start.exactonline.nl/api/v1/"

        current_division_code = current_division_call(token_struct, url, connection_string, finn_it_connection_string, klantnaam, script_id, script)
        if current_division_code is None:
            return
    
        # Divisie tabel updaten
        divisions_df, error = execute_divisions_call(token_struct, url, connection_string, current_division_code, finn_it_connection_string, klantnaam, script_id, script)
        if error:
            errors_occurred = True
            return
                            
        if divisions_df is None:
            return
                
        # Kolom mapping toepassen
        df_transformed = apply_column_mapping(token_struct, divisions_df, finn_it_connection_string, klantnaam, script_id, script, current_division_code, tabel)
        if df_transformed is None:
            return

        # Type conversie toepassen
        df_converted = apply_type_conversion(token_struct, df_transformed, finn_it_connection_string, klantnaam, script_id, script, current_division_code, tabel)
        if df_converted is None:
            return
        
        # Rijen verwijderen
        try:
            clear_division_table(token_struct, connection_string, tabel)
        except Exception as e:
            log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het leegmaken van de tabel: {e}", script_id, script)
            errors_occurred = True
            return

        # Rijen toevoegen
        try:
            write_divisions_to_database(token_struct, df_converted, tabel, connection_string)
        except Exception as e:
            log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het toevoegen van de rijen: {e}", script_id, script)
            errors_occurred = True
            return
                            
        # Ophalen divisie code dictionary
        division_dict = create_division_dict(token_struct, finn_it_connection_string, klantnaam, connection_string, script, script_id)
        if division_dict is None:
            return
        
    except Exception as e:
        log(token_struct, finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het ophalen van de divisies: {e}", script_id, script)
        errors_occurred = True
        return
    
    # Verbinding maken per divisie code
    for division_name, division_code in division_dict.items():
        # Start log en print
        print(f"Begin GET Requests voor divisie: {division_name} ({division_code}) | {klantnaam}")
        log(token_struct, finn_it_connection_string, klantnaam, f"Start GET Requests voor nieuwe divisie", script_id, script, division_code)

        url = f"https://start.exactonline.nl/api/v1/{division_code}/"
        
        # Bepaal of het een reset of reguliere sync is
        endpoints = define_sync_format(token_struct, finn_it_connection_string, klantnaam, laatste_sync, reporting_year, script_id, script, division_code)

        # Endpoint loop
        for tabel, endpoint in endpoints.items():
            for table, status in table_config_dict.items():
                if table == tabel:
                    if status == 0:
                        print(f"Overslaan van GET Requests voor endpoint: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                        log(token_struct, finn_it_connection_string, klantnaam, f"Overslaan van GET Requests", script_id, script, division_code, tabel)
                        continue
                    
                    else:
                        # Uitvoeren GET Request
                        df, error = execute_get_request(token_struct, division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel, script_id, script, division_name)

                        if error:
                            errors_occurred = True
                            break
                        
                        if df is None:
                            continue

                        # Line appending functies toepassen
                        appended_df = apply_appending_functions(token_struct, df, tabel, klantnaam, finn_it_connection_string, script_id, script, division_code, division_name)
                        if appended_df is None:
                            continue

                        # Kolom mapping toepassen
                        try:
                            df_transformed = apply_column_mapping(token_struct, appended_df, finn_it_connection_string, klantnaam, script_id, script, division_code, tabel)
                        except Exception as e:
                            print(f"FOUTMELDING | Fout bij het toepassen van de kolom mapping: {e}")
                            continue

                        if df_transformed is None:
                            continue

                        # Type conversie toepassen
                        try:
                            df_converted = apply_type_conversion(token_struct, df_transformed, finn_it_connection_string, klantnaam, script_id, script, division_code, tabel)
                        except Exception as e:
                            print(f"FOUTMELDING | Fout bij het toepassen van de type conversie: {e}")
                            continue

                        if df_converted is None:
                            continue

                        # Rijen verwijderen
                        cleared = apply_table_clearing(token_struct, connection_string, reporting_year, finn_it_connection_string, klantnaam, script_id, script, division_code, tabel)
                        if cleared is False:
                            continue

                        # Rijen toevoegen
                        written = apply_table_writing(token_struct, df_converted, connection_string, finn_it_connection_string, klantnaam, script_id, script, division_code, tabel, laatste_sync)
                        if written is False:
                            errors_occurred = True
                            continue
                        
        # Succes log
        log(token_struct, finn_it_connection_string, klantnaam, f"GET Requests succesvol afgerond deze divisie", script_id, script, division_code)
