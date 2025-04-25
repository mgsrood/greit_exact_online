from ex_modules.config import create_table_config_dict, create_division_dict
from ex_modules.full_database import apply_table_writing, apply_table_clearing
from ex_modules.data_transformation import apply_appending_functions
from ex_modules.type_mapping import apply_type_conversion
from ex_modules.table_mapping import apply_column_mapping
from ex_modules.get_request import execute_get_request
from ex_modules.full_endpoints import get_endpoints
from datetime import datetime, timedelta
from ex_modules.log import log

def full_ex_main(connection_string, finn_it_connection_string, klantnaam, script, script_id):
    # Klant configuratie
    errors_occurred = False
    
    # Ophalen table configuratie gegevens
    table_config_dict = create_table_config_dict(connection_string, finn_it_connection_string, klantnaam, script, script_id)
    if table_config_dict is None:
        return
    
    # Ophalen divisie code dictionary
    division_dict = create_division_dict(finn_it_connection_string, klantnaam, connection_string, script, script_id)
    if division_dict is None:
        return
    
    # Verbinding maken per divisie code
    for division_name, division_code in division_dict.items():
        # Start log en print
        print(f"Begin GET Requests voor divisie: {division_name} ({division_code}) | {klantnaam}")
        log(finn_it_connection_string, klantnaam, f"Start GET Requests voor nieuwe divisie", script_id, script, division_code)

        url = f"https://start.exactonline.nl/api/v1/{division_code}/"
        
        # Endpoints ophalen
        endpoints = get_endpoints()
        
        # Endpoint loop
        for tabel, endpoint in endpoints.items():
            for table, status in table_config_dict.items():
                if table == tabel:
                    if status == 0:
                        print(f"Overslaan van GET Requests voor endpoint: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                        log(finn_it_connection_string, klantnaam, f"Overslaan van GET Requests", script_id, script, division_code, tabel)
                        continue
                    
                    else:
                        # Start date
                        last_year = datetime.now() - timedelta(days=365)
                        start_date = last_year.replace(day=1, month=1, hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")   
                        print(start_date)
                        # Uitvoeren GET Request
                        df, error = execute_get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel, script_id, script, division_name)

                        if error:
                            errors_occurred = True
                            break
                        
                        if df is None:
                            continue
                        
                        # Line appending functies toepassen
                        appended_df = apply_appending_functions(df, tabel, klantnaam, finn_it_connection_string, script_id, script, division_code, division_name)
                        if appended_df is None:
                            continue
                        
                        # Kolom mapping toepassen
                        df_transformed = apply_column_mapping(appended_df, finn_it_connection_string, klantnaam, script_id, script, division_code, tabel) 
                        if df_transformed is None:
                            continue
                        
                        # Type conversie toepassen
                        df_converted = apply_type_conversion(df_transformed, finn_it_connection_string, klantnaam, script_id, script, division_code, tabel)
                        if df_converted is None:
                            continue
                        
                        # Rijen verwijderen
                        cleared = apply_table_clearing(connection_string, start_date, finn_it_connection_string, klantnaam, script_id, script, division_code, tabel)
                        if cleared is False:
                            continue
                        
                        # Rijen toevoegen
                        written = apply_table_writing(df_converted, connection_string, finn_it_connection_string, klantnaam, script_id, script, division_code, tabel)
                        if written is False:
                            errors_occurred = True
                            continue
                        
    
    # Succes log
    log(finn_it_connection_string, klantnaam, f"GET Requests succesvol afgerond deze divisie", script_id, script, division_code)