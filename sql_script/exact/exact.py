# Voeg de root van het project toe aan de Python path
import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
sys.path.append(project_root)

from exact.modules.get_request import current_division_call, execute_get_request, execute_divisions_call
from exact.modules.clear_and_write import apply_table_clearing, apply_table_writing
from exact.modules.column_management import apply_column_mapping
from exact.modules.type_mapping import apply_type_conversion
from exact.modules.appending_functions import DataTransformer
from exact.modules.synchronisation import SyncFormatManager
from sql_script.utils.env_config import EnvConfig
from datetime import datetime
import pandas as pd
import logging

def exact(connection_string, config_manager):
    """
    Hoofdfunctie voor het ophalen van Exact Online data.
    
    Args:
        connection_string: Connectiestring voor de database
        config_manager: Instantie van ConfigManager
    """
    # Environment configuratie
    env_config = EnvConfig()
    env_config_dict = env_config.get_database_config()
    
    # Klant configuratie
    errors_occurred = False
    nieuwe_laatste_sync = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    # Configuraties ophalen
    config_dict = config_manager.get_configurations(connection_string)
    if config_dict is not None:
        laatste_sync = config_dict["Laatste_sync"]
        reporting_year = config_dict["ReportingYear"]
    else:
        errors_occurred = True
        return False

    # Ophalen tabel configuratie gegevens
    table_config_dict = config_manager.get_table_configurations(connection_string)
    if table_config_dict is None:
        errors_occurred = True
        return False
    
    try:
        # Basis URL voor Exact Online API
        url = "https://start.exactonline.nl/api/v1/"
        
        # Huidige divisiecode ophalen
        current_division_code = current_division_call(config_manager, url, connection_string)
        
        if current_division_code is None:
            logging.error("Kon geen huidige divisie ophalen")
            return False
        
        # Divisies ophalen
        divisions_df, errors = execute_divisions_call(config_manager, url, connection_string, current_division_code)

        if errors:
            logging.error("Fout bij ophalen divisies")
            errors_occurred = True
            return False
            
        if divisions_df is None or divisions_df.empty:
            logging.warning("Geen relevante divisies gevonden")
            return False
        
        # Kolom mapping toepassen
        df_transformed = apply_column_mapping(divisions_df, "Divisions", current_division_code)
        
        if df_transformed is None:
            errors_occurred = True
            return False
        
        # Type conversie toepassen
        df_converted = apply_type_conversion(df_transformed, "Divisions")
        
        if df_converted is None:
            errors_occurred = True
            return False
        
        # Legen Divisions tabel
        apply_table_clearing(connection_string, "Divisions")
        
        # Divisions tabel vullen
        apply_table_writing(connection_string, df_converted, "Divisions")
        
    except Exception as e:
        logging.error(f"Onverwachte fout in Exact verwerking: {e}")
        errors_occurred = True
        return False
    
    # Divisies ophalen
    division_codes = config_manager.get_division_codes(connection_string)
    
    if not division_codes:
        logging.error("Geen divisie codes kunnen ophalen")
        errors_occurred = True
        return False

    # Loop door alle divisie codes
    for division_name, division_code in division_codes.items():
        logging.info(f"Verwerken divisie: {division_name} ({division_code})")
        
        # Update de klantnaam voor logging
        config_manager.update_klant(division_name)
        
        # Basis URL voor Exact Online API met Divisie code extensie
        url = f"https://start.exactonline.nl/api/v1/{division_code}/"
        
        # Synchroinsatie vorm bepalen
        sync_manager = SyncFormatManager(config_manager)
        
        try:
            endpoints = sync_manager.define_sync_format(laatste_sync, reporting_year)
            # Gebruik de endpoints voor verdere verwerking
            
        except Exception as e:
            config_manager.logger.error(f"Fout tijdens bepalen sync format: {str(e)}")
            errors_occurred = True
            continue
        
        # Endpoint loop
        for tabel, endpoint in endpoints.items():
            for table, status in table_config_dict.items():
                if table == tabel:
                    if status == 0:
                        logging.info(f"Overslaan van GET Requests voor endpoint: {tabel} | {division_name} ({division_code})")
                        continue
                    
                    else:
                        # Uitvoeren GET Request
                        df, error = execute_get_request(config_manager, division_code, url, endpoint, connection_string, tabel, division_name)
                        
                        if error:
                            errors_occurred = True
                            continue
                        
                        if df is None or df.empty:
                            logging.warning(f"Geen data opgehaald voor tabel: {tabel} | {division_name} ({division_code})")
                            continue
                        
                        # Maak een DataTransformer instantie
                        transformer = DataTransformer(config_manager)
                        
                        # Mogelijke appending functies toepassen
                        df_appended = transformer.transform_data(df, tabel, division_code, division_name)
                        
                        # Kolom mapping toepassen
                        df_transformed = apply_column_mapping(df_appended, tabel, division_code)
                        
                        # Type conversie toepassen
                        df_converted = apply_type_conversion(df_transformed, tabel)

                        # Rijen verwijderen
                        apply_table_clearing(connection_string, tabel, division_code, reporting_year)
                        
                        # Rijen toevoegen
                        succes = apply_table_writing(connection_string, df_converted, tabel, laatste_sync)
                        
                        if succes is False:
                            errors_occurred = True
                            continue
        
        # Logging van afronding 
        logging.info(f"GET Requests succesvol afgerond voor divisie: {division_name} ({division_code})")
    
    # Logging van afronding 
    logging.info(f"Alle divisies succesvol verwerkt voor klant {env_config_dict['klant_naam']}")
    
    # Laatste sync en rapportage jaar bijwerken
    if errors_occurred is True:
        config_manager.update_last_sync(connection_string, nieuwe_laatste_sync)
        config_manager.update_reporting_year(connection_string)
    else:
        logging.error(f"Fout bij het verwerken van de divisies voor klant {env_config_dict['klant_naam']}, laatste sync en rapportage jaar niet bijgewerkt")