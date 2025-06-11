from afas.modules.clear_and_write import apply_table_clearing, apply_table_writing
from afas.modules.type_mapping import apply_type_conversion, add_environment_id
from greit_exact_online.sql_script.utils.env_config import EnvConfig
from afas.modules.get_request import execute_get_request
from afas.modules.get_request import SyncFormatManager
from datetime import datetime
import pandas as pd
import logging

def afas(connection_string, config_manager, klant):
    """
    Hoofdfunctie voor het ophalen van AFAS data.
    
    Args:
        connection_string: Connectiestring voor de database
        config_manager: Instantie van ConfigManager
        klant: Klantnaam
    """
    
    try:
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

        else:
            errors_occurred = True
            return 'false'

        # Ophalen tabel configuratie gegevens
        table_config_dict = config_manager.get_table_configurations(connection_string)
        if table_config_dict is None:
            errors_occurred = True
            return 'false'

        # Ophalen omgevings configuratie gegevens
        environment_dict = config_manager.create_environment_dict(connection_string)
        if environment_dict is None:
            errors_occurred = True
            return 'false'

        # Endpoint loop
        for klant, (omgeving_id, api_string, token, status, volledige_sync) in environment_dict.items():
            if status == 0:
                logging.info(f"Overslaan van GET Requests voor omgeving: {klant}")
                continue
            
            if volledige_sync == 1:
                laatste_sync = "2000-01-01T00:00:00"
                logging.info(f"Volledige sync voor omgeving: {klant}")
            
            # Stel de logging context in voor deze omgeving
            config_manager.set_logging_context(administratiecode=omgeving_id, tabel=None)
            
            for table, status in table_config_dict.items():
                if status == 0:
                    logging.info(f"Overslaan van GET Requests voor endpoint: {table} | {klant}")
                    continue
            
                # Stel de logging context in voor deze tabel, behoud de administratiecode
                config_manager.set_logging_context(administratiecode=omgeving_id, tabel=table)
                
                # Connector ophalen
                sync_manager = SyncFormatManager(config_manager)
                connectors = sync_manager.return_connectors(laatste_sync)
                
                if table in connectors:
                    connector = connectors[table]
                
                    # Uitvoeren GET Request
                    logging.info(f"Start GET Requests voor tabel: {table} | {klant}")
                    df, error = execute_get_request(api_string, token, connector, klant, table)
                    
                    if error:
                        errors_occurred = True
                        
                    if df is None or df.empty:
                        # Als de DataFrame leeg is, sla deze omgeving/tabel over
                        logging.info(f"Overslaan van verdere verwerking voor tabel {table} omdat er geen data is.")
                        continue
                    
                    # Omgeving ID toevoegen
                    df = add_environment_id(df, omgeving_id)
                    
                    if df is None:
                        errors_occurred = True
                        continue
                    
                    # Type conversie toepassen
                    df_converted = apply_type_conversion(df, table)
                    if df_converted is None:
                        continue
                    
                    # Rijen verwijderen
                    apply_table_clearing(connection_string, table, omgeving_id, laatste_sync)
                    
                    # Rijen toevoegen
                    succes = apply_table_writing(df_converted, connection_string, table, laatste_sync)

                    if succes is False:
                        errors_occurred = True
                        continue
                    
                # Logging van afronding 
                if errors_occurred is False:
                    logging.info(f"GET Request succesvolafgerond voor tabel: {table} | {klant} ({omgeving_id})")
                else:
                    logging.error(f"Fout bij GET Request voor tabel: {table} | {klant} ({omgeving_id})")
        
            # Logging van afronding 
            if errors_occurred is False:
                logging.info(f"GET Requests succesvol afgerond voor divisie: {klant} ({omgeving_id})")
            else:
                logging.error(f"Fout bij GET Requests voor divisie: {klant} ({omgeving_id})")
        
        # Laatste sync en rapportage jaar bijwerken
        if errors_occurred is False:
            logging.info(f"Script succesvol afgerond voor klant {klant}")
            
            if env_config_dict["script_name"] == "Wijzigingen":
                config_manager.update_last_sync(connection_string, nieuwe_laatste_sync)
                config_manager.update_reporting_year(connection_string)
                logging.info("Laatste sync en reporting year succesvol geüpdate")
        else:
            logging.error(f"Fout bij het verwerken van de divisies voor klant {klant}, laatste sync en rapportage jaar niet bijgewerkt")

    except Exception as e:
        logging.error(f"Fout bij het uitvoeren van het script: {e}")