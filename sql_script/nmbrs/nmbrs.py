from nmbrs.modules.data_extracties import extract_rest_data
from nmbrs.modules.get_request import get_debtor_list
from nmbrs.modules.soap import SoapManager
from datetime import datetime
import pandas as pd
import logging
import json

def nmbrs(connection_string, config_manager, klant):
    """
    Hoofdfunctie voor het ophalen van NMBRS data.
    
    Args:
        connection_string: Connectiestring voor de database
        config_manager: Instantie van ConfigManager
        klant: Klantnaam
    """
      
    try:
        # Klant configuratie
        errors_occurred = False
        nieuwe_laatste_sync = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            
        # Ophalen tabel configuratie gegevens
        table_config_dict = config_manager.get_table_configurations(connection_string)
        if table_config_dict is None:
            errors_occurred = True
            return False
        
        # Debiteuren verwerken
        succes = extract_rest_data(config_manager, connection_string, "Debiteuren")
        if not succes:
            errors_occurred = True
            return False
        
        # Bedrijven verwerken
        succes = extract_rest_data(config_manager, connection_string, "Bedrijven")
        if not succes:
            errors_occurred = True
            return False
        
        # Schema's verwerken
        succes = extract_rest_data(config_manager, connection_string, "FTE")
        if not succes:
            errors_occurred = True
            return False
        
        # Contracten verwerken
        succes = extract_rest_data(config_manager, connection_string, "Contracten")
        if not succes:
            errors_occurred = True
            return False
        
        # Succes logging
        if not errors_occurred:
            logging.info(f"Script succesvol afgerond")
            logging.info(f"Alle divisies succesvol verwerkt voor klant {klant}")
        else:
            logging.error(f"Fout bij het verwerken van de divisies voor klant {klant}, laatste sync en rapportage jaar niet bijgewerkt")

    except Exception as e:
        logging.error(f"Fout bij het uitvoeren van het script: {e}")
        return False

