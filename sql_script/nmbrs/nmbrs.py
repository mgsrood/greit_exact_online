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
        
        # Configuraties ophalen
        config_dict = config_manager.get_configurations(connection_string)
        if config_dict is None:
            errors_occurred = True
            return False

        laatste_sync = config_dict["Laatste_sync"]
            
        # Ophalen tabel configuratie gegevens
        table_config_dict = config_manager.get_table_configurations(connection_string)
        if table_config_dict is None:
            errors_occurred = True
            return False
        
        """# Debiteuren verwerken
        succes = extract_rest_data(config_manager, connection_string, "Debiteuren")
        if not succes:
            errors_occurred = True
            return False
        
        # Bedrijven verwerken
        succes = extract_rest_data(config_manager, connection_string, "Bedrijven")
        if not succes:
            errors_occurred = True
            return False"""
        
        # Schema's verwerken
        succes = extract_rest_data(config_manager, connection_string, "FTE")
        if not succes:
            errors_occurred = True
            return False
        
        """# Bedrijven ophalen
        succes = extract_klantniveau(soap_manager, connection_string)
        if not succes:
            errors_occurred = True
            return False
        
        # Data ophalen per bedrijf
        succes = extract_bedrijfsniveau(soap_manager, connection_string, table_config_dict, config_manager)
        if not succes:
            errors_occurred = True
            return False"""
        
        """# Data ophalen per werknemer
        succes = extract_werknemerniveau(soap_manager, connection_string, table_config_dict, config_manager)
        if not succes:
            errors_occurred = True
            return False
        
        # Laatste sync en rapportage jaar bijwerken
        if not errors_occurred:
            logging.info(f"Script succesvol afgerond")
            logging.info(f"Alle divisies succesvol verwerkt voor klant {klant}")
        else:
            logging.error(f"Fout bij het verwerken van de divisies voor klant {klant}, laatste sync en rapportage jaar niet bijgewerkt")
"""
    except Exception as e:
        logging.error(f"Fout bij het uitvoeren van het script: {e}")
        return False

