from nmbrs.modules.data_extracties import extract_rest_data
from nmbrs.modules.get_request import get_debtor_list
from nmbrs.modules.soap import SoapManager
from datetime import datetime
import pandas as pd
import logging
import json
import time

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
        
        # NMBRS SOAP requests
        domain = "beuklonen"
        username = "kevin@finnit.nl"
        token = "f8e3f91ec2e34a85ba5be8389713cfc6"
        soap_manager = SoapManager(config_manager, domain, username, token)
        report_guid = soap_manager.execute_report_request("32885", "2022")
        
        print(f"Report aangemaakt met GUID: {report_guid}")
        
        # Poll voor de status van het report
        max_attempts = 30  # Maximum aantal pogingen
        wait_time = 10     # Wacht 10 seconden tussen elke check
        
        for attempt in range(max_attempts):
            status = soap_manager.check_report_status(report_guid)
            print(f"Status check {attempt + 1}/{max_attempts}: {status}")
            
            if status == "Success":
                print("Report is succesvol gegenereerd!")
                break
            elif status == "Error":
                print("Report genereren is mislukt!")
                break
            elif status == "Unknown":
                print("Report status is onbekend!")
                break
                
            if attempt < max_attempts - 1:  # Niet wachten na de laatste poging
                time.sleep(wait_time)
        else:
            print("Timeout: Report is niet binnen de verwachte tijd voltooid")
        
        """
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
            logging.error(f"Fout bij het verwerken van de divisies voor klant {klant}, laatste sync en rapportage jaar niet bijgewerkt")"""

    except Exception as e:
        logging.error(f"Fout bij het uitvoeren van het script: {e}")
        return False

