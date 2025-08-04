from nmbrs.modules.clear_and_write import apply_table_clearing, apply_table_writing
from nmbrs.modules.column_management import apply_column_mapping
from nmbrs.modules.type_mapping import apply_type_conversion
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
        
        # Configuraties ophalen
        config_dict = config_manager.get_configurations(connection_string)
        if config_dict is not None:
            domain = config_dict["Domain"]
            username = config_dict["Username"]
            token = config_dict["Token"]
        else:
            errors_occurred = True
            return False
        
        # SoapManager definiëren
        soap_manager = SoapManager(domain, username, token)
        
        try:
            # Bedrijven ophalen
            df_company_list = soap_manager.get_company_list()
            if df_company_list.empty:
                logging.error("Kon geen bedrijfslijst ophalen.")
                errors_occurred = True
                return False
            
            # Kolom mapping toepassen
            df_transformed = apply_column_mapping(df_company_list, "Bedrijven")
            
            if df_transformed is None:
                errors_occurred = True
                return False
            
            # Type conversie toepassen
            df_converted = apply_type_conversion(df_transformed, "Bedrijven")
            
            if df_converted is None:
                errors_occurred = True
                return False
            
            # Legen Divisions tabel
            apply_table_clearing(connection_string, "Bedrijven")
            
            # Divisions tabel vullen
            apply_table_writing(df_converted, connection_string, "Bedrijven")
            
        except Exception as e:
            logging.error(f"Onverwachte fout in Exact verwerking: {e}")
            errors_occurred = True
            return False

        # Bedrijfsloop
        total_companies = len(df_converted)
        processed_companies = 0
        logging.info(f"Starten met verwerking van {total_companies} bedrijven voor NMBRS looncomponenten.")

        for company_id, company_name in zip(df_converted["BedrijfID"], df_converted["Bedrijfsnaam"]):
            processed_companies += 1
            logging.info(f"Verwerken bedrijf {processed_companies}/{total_companies}: {company_name} (ID: {company_id}")
        
            year = "2025"
            
            # Roep de nieuwe methode aan in SoapManager
            df_report_data = soap_manager.execute_report_creation(company_id, year)
            pd.set_option('display.max_columns', None)
            print(df_report_data.head())
            
            if df_report_data is None:
                logging.error(f"Kon geen rapportdata ophalen voor company {company_id}, year {year}. Verwerking gestopt.")
                errors_occurred = True
                continue
            elif df_report_data.empty:
                logging.info(f"Kon geen rapportdata ophalen voor company {company_id}, year {year}. Verwerking gestopt.")
                continue
            else:
                logging.info(f"Succesvol rapportdata (DataFrame) ontvangen voor company {company_id}, year {year}.")
            
            # Kolom mapping toepassen
            df_transformed = apply_column_mapping(df_report_data, "Looncomponenten")
            
            if df_transformed is None:
                errors_occurred = True
                continue
            
            # Type conversie toepassen
            df_converted = apply_type_conversion(df_transformed, "Looncomponenten")
            
            if df_converted is None:
                errors_occurred = True
                continue
            
            # Legen Divisions tabel
            apply_table_clearing(connection_string, "Looncomponenten", company_id, year)
            
            # Divisions tabel vullen
            apply_table_writing(df_converted, connection_string, "Looncomponenten")
            
            logging.info(f"Looncomponenten tabel succesvol verwerkt voor {company_name} | {company_id}, year {year}")
        
        logging.info(f"Verwerking NMBRS looncomponenten voltooid. {processed_companies}/{total_companies} bedrijven-iteraties doorlopen.")
        # Succes logging
        if not errors_occurred:
            logging.info(f"Script succesvol afgerond")
            logging.info(f"Alle divisies succesvol verwerkt voor klant {klant}")
        else:
            logging.error(f"Fout bij het verwerken van de divisies voor klant {klant}")

    except Exception as e:
        logging.error(f"Fout bij het uitvoeren van het script: {e}")
        return False