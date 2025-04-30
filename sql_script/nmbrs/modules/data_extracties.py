from nmbrs.modules.get_request import get_debtor_list, get_companies_per_debtor, get_employee_schedules
from nmbrs.modules.data_loading import get_debiteuren_from_db, get_bedrijven_from_db
from nmbrs.modules.clear_and_write import apply_table_clearing, apply_table_writing
from nmbrs.modules.column_management import apply_column_mapping
from nmbrs.modules.type_mapping import apply_type_conversion
import pandas as pd
import logging

# Mapping van tabellen naar hun REST API functies
REST_TABLE_MAPPING = {
    "Debiteuren": get_debtor_list,
    "Bedrijven": get_companies_per_debtor,
    "FTE": get_employee_schedules
    # Hier kunnen later meer tabellen worden toegevoegd
}

def extract_rest_data(config_manager, connection_string, table):
    """
    Haalt data op via REST API en verwerkt deze.
    
    Args:
        config_manager: Instantie van ConfigManager
        connection_string: Connectiestring voor de database
        table: Naam van de tabel
        
    Returns:
        bool: Succes status van de operatie
    """
    try:
        logging.info(f"Start REST data extractie voor tabel: {table}")
        
        # Controleer of de tabel ondersteund wordt door REST API
        if table not in REST_TABLE_MAPPING:
            logging.error(f"Tabel {table} wordt niet ondersteund door REST API")
            return False
            
        # Haal de juiste functie op uit de mapping
        rest_function = REST_TABLE_MAPPING[table]
        
        if table == "Bedrijven":
            # Voor bedrijven moeten we eerst alle debiteuren ophalen
            debiteuren_df = get_debiteuren_from_db(config_manager, connection_string)
            if debiteuren_df is None or debiteuren_df.empty:
                logging.error("Geen debiteuren gevonden in de database")
                return False
                
            all_bedrijven = []
            for _, debiteur in debiteuren_df.iterrows():
                debtor_id = debiteur['DebtorID']
                bedrijven_df = get_companies_per_debtor(config_manager, connection_string, debtor_id)
                if bedrijven_df is not None and not bedrijven_df.empty:
                    all_bedrijven.append(bedrijven_df)
                    
            if not all_bedrijven:
                logging.error("Geen bedrijven gevonden voor de debiteuren")
                return False
                
            df = pd.concat(all_bedrijven, ignore_index=True)
            
            # Verwerk de data voor alle bedrijven
            if df.empty:
                logging.info(f"Geen data gevonden voor tabel {table}")
                return True
                
            # Kolom mapping toepassen
            df_transformed = apply_column_mapping(df, table)
            
            if df_transformed is None:
                logging.error(f"Fout bij kolom mapping voor {table}")
                return False
            
            # Type conversie toepassen
            df_converted = apply_type_conversion(df_transformed, table)
            
            if df_converted is None:
                logging.error(f"Fout bij type conversie voor {table}")
                return False
            
            # Rijen verwijderen
            apply_table_clearing(connection_string, table)
            
            # Rijen toevoegen
            succes = apply_table_writing(df_converted, connection_string, table)
            
            if not succes:
                logging.error(f"Fout bij het schrijven van {table} data")
                return False
                
            logging.info(f"{table} data succesvol verwerkt")
            return True
            
        elif table == "FTE":
            # Voor FTE verwerken we per bedrijf
            bedrijven_df = get_bedrijven_from_db(config_manager, connection_string)
            if bedrijven_df is None or bedrijven_df.empty:
                logging.error("Geen bedrijven gevonden in de database")
                return False
                
            totaal_bedrijven = len(bedrijven_df)
            verwerkte_bedrijven = 0
            
            for _, bedrijf in bedrijven_df.iterrows():
                verwerkte_bedrijven += 1
                company_id = bedrijf['CompanyID']
                bedrijfsnaam = bedrijf['Naam']
                logging.info(f"Verwerken FTE voor bedrijf {bedrijfsnaam} ({verwerkte_bedrijven}/{totaal_bedrijven})")
                
                # Data ophalen voor dit bedrijf
                fte_df = get_employee_schedules(config_manager, connection_string, company_id)
                if fte_df is None or fte_df.empty:
                    logging.warning(f"Geen FTE gevonden voor bedrijf {bedrijfsnaam} ({verwerkte_bedrijven}/{totaal_bedrijven})")
                    continue
                    
                # Kolom mapping toepassen
                df_transformed = apply_column_mapping(fte_df, table)
                if df_transformed is None:
                    logging.error(f"Fout bij kolom mapping voor {table} bij bedrijf {bedrijfsnaam} ({verwerkte_bedrijven}/{totaal_bedrijven})")
                    continue
                
                # Type conversie toepassen
                df_converted = apply_type_conversion(df_transformed, table)
                if df_converted is None:
                    logging.error(f"Fout bij type conversie voor {table} bij bedrijf {bedrijfsnaam} ({verwerkte_bedrijven}/{totaal_bedrijven})")
                    continue
                
                # Rijen verwijderen voor dit bedrijf
                apply_table_clearing(connection_string, table, company_id)
                
                # Rijen toevoegen voor dit bedrijf
                succes = apply_table_writing(df_converted, connection_string, table)
                if not succes:
                    logging.error(f"Fout bij het schrijven van {table} data voor bedrijf {bedrijfsnaam} ({verwerkte_bedrijven}/{totaal_bedrijven})")
                    continue
                    
                logging.info(f"FTE succesvol verwerkt voor bedrijf {bedrijfsnaam} ({verwerkte_bedrijven}/{totaal_bedrijven})")
            
            logging.info(f"Verwerking FTE voltooid voor alle {totaal_bedrijven} bedrijven")
            return True

        else:
            # Voor andere tabellen gebruiken we de standaard functie
            df = rest_function(config_manager, connection_string)
            if df is None:
                logging.error(f"Fout bij ophalen data voor tabel {table}")
                return False
                
            if df.empty:
                logging.info(f"Geen data gevonden voor tabel {table}")
                return True
                
            # Kolom mapping toepassen
            df_transformed = apply_column_mapping(df, table)
            
            if df_transformed is None:
                logging.error(f"Fout bij kolom mapping voor {table}")
                return False
            
            # Type conversie toepassen
            df_converted = apply_type_conversion(df_transformed, table)
            
            if df_converted is None:
                logging.error(f"Fout bij type conversie voor {table}")
                return False
            
            # Rijen verwijderen
            apply_table_clearing(connection_string, table)
            
            # Rijen toevoegen
            succes = apply_table_writing(df_converted, connection_string, table)
            
            if not succes:
                logging.error(f"Fout bij het schrijven van {table} data")
                return False
                
            logging.info(f"{table} data succesvol verwerkt")
            return True
        
    except Exception as e:
        logging.error(f"Fout tijdens REST data extractie: {e}")
        return False
