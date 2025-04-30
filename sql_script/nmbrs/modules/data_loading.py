import pandas as pd
import logging

def get_debiteuren_from_db(config_manager, connection_string):
    """
    Haalt alle debiteuren op uit de database.
    
    Args:
        config_manager: Instantie van ConfigManager
        connection_string: Connectiestring voor de database
        
    Returns:
        pandas.DataFrame: DataFrame met debiteuren data
    """
    try:
        with config_manager.get_connection(connection_string) as conn:
            query = """
                SELECT DebtorID
                FROM Debiteuren
            """
            df = pd.read_sql(query, conn)
            logging.info("Debiteuren succesvol opgehaald uit de database")
            return df
    except Exception as e:
        logging.error(f"Fout bij het ophalen van debiteuren uit de database: {e}")
        return None

def get_bedrijven_from_db(config_manager, connection_string):
    """
    Haalt alle bedrijven op uit de database.
    
    Args:
        config_manager: Instantie van ConfigManager
        connection_string: Connectiestring voor de database
        
    Returns:
        pandas.DataFrame: DataFrame met bedrijven data
    """
    try:
        with config_manager.get_connection(connection_string) as conn:
            query = """
                SELECT CompanyID, Bedrijfsnaam as Naam
                FROM Bedrijven
            """
            df = pd.read_sql(query, conn)
            logging.info("Bedrijven succesvol opgehaald uit de database")
            return df
    except Exception as e:
        logging.error(f"Fout bij het ophalen van bedrijven uit de database: {e}")
        return None
