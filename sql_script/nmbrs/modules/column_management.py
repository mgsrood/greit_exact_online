from dataclasses import dataclass
import pandas as pd
import logging
import traceback

@dataclass
class ColumnMappingConfig:
    """Configuratie class voor kolom mappings."""
    mappings = None

    def __post_init__(self):
        self.mappings = {
            "Bedrijven": {
                'ID': 'BedrijfID',
                'Number': 'Bedrijfsnummer',
                'Name': 'Bedrijfsnaam',
            },
            "Looncomponenten": {
                'CompanyID': 'BedrijfID',
                'CompanyNumber': 'Bedrijfsnummer',
                'CompanyName': 'Bedrijfsnaam',
                'EmployeeID': 'WerknemerID',
                'EmployeeNumber': 'Werknemer_nummer',
                'ComponentGuid': 'LooncomponentID',
                'ComponentNumber': 'Looncomponent_nummer',
                'ComponentName': 'Looncomponentnaam',
                'ComponentValue': 'Looncomponentwaarde',
                'Period': 'Periode',
                'Jaar': 'Jaar',
                'Run': 'Run'
            }
        }
        
    def get_mapping(self, table_name):
        """Haal de kolom mapping op voor een specifieke tabel.
        
        Args:
            table_name: Naam van de tabel waarvoor mapping nodig is
            
        Returns:
            Dict met kolom mappings of None als tabel niet bestaat
        """
        return self.mappings.get(table_name)

def transform_columns(df, column_mapping, company_id=None):
    """Transform kolommen van een DataFrame volgens de gegeven mapping.
    
    Args:
        df: Input DataFrame
        column_mapping: Dictionary met oude->nieuwe kolomnamen
        
    Returns:
        Getransformeerde DataFrame
    
    Raises:
        ValueError: Als verplichte kolommen ontbreken
    """
    try:
        # Voor lege DataFrames
        if df.empty:
            logging.info("Lege DataFrame ontvangen, retourneer lege DataFrame met correcte kolommen")
            return pd.DataFrame(columns=list(column_mapping.values()))

        # Selecteer alleen de kolommen die in de mapping staan
        df = df[list(column_mapping.keys())]
        
        # Hernoem kolommen
        df = df.rename(columns=column_mapping)
        
        # Controleer en herorden kolommen
        new_columns = list(column_mapping.values())
        missing_columns = [col for col in new_columns if col not in df.columns]
        
        if missing_columns:
            error_msg = f"Ontbrekende kolommen in DataFrame: {', '.join(missing_columns)}"
            logging.error(error_msg)
            raise ValueError(error_msg)

        return df

    except Exception as e:
        logging.error(f"Fout bij transformeren van kolommen: {str(e)}")
        raise

def apply_column_mapping(df, table_name, company_id=None, config=None):
    """Pas kolom mapping toe op een DataFrame.
    
    Args:
        df: Input DataFrame
        table_name: Naam van de tabel
        config: Optionele mapping configuratie
        
    Returns:
        Getransformeerde DataFrame of None bij fouten
    """
    try:
        if config is None:
            config = ColumnMappingConfig()
        
        column_mapping = config.get_mapping(table_name)
        if column_mapping is None:
            return None


        df_transformed = transform_columns(df, column_mapping, company_id)
        return df_transformed

    except Exception as e:
        logging.error(f"Fout bij toepassen kolom mapping: {str(e)}")
        logging.error(f"Stack trace: {traceback.format_exc()}")
        return None