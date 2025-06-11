from datetime import date, datetime
from dataclasses import dataclass
from decimal import Decimal
import pandas as pd
import numpy as np
import logging
import uuid

@dataclass
class TypeMappingConfig:
    """Configuratie class voor type mappings."""
    mappings = None

    def __post_init__(self):
        self.mappings = {
            "Bedrijven": {
                'BedrijfID': 'int',
                'Bedrijfsnummer': 'int',
                'Bedrijfsnaam': 'nvarchar',
            },
            "Looncomponenten": {
                'BedrijfID': 'int',
                'Bedrijfsnummer': 'int',
                'Bedrijfsnaam': 'nvarchar',
                'WerknemerID': 'int',
                'Werknemer_nummer': 'int',
                'LooncomponentID': 'int',
                'Looncomponent_nummer': 'int',
                'Looncomponentnaam': 'nvarchar',
                'Looncomponentwaarde': 'decimal',
                'Periode': 'int',
                'Jaar': 'int',
                'Run': 'int'
            }
        }

    def get_mapping(self, table_name):
        """Haal de type mapping op voor een specifieke tabel.
            
        Args:
            table_name: Naam van de tabel waarvoor mapping nodig is
            
        Returns:
            Dict met type mappings of None als tabel niet bestaat
        """
        return self.mappings.get(table_name)

def convert_column_types(df, column_types):
    """Converteer kolom types van een DataFrame volgens de gegeven mapping."""
    try:
        pd.set_option('future.no_silent_downcasting', True)
        
        for column, dtype in column_types.items():
            if column not in df.columns:
                raise ValueError(f"Kolom '{column}' niet gevonden in DataFrame.")
                
            try:
                if dtype == 'uniqueidentifier':
                    def convert_uuid(value):
                        if pd.isna(value):
                            return None
                        try:
                            if isinstance(value, (int, float)):
                                if value == 0:
                                    return None
                                logging.info(f"Integer waarde gevonden voor UUID kolom {column}: {value}")
                                return None
                            return str(uuid.UUID(str(value)))
                        except (ValueError, TypeError) as e:
                            logging.info(f"Kon UUID niet converteren: {value} - {str(e)}")
                            return None
                            
                    df[column] = df[column].apply(convert_uuid)
                elif dtype == 'int':
                    df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
                elif dtype == 'nvarchar':
                    df[column] = df[column].astype(str)
                elif dtype == 'decimal':
                    df[column] = df[column].apply(lambda x: Decimal(x) if pd.notna(x) else None)
                elif dtype == 'bit':
                    df[column] = df[column].astype(str).str.lower().map({'true': True, '1': True, 'false': False, '0': False}).fillna(False).astype(bool)
                elif dtype == 'date':
                    def convert_date(value):
                        if pd.isna(value):
                            return None
                        try:
                            if isinstance(value, str) and '/Date(' in value:
                                timestamp = int(value.replace('/Date(', '').replace(')/', ''))
                                return pd.Timestamp(timestamp, unit='ms').date()
                            else:
                                date_value = pd.to_datetime(value, errors='coerce')
                                return date_value.date() if pd.notna(date_value) else None
                        except (ValueError, TypeError) as e:
                            logging.error(f"Fout bij datum conversie: {value} - {str(e)}")
                            return None
                            
                    df[column] = df[column].apply(convert_date)
                    
                    invalid_dates = df[column].apply(lambda x: x is not None and not isinstance(x, date))
                    if invalid_dates.any():
                        logging.error(f"Ongeldige datums gevonden in kolom {column}")
                        raise ValueError(f"Ongeldige datums gevonden in kolom {column}")
                        
                elif dtype == 'datetime':
                    def convert_datetime(value):
                        if pd.isna(value):
                            return None
                        try:
                            
                            # Als het een integer is, probeer het als timestamp te interpreteren
                            if isinstance(value, (int, float)):
                                try:
                                    return pd.Timestamp(value, unit='ms').tz_localize(None)
                                except (ValueError, TypeError) as e:
                                    logging.error(f"Fout bij integer timestamp conversie: {value} - {str(e)}")
                                    return None
                                
                            # Als het een string is, probeer eerst de /Date() notatie
                            if isinstance(value, str):
                                if '/Date(' in value:
                                    try:
                                        timestamp = int(value.replace('/Date(', '').replace(')/', ''))
                                        return pd.Timestamp(timestamp, unit='ms').tz_localize(None)
                                    except (ValueError, TypeError) as e:
                                        logging.error(f"Fout bij /Date() conversie: {value} - {str(e)}")
                                        pass
                                
                                # Probeer verschillende datetime formaten
                                formats = [
                                    '%Y-%m-%dT%H:%M:%S.%f%z',  # ISO format met timezone
                                    '%Y-%m-%dT%H:%M:%S%z',     # ISO format met timezone (zonder milliseconden)
                                    '%Y-%m-%d %H:%M:%S.%f%z',  # Standaard SQL format met timezone
                                    '%Y-%m-%d %H:%M:%S%z',     # Standaard SQL format met timezone (zonder milliseconden)
                                    '%Y-%m-%dT%H:%M:%S.%f',    # ISO format zonder timezone
                                    '%Y-%m-%dT%H:%M:%S',       # ISO format zonder timezone (zonder milliseconden)
                                    '%Y-%m-%d %H:%M:%S.%f',    # Standaard SQL format zonder timezone
                                    '%Y-%m-%d %H:%M:%S',       # Standaard SQL format zonder timezone (zonder milliseconden)
                                    '%Y-%m-%d',                # Alleen datum
                                    '%d-%m-%Y %H:%M:%S',       # Nederlands format
                                    '%d-%m-%Y'                 # Nederlands datum format
                                ]
                                
                                for fmt in formats:
                                    try:
                                        dt = pd.to_datetime(value, format=fmt)
                                        # Verwijder timezone informatie en converteer naar naive datetime
                                        return dt.tz_localize(None) if dt.tz is not None else dt
                                    except (ValueError, TypeError):
                                        continue
                            
                            # Als laatste optie, probeer pandas' eigen parser
                            dt = pd.to_datetime(value, errors='coerce')
                            return dt.tz_localize(None) if dt.tz is not None else dt
                            
                        except (ValueError, TypeError) as e:
                            logging.error(f"Kon datetime waarde niet converteren: {value} - {str(e)}")
                            return None
                            
                    df[column] = df[column].apply(convert_datetime)
                    
                    # Controleer op ongeldige datetimes
                    invalid_datetimes = df[column].isna()
                    if invalid_datetimes.any():
                        invalid_count = invalid_datetimes.sum()
                        logging.info(f"Aantal ongeldige datetime waarden in kolom {column}: {invalid_count}")
                        # In plaats van een error te gooien, zetten we ongeldige waarden op None
                        df[column] = df[column].where(pd.notna(df[column]), None)
                        
                elif dtype == 'tinyint':
                    df[column] = df[column].fillna(0).infer_objects(copy=False).astype(np.uint8)
                else:
                    raise ValueError(f"Onbekend datatype '{dtype}' voor kolom '{column}'")
                    
            except ValueError as e:
                logging.error(f"Fout bij conversie van kolom '{column}' naar type '{dtype}': {str(e)}")
                raise
                
        return df
        
    except Exception as e:
        logging.error(f"Fout bij type conversie: {str(e)}")
        raise

def apply_type_conversion(df, table_name, config=None):
    """Pas type conversie toe op een DataFrame."""
    try:
        if not isinstance(config, TypeMappingConfig):
            config = TypeMappingConfig()
            
        logging.info(f"Start type conversie voor tabel: {table_name}")
        
        column_types = config.get_mapping(table_name)
        if column_types is None:
            logging.error(f"Geen type mapping gevonden voor tabel: {table_name}")
            return None
            
        try:
            df_converted = convert_column_types(df, column_types)
            logging.info(f"Type conversie succesvol toegepast voor tabel: {table_name}")
            return df_converted
        except Exception as e:
            logging.error(f"Fout bij type conversie: {str(e)}")
            return None
            
    except Exception as e:
        logging.error(f"Fout bij toepassen type conversie: {str(e)}")
        return None