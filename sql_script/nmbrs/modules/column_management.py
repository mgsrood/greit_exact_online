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
            "Debiteuren": {
                'debtorId': 'DebtorID',
                'number': 'Debiteur_Nummer',
                'name': 'Debiteurnaam',
            },
            "Bedrijven": {
                'companyId': 'CompanyID',
                'number': 'Bedrijfs_Nummer',
                'name': 'Bedrijfsnaam',
                'debtorId': 'DebtorID',
            },
            "FTE": {
                'employeeId': 'WerknemerID',
                'CompanyID': 'CompanyID',
                'schedule_scheduleId': 'SchemaID',
                'schedule_startDate': 'Start_Datum',
                'schedule_parttimePercentage': 'FTE_Percentage',
                'schedule_hoursPerWeek': 'Uren_Per_Week',
                'schedule_daysPerWeek': 'Dagen_Per_Week',
                'schedule_createdAt': 'Aanmaakdatum',
                'schedule_week1.hoursMonday': 'Uren_Maandag_Week1',
                'schedule_week1.hoursTuesday': 'Uren_Dinsdag_Week1',
                'schedule_week1.hoursWednesday': 'Uren_Woensdag_Week1',
                'schedule_week1.hoursThursday': 'Uren_Donderdag_Week1',
                'schedule_week1.hoursFriday': 'Uren_Vrijdag_Week1',
                'schedule_week1.hoursSaturday': 'Uren_Zaterdag_Week1',
                'schedule_week1.hoursSunday': 'Uren_Zondag_Week1',
                'schedule_week2.hoursMonday': 'Uren_Maandag_Week2',
                'schedule_week2.hoursTuesday': 'Uren_Dinsdag_Week2',
                'schedule_week2.hoursWednesday': 'Uren_Woensdag_Week2',
                'schedule_week2.hoursThursday': 'Uren_Donderdag_Week2',
                'schedule_week2.hoursFriday': 'Uren_Vrijdag_Week2',
                'schedule_week2.hoursSaturday': 'Uren_Zaterdag_Week2',
                'schedule_week2.hoursSunday': 'Uren_Zondag_Week2',
            },
            "Contracten": {
                'employeeId': 'WerknemerID',
                'CompanyID': 'CompanyID',
                'employment_employmentId': 'ContractID',
                'employment_startDate': 'Start_Datum',
                'employment_endDate': 'Eind_Datum',
                'employment_seniorityDate': 'InDienstNemen_Datum',
                'employment_endContractReason.code': 'Reden_Code',
                'employment_endContractReason.reason': 'Reden_Omschrijving',
                'employment_changedDate': 'Wijzigings_Datum',
            },
            "Uurcodes": {
                'Code': 'Uurcode',
                'Description': 'Omschrijving',
            },
            "Uren_Vast": {
                'Id': 'UurID',
                'Code': 'Uurcode',
                'Value': 'Aantal_Uren',
                'WerknemerID': 'WerknemerID',
            },
            "Uren_Variabel": {
                'Id': 'UurID',
                'Code': 'Uurcode',
                'Value': 'Aantal_Uren',
                'WerknemerID': 'WerknemerID',
            },
            "Uren_Schemas": {
                'HoursMonday': 'Uren_Maandag',
                'HoursTuesday': 'Uren_Dinsdag',
                'HoursWednesday': 'Uren_Woensdag',
                'HoursThursday': 'Uren_Donderdag',
                'HoursFriday': 'Uren_Vrijdag',
                'HoursSaturday': 'Uren_Zaterdag',
                'HoursSunday': 'Uren_Zondag',
                'HoursMonday2': 'Uren_Maandag2',
                'HoursTuesday2': 'Uren_Dinsdag2',
                'HoursWednesday2': 'Uren_Woensdag2',
                'HoursThursday2': 'Uren_Donderdag2',
                'HoursFriday2': 'Uren_Vrijdag2',
                'HoursSaturday2': 'Uren_Zaterdag2',
                'HoursSunday2': 'Uren_Zondag2',
                'ParttimePercentage': 'FTE_Percentage',
                'StartDate': 'Start_Datum',
                'WerknemerID': 'WerknemerID',
            }
        }
        # Voeg alle bestaande mappings toe aan self.mappings
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