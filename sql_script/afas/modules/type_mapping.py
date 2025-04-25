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
            "GrootboekRubriek": {
                "OmgevingID": "int",
                "Grootboek_RubriekID": "nvarchar",
                "Rubriek_Code": "nvarchar",
                "Rubriek": "nvarchar",
                "Balanscode": "nvarchar",
                "Soort": "nvarchar",
                "Hoofdgroep_Code": "nvarchar",
                "HoofdgroepID": "nvarchar",
                "Hoofdgroep_Omschrijving": "nvarchar",
                "Subgroep_Code": "nvarchar",
                "SubgroepID": "nvarchar",
                "Subgroep_Omschrijving": "nvarchar",
                "Subgroep2_Code": "nvarchar",
                "Subgroep2ID": "nvarchar",
                "Subgroep2_Omschrijving": "nvarchar",
            },
            "Grootboekrekening": {
                'OmgevingID': 'int',
                'Administratie_Code': 'int',
                'GrootboekID': 'nvarchar',
                'Type_RekeningID': 'nvarchar',
                'Type_Rekening_Omschrijving': 'nvarchar',
                'Kenmerk_RekeningID': 'nvarchar',
                'Kenmerk_Rekening_Omschrijving': 'nvarchar',
                'Omschrijving': 'nvarchar',
                'Soort_Rekening': 'nvarchar',
                'Rubriek_Code': 'nvarchar',
                'Balans_Code': 'nvarchar',
                'Gewijzigd_Op': 'date',
                'Intercompany': 'bit',
            },
            "Budget": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "Budget_Scenario_Code": "nvarchar",
                "Budget_Scenario": "nvarchar",
                "GrootboekID": "nvarchar",
                "Boekjaar": "int",
                "Boekperiode": "int",
                "Datum": "date",
                "Bedrag": "decimal",
                "KostenplaatsID": "nvarchar",
                "KostendragerID": "nvarchar",
            },
            "GrootboekMutaties": {
                'OmgevingID': 'int',
                'Administratie_Code': 'int',
                'Boekjaar': 'int',
                'Code_Dagboek': 'nvarchar',
                'Nummer_Journaalpost': 'int',
                'Volgnummer_Journaalpost': 'int',
                'GrootboekID': 'nvarchar',
                'RelatieID': 'nvarchar',
                'Datum': 'date',
                'Boekperiode': 'int',
                'Omschrijving': 'nvarchar',
                'Boekstuk_Nummer': 'nvarchar',
                'Factuur_Nummer': 'nvarchar',
                'Order_Nummer': 'nvarchar',
                'Valuta': 'nvarchar',
                'Bedrag': 'decimal',
                'BTW_Bedrag': 'decimal',
                'BTW_Percentage': 'decimal',
                'Type_Boeking': 'nvarchar',
                'Type_Mutatie': 'nvarchar',
                'Dagboek_Soort': 'nvarchar',
                'KostenplaatsID': 'nvarchar',
                'KostendragerID': 'nvarchar',
                'Gewijzigd_Op': 'datetime'
            },
            "Divisions": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "Administratie_Naam": "nvarchar",
            },
            "Medewerkers": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "GUID": "uniqueidentifier",
                "Medewerker": "nvarchar",
                "Start_Datum": "date",
                "Eind_Datum": "date",
                "Geslacht": "nvarchar",
                "Functie": "nvarchar",
                "Type_Functie": "nvarchar",
                "AfdelingID": "nvarchar",
                "Afdeling": "nvarchar",
            },
            "Projecten": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "Project_Classificatie": "nvarchar",
                "ProjectID": "int",
                "Project": "nvarchar",
                "Project_Type": "nvarchar",
                "Project_Status": "nvarchar",
                "Project_Status_Code": "int",
                "Projectleider_GUID": "uniqueidentifier",
                "Uitvoerend_Consultant_GUID": "uniqueidentifier",
                "Opdrachtnemer_GUID": "uniqueidentifier",
                "Organisatie_ID": "int",
                "OpdrachtgeverID": "nvarchar",
                "RelatieID": "int",
                "Contactpersoon": "nvarchar",
                "HoofdprojectID": "int",
                "Hoofdproject_Omschrijving": "nvarchar",
                "Hoofdproject_Status_Code": "int",
                "Hoofdproject_Status": "nvarchar",
                "Start_Datum": "date",
                "Voorcalculatie_Uren": "decimal",
                "Voorcalculatie_Kostprijs": "decimal",
                "Nacalculatie_Uren": "decimal",
                "Nacalculatie_Kostprijs": "decimal",
                "Eind_Datum_Planning": "date",
                "Eind_Datum": "date",
                "Voortgang_Percentage": "decimal",
                "Decharge_Verzonden": "bit",
                "Afgemeld": "bit",
                "Gewijzigd_Op": "datetime",
            },
            "Relaties": {
                "OmgevingID": "int",
                "Soort_Contact": "nvarchar",
                "Naam": "nvarchar",
                "Inkooprelatie": "bit",
                "Verkooprelatie": "bit",
                "Medewerker": "bit",
                "Medewerker_GUID": "uniqueidentifier",
                "VerkooprelatieID": "int",
                "InkooprelatieID": "int",
            },
            "Urenregistratie": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "Urenregistratie_GUID": "uniqueidentifier",
                "ProjectID": "int",
                "RelatieID": "int",
                "Medewerker_GUID": "uniqueidentifier",
                "Uur_SoortID": "int",
                "Datum": "date",
                "Datum_gewijzigd": "date",
                "Aantal": "decimal",
                "Valuta_Code": "nvarchar",
                "Prijs": "decimal",
                "Bedrag": "decimal",
                "Geaccordeerd": "bit",
                "Gereedgemeld": "bit",
                "Gefactureerd": "bit",
                "Kostprijs": "decimal",
                "Kostprijs_Bedrag": "decimal",
                "Doorbelasten": "bit",
                "Uur_Type": "int",
                "Uur_Type_Detail": "nvarchar",
            },
            "Verlof": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "Medewerker_GUID": "uniqueidentifier",
                "Verlof": "nvarchar",
                "Start_Datum": "date",
                "Eind_Datum": "date",
                "Verlof_Uren": "decimal",
                "Verlof_Type": "nvarchar",
                "AfdelingID": "nvarchar",
            },
            "VerzuimUren": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "Medewerker_GUID": "uniqueidentifier",
                "Verzuim": "nvarchar",
                "Start_Datum": "datetime",
                "Eind_Datum": "datetime",
                "Verzuim_Uren": "decimal",
                "Aanwezigheid_Percentage": "decimal",
                "Verzuim_Type": "nvarchar",
                "Status": "decimal",
                "AfdelingID": "nvarchar",
            },
            "VerzuimVerloop": {
                "OmgevingID": "int",
                "Verzuimmelding_GUID": "uniqueidentifier",
                "Verzuimverloop_GUID": "uniqueidentifier",
                "Medewerker_GUID": "uniqueidentifier",
                "Gewijzigd_Op": "datetime",
                "Datum": "datetime",
                "Vangnet_Regeling": "bit",
                "Aanwezigheid_Percentage": "decimal",
            },
            "Contracten": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "Arbeids_ContractID": "int",
                "Medewerker_GUID": "uniqueidentifier",
                "Medewerker_Type": "nvarchar",
                "Start_Datum": "date",
                "Eind_Datum": "date",
                "Proeftijd_Eind_Datum": "date",
                "Contract_Type": "nvarchar",
                "Volgnummer": "int",
                "AfdelingID": "nvarchar",
                "Meedere_Dienstverbanden": "bit",
                "Aantal_FTE": "decimal",
                "Dagen_Per_Week": "decimal",
                "Uren_Per_Week": "decimal",
            },
            "Abonnementen": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "AbonnementID": "int",
                "Licentiehouder": "nvarchar",
                "RelatieID": "int",
                "Abonnement_Type": "nvarchar",
                "Item": "nvarchar",
                "Licentie_Type": "nvarchar",
                "Aantal": "decimal",
                "Eenheid": "nvarchar",
                "Prijs": "decimal",
                "Waarde": "decimal",
                "Begin_Regel": "date",
                "Start_Datum": "date",
                "Eind_Regel": "date",
                "Eind_Datum": "date",
                "Gefactureerd_Tot": "date",
                "Gefactureerd_Vanaf": "date",
                "Gecrediteerd": "bit",
                "Crediteren_Vanaf": "date",
                "Cyclus_Code": "nvarchar",
                "Cyclus": "nvarchar",
                "Cyclus_Begin": "date",
                "Gewijzigd_Op": "datetime",
            },
            "CaseLogging": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "Logregel": "int",
                "DossierID": "int",
                "ForecastID": "int",
                "ActieID": "int",
                "Volgende_Actie": "nvarchar",
                "Begin_Datum": "datetime",
                "Uitvoer_Datum": "datetime",
                "Gekozen_Actie": "nvarchar",
                "Doorlooptijd_Sec": "decimal",
                "Doorlooptijd_Uur": "decimal",
                "Gewijzigd_Op": "datetime",
                "Omschrijving": "nvarchar",
            },
            "Dossiers": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "DossierID": "int",
                "Dossier_Nummer": "int",
                "Instuur_Datum": "date",
                "Onderwerp": "nvarchar",
                "Aanmaker": "nvarchar",
                "AanmakerID": "int",
                "Verantwoordelijke": "nvarchar",
                "VerantwoordelijkeID": "int",
                "Potentiele_Omzet": "decimal",
                "Oplossing": "nvarchar",
                "Volgende_Actie": "nvarchar",
                "Prioriteit": "nvarchar",
                "Forecast": "nvarchar",
                "Huidige_Status": "nvarchar",
                "Datum_Afgehandeld": "datetime",
                "Gewijzigd_Op": "datetime",
                "Organisatie_Naam": "nvarchar",
                "OrganisatieID": "int",
                "ForecastID": "int",
                "Eind_Datum": "datetime",
                "Bevinding": "nvarchar",
                "Prioriteit_Herstelcase": "nvarchar",
                "Reactie_Datum": "datetime",
                "TypeDossieritemID": "int",
                "TypeDossierOmschrijving": "nvarchar",
                "Woonplaats": "nvarchar",
                "Postcode": "nvarchar",
            },
            "Forecasts": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "ForecastID": "int",
                "OrganisatieID": "int",
                "Organisatie_Naam": "nvarchar",
                "Start_Datum": "date",
                "Eind_Datum_Verwacht": "date",
                "Eind_Datum": "date",
                "Forecast_Bedrag": "decimal",
                "Verwacht_Bedrag": "decimal",
                "Verantwoordelijke_GUID": "uniqueidentifier",
                "Omschrijving": "nvarchar",
                "Forecastgroep_Code": "int",
                "Forecastgroep": "nvarchar",
                "Datum_Laatste_Wijziging": "datetime",
                "Status": "nvarchar",
                "Voortgang": "nvarchar",
                "RPA_Diensten": "decimal",
                "RPA_Licentie": "decimal",
                "Bruto_Omzet": "decimal",
                "Aantal_Uren": "decimal",
                "Scoringskans": "decimal",
                "Oplossing": "nvarchar",
                "Type_Abonnement_Code_Verwacht": "int",
                "Type_Abonnement_Verwacht": "nvarchar",
                "Gewijzigd_Op": "datetime",
            },
            "Nacalculatie": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "Nacalculatie_GUID": "uniqueidentifier",
                "Datum": "date",
                "Boekjaar": "int",
                "Periode": "int",
                "ProjectID": "int",
                "Project_Naam": "nvarchar",
                "Projectgroep": "nvarchar",
                "Project_Type": "nvarchar",
                "Start_Datum": "date",
                "Organisatie": "nvarchar",
                "OrganisatieID": "int",
                "RelatieID": "int",
                "Uitvoerend_Consultant_GUID": "uniqueidentifier",
                "Medewerker_GUID": "uniqueidentifier",
                "Uren": "decimal",
                "Omschrijving": "nvarchar",
                "Werksoort_Type": "int",
                "Werksoort": "nvarchar",
                "Integratiegroep_Code": "nvarchar",
                "Integratiegroep": "nvarchar",
                "Afdeling_Code": "nvarchar",
                "Geaccordeerd": "bit",
                "Doorbelasten": "bit",
                "Gefactureerd": "bit",
                "Verkoop_Bedrag": "decimal",
                "Uurprijs_Verkoop": "decimal",
                "Item_Code": "nvarchar",
                "Item_Omschrijving": "nvarchar",
                "Innovatie": "nvarchar",
                "WBSO": "bit",
                "Jira_Ticket_Nummer": "nvarchar",
                "Gewijzigd_Op": "datetime",
            },
            "Roosters": {
                "OmgevingID": "int",
                "Administratie_Code": "int",
                "Medewerker_Naam": "nvarchar",
                "Medewerker_GUID": "uniqueidentifier",
                "Type": "nvarchar",
                "Datum": "date",
                "Uren": "decimal",
                "FTE": "decimal",
                "Parttime_Percentage": "decimal",
                "Afdeling_Code": "nvarchar",
                "Gewijzigd_Op": "datetime",
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
                                logging.warning(f"Integer waarde gevonden voor UUID kolom {column}: {value}")
                                return None
                            return str(uuid.UUID(str(value)))
                        except (ValueError, TypeError) as e:
                            logging.warning(f"Kon UUID niet converteren: {value} - {str(e)}")
                            return None
                            
                    df[column] = df[column].apply(convert_uuid)
                elif dtype == 'int':
                    df[column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)
                elif dtype == 'nvarchar':
                    df[column] = df[column].astype(str)
                elif dtype == 'decimal':
                    df[column] = df[column].apply(lambda x: Decimal(x) if pd.notna(x) else None)
                elif dtype == 'bit':
                    df[column] = df[column].astype(str).str.lower().map({'true': True, 'false': False, '1': True, '0': False}).astype(bool)
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
                        logging.warning(f"Aantal ongeldige datetime waarden in kolom {column}: {invalid_count}")
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

def add_environment_id(df, environment_id):
    if environment_id is None:
        return None
    
    else: 
        df['OmgevingID'] = environment_id
        return df