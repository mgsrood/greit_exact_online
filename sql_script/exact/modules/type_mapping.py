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
            "CrediteurenOpenstaand": {
                'ID': 'int',
                'AdministratieCode': 'int',
                'RelatieID': 'nvarchar',
                'Valuta': 'nvarchar',
                'Bedrag': 'decimal',
                'Omschrijving': 'nvarchar',
                'BoekstukNummer': 'nvarchar',
                'FactuurNummer': 'nvarchar',
                'Referentie': 'nvarchar',
                'FactuurDatum': 'date',
                'VervalDatum': 'date'
            },
            "DebiteurenOpenstaand": {
                'ID': 'int',
                'AdministratieCode': 'int',
                'RelatieID': 'nvarchar',
                'Valuta': 'nvarchar',
                'Bedrag': 'decimal',
                'Omschrijving': 'nvarchar',
                'BoekstukNummer': 'nvarchar',
                'FactuurNummer': 'nvarchar',
                'Referentie': 'nvarchar',
                'FactuurDatum': 'date',
                'VervalDatum': 'date'
            },
            "GrootboekRubriek": {
                'ID': 'uniqueidentifier',
                'AdministratieCode': 'int',
                'RubriekCode': 'nvarchar',
                'Naam': 'nvarchar',
                'ParentID': 'nvarchar',
                'GrootboekSchema': 'nvarchar'
            },
            "Grootboekrekening": {
                'ID': 'uniqueidentifier',
                'AdministratieCode': 'int',
                'Grootboekrekeningen': 'nvarchar',
                'Grootboek': 'nvarchar',
                'BalansType': 'nvarchar',
                'DebetCredit': 'nvarchar',
                'GrootboekIndelingID': 'nvarchar',
                'GrootboekIndeling': 'nvarchar',
                'KostenplaatsID': 'nvarchar',
                'Kostenplaats': 'nvarchar',
                'KostenDragerID': 'nvarchar',
                'KostenDrager': 'nvarchar'
            },
            "Relaties": {
                'ID': 'uniqueidentifier',
                'AdministratieCode': 'int',
                'RelatieCode': 'nvarchar',
                'KlantSinds': 'date',
                'KlantNaam': 'nvarchar',
                'Adres': 'nvarchar',
                'Postcode': 'nvarchar',
                'Plaats': 'nvarchar',
                'Provincie': 'nvarchar',
                'Land': 'nvarchar',
                'Relatiebeheerder': 'nvarchar',
                'BusinessType': 'nvarchar',
                'BusinessSector': 'nvarchar',
                'BusinessSubSector': 'nvarchar',
                'RelatieGroep': 'nvarchar',
                'RelatieGroepID1': 'nvarchar',
                'RelatieGroepID2': 'nvarchar',
                'RelatieGroepID3': 'nvarchar',
                'RelatieGroepID4': 'nvarchar',
                'RelatieGroepID5': 'nvarchar',
                'RelatieGroepID6': 'nvarchar',
                'RelatieGroepID7': 'nvarchar',
                'RelatieGroepID8': 'nvarchar',
                'RelatieGroepOmschrijving': 'nvarchar',
                'Parent': 'nvarchar',
                'IsSupplier': 'bit',
                'IsPurchase': 'bit',
                'Status': 'nvarchar'
            },
            "RelatieKeten": {
                'ID': 'uniqueidentifier',
                'AdministratieCode': 'int',
                'ClassificatieNaam': 'nvarchar',
                'Code': 'nvarchar',
                'Omschrijving': 'nvarchar'
            },
            "Budget": {
                'ID': 'uniqueidentifier',
                'AdministratieCode': 'int',
                'BudgetScenario': 'nvarchar',
                'GrootboekID': 'nvarchar',
                'Kostenplaats': 'nvarchar',
                'Kostendrager': 'nvarchar',
                'Artikel': 'nvarchar',
                'Boekjaar': 'int',
                'Boekperiode': 'int',
                'Bedrag': 'decimal'
            },
            "GrootboekMapping": {
                'ID': 'uniqueidentifier',
                'AdministratieCode': 'int',
                'GrootboekRubriekID': 'nvarchar',
                'GrootboekID': 'nvarchar',
                'GrootboekMappingSchema': 'nvarchar'
            },
            "ReportingBalance": {
                'ID': 'int',
                'AdministratieCode': 'int',
                'GrootboekID': 'nvarchar',
                'ReportingPeriod': 'int',
                'ReportingYear': 'int',
                'AantalMutaties': 'int',
                'KostenPlaats': 'nvarchar',
                'KostenDrager': 'nvarchar',
                'BalansType': 'nvarchar',
                'Bedrag': 'decimal',
                'BedragCredit': 'decimal',
                'BedragDebit': 'decimal'
            },
            "GrootboekMutaties": {
                'ID': 'uniqueidentifier',
                'AdministratieCode': 'int',
                'GrootboekID': 'nvarchar',
                'RelatieID': 'nvarchar',
                'Datum': 'date',
                'Boekjaar': 'int',
                'BoekPeriode': 'int',
                'Omschrijving': 'nvarchar',
                'BoekstukNummer': 'nvarchar',
                'FactuurNummer': 'nvarchar',
                'OrderNummer': 'nvarchar',
                'Referentie': 'nvarchar',
                'Valuta': 'nvarchar',
                'Bedrag': 'decimal',
                'BTWBedrag': 'decimal',
                'BTWPercentage': 'decimal',
                'Type': 'int',
                'KostenplaatsID': 'nvarchar',
                'Kostenplaats': 'nvarchar',
                'KostenDragerID': 'nvarchar',
                'KostenDrager': 'nvarchar',
            },
            "Voorraad": {
                'ID': 'uniqueidentifier',
                'HuidigeVoorraad': 'decimal',
                'AdministratieCode': 'int',
                'ArtikelID': 'nvarchar',
                'ArtikelEindDatum': 'date',
                'ArtikelStartDatum': 'date',
                'ArtikelEenheid': 'nvarchar',
                'ArtikelEenheidOmschrijving': 'nvarchar',
                'MaximaleVoorraad': 'decimal',
                'GeplandeInkomendeVoorraad': 'decimal',
                'GeplandeUitgaandeVoorraad': 'decimal',
                'VerwachteVoorraad': 'decimal',
                'GereserveerdeVoorraad': 'decimal',
                'Veiligheidsvoorraad': 'decimal',
                'MagazijnID': 'nvarchar',
                'MagazijnCode': 'nvarchar',
                'MagazijnOmschrijving': 'nvarchar',
            },
            "Artikelen": {
                'ID': 'uniqueidentifier',
                'StandaardVerkoopprijs': 'decimal',
                'Categorie1': 'nvarchar',
                'Categorie2': 'nvarchar',
                'Categorie3': 'nvarchar',
                'Categorie4': 'nvarchar',
                'Categorie5': 'nvarchar',
                'Categorie6': 'nvarchar',
                'Categorie7': 'nvarchar',
                'Categorie8': 'nvarchar',
                'Categorie9': 'nvarchar',
                'Categorie10': 'nvarchar',
                'Artikelcode': 'nvarchar',
                'Valuta': 'nvarchar',
                'NieuweKostprijs': 'decimal',
                'StandaardKostprijs': 'decimal',
                'GemiddeldeKostprijs': 'decimal',
                'AangemaaktOp': 'date',
                'Artikelomschrijving': 'nvarchar',
                'AdministratieCode': 'int',
                'Einddatum': 'date',
                'ExtraOmschrijving': 'nvarchar',
                'VrijBooleanVeld1': 'bit',
                'VrijBooleanVeld2': 'bit',
                'VrijBooleanVeld3': 'bit',
                'VrijBooleanVeld4': 'bit',
                'VrijBooleanVeld5': 'bit',
                'VrijDatumVeld1': 'date',
                'VrijDatumVeld2': 'date',
                'VrijDatumVeld3': 'date',
                'VrijDatumVeld4': 'date',
                'VrijDatumVeld5': 'date',
                'VrijNummerVeld1': 'decimal',
                'VrijNummerVeld2': 'decimal',
                'VrijNummerVeld3': 'decimal',
                'VrijNummerVeld4': 'decimal',
                'VrijNummerVeld5': 'decimal',
                'VrijNummerVeld6': 'decimal',
                'VrijNummerVeld7': 'decimal',
                'VrijNummerVeld8': 'decimal',
                'VrijTekstVeld1': 'nvarchar',
                'VrijTekstVeld2': 'nvarchar',
                'VrijTekstVeld3': 'nvarchar',
                'VrijTekstVeld4': 'nvarchar',
                'VrijTekstVeld5': 'nvarchar',
                'VrijTekstVeld6': 'nvarchar',
                'VrijTekstVeld7': 'nvarchar',
                'VrijTekstVeld8': 'nvarchar',
                'VrijTekstVeld9': 'nvarchar',
                'VrijTekstVeld10': 'nvarchar',
                'Artikelgroep': 'nvarchar',
                'IsMaakartikel': 'tinyint',
                'IsNieuwContract': 'tinyint',
                'IsOpAanvraagArtikel': 'tinyint',
                'IsVerpakkingArtikel': 'bit',
                'IsInkoopArtikel': 'bit',
                'IsVerkoopArtikel': 'bit',
                'IsSerieArtikel': 'bit',
                'IsVoorraadArtikel': 'bit',
                'IsOnderaannemerArtikel': 'bit',
                'IsBelastingArtikel': 'tinyint',
                'IsTijdArtikel': 'tinyint',
                'IsWebshopArtikel': 'tinyint',
                'BrutoGewicht': 'decimal',
                'NettoGewicht': 'decimal',
                'NettoGewichtEenheid': 'nvarchar',
                'Notities': 'nvarchar',
                'BTWcode': 'nvarchar',
                'BTWcodeBeschrijving': 'nvarchar',
                'Beveiligingsniveau': 'int',
                'Startdatum': 'date',
                'GoederenCode': 'nvarchar',
                'Eenheid': 'nvarchar',
                'EenheidBeschrijving': 'nvarchar',
                'EenheidType': 'nvarchar',
            },
            "ArtikelenExtraVelden": {
                'ArtikelID': 'nvarchar',
                'GewijzigdOp': 'date',
                'Nummer': 'int',
                'Omschrijving': 'nvarchar',
                'Waarde': 'nvarchar',
                'AdministratieCode': 'int',
            },
            "ArtikelGroepen": {
                'ID': 'uniqueidentifier',
                'Code': 'nvarchar',
                'Omschrijving': 'nvarchar',
                'AdministratieCode': 'int',
            },
            "Verkoopfacturen": {
                'F_Valuta': 'nvarchar',
                'F_VersturenNaarID': 'nvarchar',
                'F_Omschrijving': 'nvarchar',
                'F_AdministratieCode': 'int',
                'F_Factuurdatum': 'date',
                'F_FactuurID': 'nvarchar',
                'F_Factuurnummer': 'int',
                'F_FactuurNaarID': 'nvarchar',
                'F_Orderdatum': 'date',
                'F_BesteldDoorID': 'nvarchar',
                'F_BetaalconditieBeschrijving': 'nvarchar',
                'F_Opmerkingen': 'nvarchar',
                'F_VerzendmethodeBeschrijving': 'nvarchar',
                'F_StatusBeschrijving': 'nvarchar',
                'F_UwReferentie': 'nvarchar',
                'F_StartVerkoopfactuurStatus': 'nvarchar',
                'FR_Bedrag': 'decimal',
                'FR_Kostenplaats': 'nvarchar',
                'FR_Kostendrager': 'nvarchar',
                'FR_Leverdatum': 'date',
                'FR_Omschrijving': 'nvarchar',
                'FR_Korting': 'decimal',
                'FR_WerknemerNaam': 'nvarchar',
                'FR_GrootboekrekeningID': 'nvarchar',
                'FR_FactuurregelID': 'uniqueidentifier',
                'FR_ArtikelID': 'nvarchar',
                'FR_Regelnummer': 'int',
                'FR_Aantal': 'decimal',
                'FR_VerkooporderID': 'nvarchar',
                'FR_VerkooporderRegelID': 'nvarchar',
                'FR_EenheidBeschrijving': 'nvarchar',
                'FR_Eenheidsprijs': 'decimal',
                'FR_BTWBedrag': 'decimal',
                'FR_BTWPercentage': 'decimal',
            },
            "VerkoopOrders": {
                'O_ApprovalStatusBeschrijving': 'nvarchar',
                'O_Goedgekeurd': 'date',
                'O_GoedgekeurdDoorNaam': 'nvarchar',
                'O_Aangemaakt': 'date',
                'O_AangemaaktDoorNaam': 'nvarchar',
                'O_Valuta': 'nvarchar',
                'O_VersturenNaarID': 'nvarchar',
                'O_Omschrijving': 'nvarchar',
                'O_AdministratieCode': 'int',
                'O_FactuurStatusBeschrijving': 'nvarchar',
                'O_FactuurNaarID': 'nvarchar',
                'O_Orderdatum': 'date',
                'O_BesteldDoorNaam': 'nvarchar',
                'O_OrderID': 'nvarchar',
                'O_Ordernummer': 'int',
                'O_Opmerkingen': 'nvarchar',
                'O_VerzendmethodeBeschrijving': 'nvarchar',
                'O_StatusBeschrijving': 'nvarchar',
                'O_UwReferentie': 'nvarchar',
                'OR_Bedrag': 'decimal',
                'OR_Kostenplaats': 'nvarchar',
                'OR_Kostprijs': 'decimal',
                'OR_Kostendrager': 'nvarchar',
                'OR_Leverdatum': 'date',
                'OR_Omschrijving': 'nvarchar',
                'OR_Korting': 'decimal',
                'OR_OrderRegelID': 'uniqueidentifier',
                'OR_ArtikelID': 'nvarchar',
                'OR_Regelnummer': 'int',
                'OR_Aantal': 'decimal',
                'OR_BTWBedrag': 'decimal',
                'OR_BTWPercentage': 'decimal',
            },
            "Verkoopkansen": {
                'VerkoopkansID': 'uniqueidentifier',
                'AccountID': 'nvarchar',
                'Actiedatum': 'date',
                'Bedrag': 'decimal',
                'Sluitingsdatum': 'date',
                'Aangemaakt': 'date',
                'AangemaaktDoorNaam': 'nvarchar',
                'Valuta': 'nvarchar',
                'AdministratieCode': 'int',
                'Naam': 'nvarchar',
                'FaseBeschrijving': 'nvarchar',
                'Status': 'int',
                'Kans': 'decimal',
                'EigenaarNaam': 'nvarchar',
                'VerkooptypeBeschrijving': 'nvarchar',
                'RedenBeschrijving': 'nvarchar',
                'CampagneBeschrijving': 'nvarchar',
                'LeadBronBeschrijving': 'nvarchar',
                'ContactpersoonNaam': 'nvarchar',
            },
            "Offertes": {
                'O_AdministratieCode': 'int',
                'O_OfferteID': 'nvarchar',
                'O_Offertenummer': 'int',
                'O_Versie': 'int',
                'OR_OfferteRegelID': 'uniqueidentifier',
                'O_Medewerker': 'nvarchar',
                'O_Valuta': 'nvarchar',
                'O_Omschrijving': 'nvarchar',
                'O_Status': 'nvarchar',
                'O_RelatieID': 'nvarchar',
                'O_VerkoopKansID': 'nvarchar',
                'O_OfferteDatum': 'date',
                'O_VervalDatum': 'date',
                'O_EindDatum': 'date',
                'O_AfleverDatum': 'date',
                'O_Opmerkingen': 'nvarchar',
                'O_UwReferentie': 'nvarchar',
                'OR_Bedrag': 'decimal',
                'OR_Kostenplaats': 'nvarchar',
                'OR_Kostendrager': 'nvarchar',
                'OR_Omschrijving': 'nvarchar',
                'OR_Korting': 'decimal',
                'OR_ArtikelID': 'nvarchar',
                'OR_Regelnummer': 'int',
                'OR_Aantal': 'decimal',
                'OR_Eenheid': 'nvarchar',
                'OR_PrijsPerEenheid': 'decimal',
                'OR_BTWBedrag': 'decimal',
                'OR_BTWPercentage': 'decimal'
            },
            "Divisions": {
                'Division': 'int',
                'Entiteit': 'nvarchar',
                'CustomerCode': 'int',
                'CustomerName': 'nvarchar',
                'State': 'nvarchar',
                'City': 'nvarchar',
                'HID': 'nvarchar',
                'CompanySizeDescription': 'nvarchar'
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
                    df[column] = df[column].apply(lambda x: str(uuid.UUID(x)) if pd.notna(x) else None)
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
                                # Haal timestamp uit string en converteer naar milliseconden
                                timestamp = int(value.replace('/Date(', '').replace(')/', ''))
                                return pd.Timestamp(timestamp, unit='ms').date()
                            else:
                                # Voor andere formaten, gebruik pandas to_datetime
                                date_value = pd.to_datetime(value, errors='coerce')
                                return date_value.date() if pd.notna(date_value) else None
                        except (ValueError, TypeError):
                            return None
                            
                    df[column] = df[column].apply(convert_date)
                    
                    # Controleer op ongeldige datums (alleen None of date objecten zijn geldig)
                    invalid_dates = df[column].apply(lambda x: x is not None and not isinstance(x, date))
                    if invalid_dates.any():
                        logging.error(f"Ongeldige datums gevonden in kolom {column}")
                        raise ValueError(f"Ongeldige datums gevonden in kolom {column}")
                        
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