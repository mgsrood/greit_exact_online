import pandas as pd
import uuid
from decimal import Decimal
import numpy as np

CrediteurenOpenstaandTyping = {
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
}

DebiteurenOpenstaandTyping = {
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
}

GrootboekRubriekTyping = {
    'ID': 'uniqueidentifier',
    'AdministratieCode': 'int',
    'RubriekCode': 'nvarchar',
    'Naam': 'nvarchar',
    'ParentID': 'nvarchar',
    'GrootboekSchema': 'nvarchar'
}

GrootboekrekeningTyping = {
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
}

RelatiesTyping = {
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
}

RelatieKetenTyping = {
    'ID': 'uniqueidentifier',
    'AdministratieCode': 'int',
    'ClassificatieNaam': 'nvarchar',
    'Code': 'nvarchar',
    'Omschrijving': 'nvarchar'
}

BudgetTyping = {
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
}

GrootboekMappingTyping = {
    'ID': 'uniqueidentifier',
    'AdministratieCode': 'int',
    'GrootboekRubriekID': 'nvarchar',
    'GrootboekID': 'nvarchar',
    'GrootboekMappingSchema': 'nvarchar'
}

ReportingBalanceTyping = {
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
}

GrootboekMutatiesTyping = {
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
}

VoorraadTyping = {
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
}

ArtikelenTyping = {
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
}

ArtikelenExtraVeldenTyping = {
    'ArtikelID': 'nvarchar',
    'GewijzigdOp': 'date',
    'Nummer': 'int',
    'Omschrijving': 'nvarchar',
    'Waarde': 'nvarchar',
    'AdministratieCode': 'int',
}

ArtikelGroepenTyping = {
    'ID': 'uniqueidentifier',
    'Code': 'nvarchar',
    'Omschrijving': 'nvarchar',
    'AdministratieCode': 'int',
}

VerkoopfacturenTyping = {
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
}

VerkoopOrdersTyping = {
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
}

VerkoopkansenTyping = {
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
}


def convert_column_types(df, column_types):
    pd.set_option('future.no_silent_downcasting', True)
    
    for column, dtype in column_types.items():
        if column in df.columns:
            try:
                if dtype == 'uniqueidentifier':
                    # Zorg ervoor dat elke waarde geldig is als UUID
                    def convert_to_uuid(x):
                        if pd.notna(x):
                            try:
                                return str(uuid.UUID(x))  # Zorg ervoor dat het als string wordt behandeld
                            except ValueError:
                                raise ValueError(f"Ongeldige UUID waarde: {x}")
                        return None

                    df[column] = df[column].apply(convert_to_uuid)
                elif dtype == 'int':
                    # Zet niet-numerieke waarden om naar NaN
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                    invalid_values = df[column].isnull()
                    
                    # Specifieke ongeldige waarden printen
                    if invalid_values.any():
                        ongeldige_waarden = df[column][invalid_values].unique()
                        print(f"Waarschuwing: {len(ongeldige_waarden)} ongeldige waarden gevonden in kolom '{column}': {ongeldige_waarden}, deze worden vervangen door 0.")
                        df[column] = df[column].fillna(0)  # Vervang NaN door 0
                    
                    df[column] = df[column].astype(int)
                elif dtype == 'nvarchar':
                    df[column] = df[column].astype(str)
                elif dtype == 'decimal':
                    df[column] = df[column].apply(lambda x: Decimal(x) if pd.notna(x) else None)
                elif dtype == 'bit':
                    df[column] = df[column].astype(bool)
                elif dtype == 'date':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
                elif dtype == 'tinyint':
                    # Vul NaN/None met 0 voordat de conversie plaats vind
                    df[column] = df[column].fillna(0).infer_objects(copy=False).astype(np.uint8)
                else:
                    raise ValueError(f"Onbekend datatype '{dtype}' voor kolom '{column}'.")
            except ValueError as e:
                raise ValueError(f"Fout bij het omzetten van kolom '{column}' naar type '{dtype}': {e}")
        else:
            raise ValueError(f"Kolom '{column}' niet gevonden in DataFrame.")
    
    return df