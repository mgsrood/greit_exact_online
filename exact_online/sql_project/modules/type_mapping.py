import pandas as pd
import uuid
from decimal import Decimal

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
    'GrootboekRubriekID': 'nvarchar',
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
}


def convert_column_types(df, column_types):
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
                    df[column] = df[column].astype(int)
                elif dtype == 'nvarchar':
                    df[column] = df[column].astype(str)
                elif dtype == 'decimal':
                    df[column] = df[column].apply(lambda x: Decimal(x) if pd.notna(x) else None)
                elif dtype == 'bit':
                    df[column] = df[column].astype(bool)
                elif dtype == 'date':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
                else:
                    raise ValueError(f"Onbekend datatype '{dtype}' voor kolom '{column}'.")
            except ValueError as e:
                raise ValueError(f"Fout bij het omzetten van kolom '{column}' naar type '{dtype}': {e}")
        else:
            raise ValueError(f"Kolom '{column}' niet gevonden in DataFrame.")
    
    return df