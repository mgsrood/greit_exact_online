import pandas as pd

CrediteurenOpenstaand = {
    'HID': 'ID',
    'Division': 'AdministratieCode',
    'AccountId': 'RelatieID',
    'CurrencyCode': 'Valuta',
    'Amount': 'Bedrag',
    'Description': 'Omschrijving',
    'EntryNumber': 'BoekstukNummer',
    'InvoiceNumber': 'FactuurNummer',
    'YourRef': 'Referentie',
    'InvoiceDate': 'FactuurDatum',
    'DueDate': 'VervalDatum',
}

DebiteurenOpenstaand = {
    'HID': 'ID',
    'Division': 'AdministratieCode',
    'AccountId': 'RelatieID',
    'CurrencyCode': 'Valuta',
    'Amount': 'Bedrag',
    'Description': 'Omschrijving',
    'EntryNumber': 'BoekstukNummer',
    'InvoiceNumber': 'FactuurNummer',
    'YourRef': 'Referentie',
    'InvoiceDate': 'FactuurDatum',
    'DueDate': 'VervalDatum',
}

GrootboekMutaties = {
    'ID': 'ID',
    'Division': 'AdministratieCode',
    'GLAccount': 'GrootboekID',
    'Account': 'RelatieID',
    'Date': 'Datum',
    'FinancialYear': 'Boekjaar',
    'FinancialPeriod': 'BoekPeriode',
    'Description': 'Omschrijving',
    'EntryNumber': 'BoekstukNummer',
    'InvoiceNumber': 'FactuurNummer',
    'OrderNumber': 'OrderNummer',
    'PaymentReference': 'Referentie',
    'Currency': 'Valuta',
    'AmountDC': 'Bedrag',
    'AmountVATFC': 'BTWBedrag',
    'VATPercentage': 'BTWPercentage'
}

GrootboekRubriek = {
    'ID': 'ID',
    'Division': 'AdministratieCode',
    'Code': 'RubriekCode',
    'Description': 'Naam',
    'Parent': 'ParentID',
    'TaxonomyNamespaceDescription': 'GrootboekSchema'
}

Grootboekrekening = {
    'ID': 'ID',
    'Division': 'AdministratieCode',
    'Code': 'Grootboekrekeningen',
    'Description': 'Grootboek',
    'BalanceType': 'BalansType',
    'BalanceSide': 'DebetCredit',
    'Type': 'GrootboekIndelingID',
    'TypeDescription': 'GrootboekIndeling',
    'Costcenter': 'KostenplaatsID',
    'CostcenterDescription': 'Kostenplaats',
    'Costunit': 'KostenDragerID',
    'CostunitDescription': 'KostenDrager'
}

Relaties = {
    'ID': 'ID',
    'Division': 'AdministratieCode',
    'Code': 'RelatieCode',
    'CustomerSince': 'KlantSinds',
    'Name': 'KlantNaam',
    'AddressLine1': 'Adres',
    'Postcode': 'Postcode',
    'City': 'Plaats',
    'StateName': 'Provincie',
    'CountryName': 'Land',
    'AccountManagerFullName': 'Relatiebeheerder',
    'BusinessType': 'BusinessType',
    'ActivitySector': 'BusinessSector',
    'ActivitySubSector': 'BusinessSubSector',
    'Classification': 'RelatieGroep',
    'Classification1': 'RelatieGroepID1',
    'Classification2': 'RelatieGroepID2',
    'Classification3': 'RelatieGroepID3',
    'Classification4': 'RelatieGroepID4',
    'Classification5': 'RelatieGroepID5',
    'Classification6': 'RelatieGroepID6',
    'Classification7': 'RelatieGroepID7',
    'Classification8': 'RelatieGroepID8',
    'ClassificationDescription': 'RelatieGroepOmschrijving',
    'Parent': 'Parent',
    'IsPurchase': 'IsSupplier',
    'IsSupplier': 'IsPurchase',
    'Status': 'Status'
}

RelatieKeten = {
    'ID': 'ID',
    'Division': 'AdministratieCode',
    'AccountClassificationNameDescription': 'ClassificatieNaam',
    'Code': 'Code',
    'Description': 'Omschrijving'
}

Budget = {
    'ID': 'ID',
    'AmountDC': 'Bedrag',
    'BudgetScenarioDescription': 'BudgetScenario',
    'CostcenterDescription': 'Kostenplaats',
    'CostunitDescription': 'Kostendrager',
    'Division': 'AdministratieCode',
    'GLAccount': 'GrootboekID',
    'ItemDescription': 'Artikel',
    'ReportingPeriod': 'Boekperiode',
    'ReportingYear': 'Boekjaar'
}

GrootboekMapping = {
    'ID': 'ID',
    'Classification': 'GrootboekRubriekID',
    'Division': 'AdministratieCode',
    'GLAccount': 'GrootboekID',
    'GLSchemeDescription': 'GrootboekMappingSchema'
}

ReportingBalance = {
    'ID': 'ID',
    'Amount': 'Bedrag',
    'AmountCredit': 'BedragCredit',
    'AmountDebit': 'BedragDebit',
    'BalanceType': 'BalansType',
    'CostCenterDescription': 'KostenPlaats',
    'CostUnitDescription': 'KostenDrager',
    'Count': 'AantalMutaties',
    'Division': 'AdministratieCode',
    'GLAccount': 'GrootboekRubriekID',
    'ReportingPeriod': 'ReportingPeriod',
    'ReportingYear': 'ReportingYear'
}

def transform_columns(df, column_mapping, division_code):
    # Controleer of de DataFrame leeg is
    if df.empty:
        
        # Controleer of 'Division' bestaat in de DataFrame
        if 'Division' not in df.columns:
            # Voeg de kolom 'Division' toe met division_code als waarde
            df['Division'] = division_code
        
            # Hernoem de kolommen
            df = df.rename(columns=column_mapping)

            # Retourneer een lege DataFrame met de juiste kolommen
            return pd.DataFrame(columns=list(column_mapping.values()))
    
    # Controleer of 'Division' bestaat in de DataFrame
    if 'Division' not in df.columns:
        # Voeg de kolom 'Division' toe met division_code als waarde
        df['Division'] = division_code

    # Hernoem de kolommen
    df = df.rename(columns=column_mapping)
    
    # Zorg ervoor dat de DataFrame kolommen overeenkomen met de nieuwe volgorde
    new_columns = list(column_mapping.values())
    # Check of alle nieuwe kolommen aanwezig zijn in de DataFrame
    missing_columns = [col for col in new_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"De volgende kolommen ontbreken in de DataFrame: {', '.join(missing_columns)}")
    
    # Herorden de DataFrame kolommen
    df = df[new_columns]

    return df