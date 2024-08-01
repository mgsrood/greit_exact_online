CrediteurenOpenstaand = {
    'ID': 'HID',
    'RelatieID': 'AccountId',
    'Valuta': 'CurrencyCode',
    'Bedrag': 'Amount',
    'Omschrijving': 'Description',
    'BoekstukNummer': 'EntryNumber',
    'FactuurNummer': 'InvoiceNumber',
    'Referentie': 'YourRef',
    'FactuurDatum': 'InvoiceDate',
    'VervalDatum': 'DueDate',
    }

DebiteurenOpenstaand = {
    'ID': 'HID',
    'RelatieID': 'AccountId',
    'Valuta': 'CurrencyCode',
    'Bedrag': 'Amount',
    'Omschrijving': 'Description',
    'BoekstukNummer': 'EntryNumber',
    'FactuurNummer': 'InvoiceNumber',
    'Referentie': 'YourRef',
    'FactuurDatum': 'InvoiceDate',
    'VervalDatum': 'DueDate',
    }

def transform_columns(df, column_mapping, division_code):
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
    
    # Controleer of 'AdministratieCode' bestaat in de DataFrame
    if 'AdministratieCode' not in df.columns:
        # Voeg de kolom 'AdministratieCode' toe met division_code als waarde
        df['AdministratieCode'] = division_code

    return df