from ex_modules.log import log
from decimal import Decimal
import pandas as pd
import numpy as np
import uuid

def add_environment_id(df, environment_id):
    if environment_id is None:
        return None
    
    else: 
        df['OmgevingID'] = environment_id
        return df

GrootboekRubriekTyping = {
   'OmgevingID': 'int',
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
}

GrootboekrekeningTyping = {
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
}

BudgetTyping = {
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
}

GrootboekMutatiesTyping = {
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
}

DivisionsTyping = {
    "OmgevingID": "int",
    "Administratie_Code": "int",
    "Administratie_Naam": "nvarchar",
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
                    df[column] = df[column].str.lower().map({'true': True, 'false': False, '1': True, '0': False})
                    df[column] = df[column].astype(bool)
                elif dtype == 'date':
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
                elif dtype == 'datetime':
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                    df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S')
                elif dtype == 'tinyint':
                    # Vul NaN/None met 0 voordat de conversie plaats vind
                    df[column] = df[column].fillna(0).infer_objects(copy=False).astype(np.uint8)
                else:
                    raise ValueError(f"Onbekend datatype '{dtype}' voor kolom '{column}'.")
            except ValueError as e:
                print("Fout bij het omzetten van kolom:", column, "naar type:", dtype)
                raise ValueError(f"Fout bij het omzetten van kolom '{column}' naar type '{dtype}': {e}")
        else:
            print("Kolom niet gevonden:", column)
            raise ValueError(f"Kolom '{column}' niet gevonden in DataFrame.")
    
    return df

def apply_type_conversion(df, finn_it_connection_string, klantnaam, script_id, script, table, omgeving_id):
    
    log(finn_it_connection_string, klantnaam, f"Start data type conversie", script_id, script, omgeving_id, table)
    
    # Column mapping
    column_dictionary = {
        "Grootboekrekening": GrootboekrekeningTyping,
        "GrootboekRubriek": GrootboekRubriekTyping,
        "GrootboekMutaties": GrootboekMutatiesTyping,
        "Budget": BudgetTyping,
        "Divisions": DivisionsTyping,
    }

    column_types = column_dictionary.get(table)

    if column_types is None:
        # Foutmelding log en print
        print(f"Geen data type mapping gevonden voor tabel: {table}")
        log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen data type mapping gevonden", script_id, script, omgeving_id, table)
        return None

    # Transform the DataFrame column types
    converted_df = convert_column_types(df, column_types)

    # Succes en start log
    log(finn_it_connection_string, klantnaam, f"Data type conversie succesvol", script_id, script, omgeving_id, table)
    
    return converted_df