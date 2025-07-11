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
    'Intercompany': 'bit',
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

MedewerkersTyping = {
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
}

ProjectenTyping = {
    "OmgevingID": "int",
    "Administratie_Code": "int",
    "ProjectID": "int",
    "Project_Code": "int",
    "Project": "nvarchar",
    "RelatieID": "int",
    "Projectleider_GUID": "uniqueidentifier",
    "Project_Type": "nvarchar",
    "Project_Classificatie": "nvarchar",
    "Start_Datum": "date",
    "Eind_Datum": "date",
}

RelatiesTyping = {
    "OmgevingID": "int",
    "Soort_Contact": "nvarchar",
    "Naam": "nvarchar",
    "Inkooprelatie": "bit",
    "Verkooprelatie": "bit",
    "Medewerker": "bit",
    "Medewerker_GUID": "uniqueidentifier",
    "VerkooprelatieID": "int",
    "InkooprelatieID": "int",
}

UrenregistratieTyping = {
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
}

VerlofTyping = {
    "OmgevingID": "int",
    "Administratie_Code": "int",
    "Medewerker_GUID": "uniqueidentifier",
    "Verlof": "nvarchar",
    "Start_Datum": "date",
    "Eind_Datum": "date",
    "Verlof_Uren": "decimal",
    "Verlof_Type": "nvarchar",
    "AfdelingID": "nvarchar",
}

VerzuimUrenTyping = {
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
}
    
VerzuimVerloopTyping = {
    "OmgevingID": "int",
    "Verzuimmelding_GUID": "uniqueidentifier",
    "Verzuimverloop_GUID": "uniqueidentifier",
    "Medewerker_GUID": "uniqueidentifier",
    "Gewijzigd_Op": "datetime",
    "Datum": "datetime",
    "Vangnet_Regeling": "bit",
    "Aanwezigheid_Percentage": "decimal",
}

ContractenTyping = {
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
                    # Controleer of de kolom stringwaarden bevat
                    if df[column].dtype == 'object':
                        df[column] = df[column].str.lower().map({'true': True, ''false'': False, '1': True, '0': 'false'})
                    # Vul NaN-waarden in met None (voor SQL NULL)
                    df[column] = df[column].where(pd.notnull(df[column]), None)
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
        "Medewerkers": MedewerkersTyping,
        "Projecten": ProjectenTyping,
        "Relaties": RelatiesTyping,
        "Urenregistratie": UrenregistratieTyping,
        "Verlof": VerlofTyping,
        "VerzuimVerloop": VerzuimVerloopTyping,
        "VerzuimUren": VerzuimUrenTyping,
        "Contracten": ContractenTyping,
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