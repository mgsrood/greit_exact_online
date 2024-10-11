import pyodbc
from dotenv import load_dotenv
import os
import time
import requests
import re
from datetime import datetime, timedelta
from modules.logging import logging
from modules.config import fetch_all_connection_strings, fetch_configurations, fetch_division_codes, fetch_table_configurations, fetch_script_id
from modules.tokens import get_new_tokens, save_refresh_token, save_access_token
from modules.database import connect_to_database, write_to_database, clear_table
from modules.type_mapping import convert_column_types, GrootboekMutatiesTyping, VerkoopfacturenTyping, VerkoopOrdersTyping
from modules.table_mapping import transform_columns, GrootboekMutaties, Verkoopfacturen, VerkoopOrders
from modules.data_transformation import append_invoice_lines, append_order_lines
import xml.etree.ElementTree as ET
import pandas as pd

def clean_xml_string(xml_string):
    """Remove invalid XML characters from the XML string."""
    # Remove invalid XML characters (e.g., control characters like `&#x1F;`)
    return re.sub(r'&#x[0-9A-Fa-f]+;', '', xml_string)

def get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel):
    full_url = f"{url}{endpoint}"
    data = []
    total_rows = 0 

    # Start logging
    print(f"Start GET Request voor tabel: {tabel} | {division_name} ({division_code})")
    logging(finn_it_connection_string, klantnaam, f"Start GET Request", script_id, script, division_code, tabel)
    logging(finn_it_connection_string, klantnaam, f"Ophalen configuratiegegevens", script_id, script, division_code, tabel)

    # Ophalen configuratie gegevens
    config_conn = connect_to_database(connection_string)

    if config_conn:
        cursor = config_conn.cursor()
        config_dict = None
        for attempt in range(3):
            try:
                config_dict = fetch_configurations(cursor)
                if config_dict:
                    break
            except Exception as e:
                time.sleep(5)

        if config_dict is None:
            # Fout logging
            print("Fout bij het ophalen van de configuratiegegevens.")
            logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het ophalen van de configuratiegegevens", script_id, script, division_code, tabel)
            return None
        else:
            # Succes logging
            logging(finn_it_connection_string, klantnaam, f"Configuratiegegevens succesvol opgehaald", script_id, script, division_code, tabel)
    else:
        # Foutmelding logging
        print(f"Fout bij het connecten met de database: {tabel} | {division_name} ({division_code}).")
        logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het connecten met de database", script_id, script, division_code, tabel)
        return None

    # Variabelen definiëren
    client_secret = config_dict['client_secret']
    client_id = config_dict['client_id']
    access_token = config_dict['access_token']
    refresh_token = config_dict['refresh_token']
    
    # Request loop
    while full_url:
        
        payload = {}
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Cookie': f'.AspNetCore.Antiforgery.cdV5uW_Ejgc=CfDJ8CTJVFXxHxJHqt315CGWnt6RmoANCHzuwWq9U7Hje9I3wCAI4LZuudgNgWB6dYyMvEmg32OtzGlkiXWwnahptcAkcALB6KJT_gEvyE6MVNsWYGaWCvjmIDAtTJaRIoAFFsgnc8-ZLrEq13YMkITaGlg; .AspNetCore.Culture=c%3Dnl%7Cuic%3Dnl; ARRAffinity=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ARRAffinitySameSite=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ASP.NET_SessionId=3esxcu3yqpsi31zurmgciobc; ExactOnlineClient=7LjlWUfX5lQbrehxxcOgynPtG4hwzKvhbdRrllvl9LzDBusePiI7bj3hvRnEiEC15zVDQrffSDkobPKR/bxTiiFmlkNu4odr6q55xN5eyQQd6dQzIORCNib+1rPe5uyf55F2LyZLmpDtrMt1lYIyYU+Y8+fr/PyNF2VDkL5wcpQ=; ExactServer{{2bd296c3-cdda-4a9b-9dec-0aad425078e1}}=Division={division_code}'
        }

        response = requests.request("GET", full_url, headers=headers, data=payload)

        # Controleer of de request succesvol was
        if response.status_code == 200:
            # Reinig de XML response van ongewenste tekens
            cleaned_response = clean_xml_string(response.text)
            
            # Parse de XML-response
            root = ET.fromstring(cleaned_response)

            # Namespace instellen
            namespace = {'ns': 'http://schemas.microsoft.com/ado/2007/08/dataservices'}

            # Check voor ArtikelenExtraVelden
            if tabel == 'ArtikelenExtraVelden':
                print("Start ArtikelenExtraVelden")
                # Loop door elk <element> en haal de data op
                for entry in root.findall('.//ns:element', namespace):
                    item_data = {
                        'ItemID': entry.find('ns:ItemID', namespace).text.strip() if entry.find('ns:ItemID', namespace) is not None else None,
                        'Modified': entry.find('ns:Modified', namespace).text.strip() if entry.find('ns:Modified', namespace) is not None else None,
                        'Number': entry.find('ns:Number', namespace).text.strip() if entry.find('ns:Number', namespace) is not None else None,
                        'Description': entry.find('ns:Description', namespace).text.strip() if entry.find('ns:Description', namespace) is not None else None,
                        'Value': entry.find('ns:Value', namespace).text.strip() if entry.find('ns:Value', namespace) is not None else None
                    }
                    data.append(item_data)
                    total_rows += 1                

                # Kijk of er een volgende pagina is
                next_link = root.find('.//{http://www.w3.org/2005/Atom}link[@rel="next"]')
                full_url = next_link.attrib['href'] if next_link is not None else None

                # Wacht 1 seconde voordat je de volgende request doet
                print(f"Totaal aantal opgehaalde rijen: {total_rows}")
                time.sleep(1)

            else:
                # Loop door elk <entry> element en haal de data op
                for entry in root.findall('.//{http://www.w3.org/2005/Atom}entry'):
                    item_data = {}
                    for prop in entry.findall('.//{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}properties/*'):
                        if prop.text is not None:
                            item_data[prop.tag.replace('{http://schemas.microsoft.com/ado/2007/08/dataservices}', '')] = prop.text.strip()
                        else:
                            item_data[prop.tag.replace('{http://schemas.microsoft.com/ado/2007/08/dataservices}', '')] = None
                    data.append(item_data)
                    total_rows += 1

                # Kijk of er een volgende pagina is
                next_link = root.find('.//{http://www.w3.org/2005/Atom}link[@rel="next"]')
                full_url = next_link.attrib['href'] if next_link is not None else None

                # Wacht 1 seconde voordat je de volgende request doet
                print(f"Opgehaalde rijen tot nu toe: {total_rows}")
                time.sleep(1)
            
        else:
            if response.status_code == 401:
                print(f"Fout bij het ophalen van gegevens: {response.status_code} - {response.text}")
                # Logging
                logging(finn_it_connection_string, klantnaam, f"Ophalen nieuwe access- en refresh token", script_id, script, division_code, tabel)
                
                # Ophalen nieuwe tokens
                oude_refresh_token = refresh_token
                client_id = client_id
                client_secret = client_secret
                new_access_token, new_refresh_token = get_new_tokens(oude_refresh_token, client_id, client_secret, finn_it_connection_string, klantnaam)

                if new_refresh_token:
                    save_refresh_token(connection_string, new_refresh_token)
                    refresh_token = new_refresh_token

                    # Succes logging
                    logging(finn_it_connection_string, klantnaam, f"Nieuwe refresh token successvol opgehaald en opgeslagen", script_id, script, division_code, tabel)

                else:
                    # Foutmelding logging
                    print(f"FOUTMELDING | Nieuwe refresh token niet kunnen ophalen voor {klantnaam}")
                    logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Nieuwe refresh token niet kunnen ophalen", script_id, script, division_code, tabel)
                    return None

                # Nu kun je de nieuwe access en refresh tokens gebruiken voor je verzoeken
                if new_access_token and new_refresh_token:
                    # Voer hier je verzoek uit met de nieuwe access token
                    save_access_token(connection_string, new_access_token)
                    access_token = new_access_token

                    # Succes logging
                    print(f"Nieuwe access token successvol opgehaald en opgeslagen")
                    logging(finn_it_connection_string, klantnaam, f"Nieuwe access token successvol opgehaald en opgeslagen", script_id, script, division_code, tabel)
                
                else:
                    # Foutmelding logging
                    logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Nieuwe access token niet kunnen ophalen", script_id, script, division_code, tabel)
                    return None
            
            else:
                print(f"Error {response.status_code}: {response.text}")
                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Exact Online API foutmelding | Status code: {response.status_code}", script_id, script, division_code, tabel)
                return None

    # Maak een DataFrame van de verzamelde gegevens
    if data:
        df = pd.DataFrame(data)

        # Succes logging
        logging(finn_it_connection_string, klantnaam, f"Data succesvol opgehaald", script_id, script, division_code, tabel)

        return df
    
    else:
        df = pd.DataFrame()
        # Informatie logging
        print(f"Geen data opgehaald voor: {tabel} | {division_name} ({division_code})")
        logging(finn_it_connection_string, klantnaam, f"Geen data opgehaald", script_id, script, division_code, tabel)

        return df

if __name__ == "__main__":

    # Definiëren van script
    script = "Volledig"

    # Definiëren van start tijd en datum
    start_time = time.time()
    last_year = datetime.now() - timedelta(days=365)
    start_date = last_year.replace(day=1, month=1, hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Laad de omgevingsvariabelen uit het .env-bestand
    load_dotenv()

    # Verbindingsinstellingen
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 17 for SQL Server}'

    # Aantal retries instellen
    max_retries = 3
    retry_delay = 5

    # Verbindingsstring
    finn_it_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

    # ScriptID ophalen
    database_conn = connect_to_database(finn_it_connection_string)
    if database_conn:
        cursor = database_conn.cursor()
        latest_script_id = fetch_script_id(cursor)
        database_conn.close()

        if latest_script_id:
            script_id = latest_script_id + 1
        else:
            script_id = 1

    # Start logging
    klantnaam = 'Finn It'
    logging(finn_it_connection_string, klantnaam, f"Script gestart", script_id, script)

    # Verbinding maken met database
    database_conn = connect_to_database(finn_it_connection_string)
    if database_conn:
        cursor = database_conn.cursor()
        connection_dict = None
        for attempt in range(max_retries):
            try:
                connection_dict = fetch_all_connection_strings(cursor)
                if connection_dict:
                    break
            except Exception as e:
                time.sleep(retry_delay)
        database_conn.close()
        if connection_dict:
            # Start logging
            logging(finn_it_connection_string, klantnaam, f"Ophalen connectiestrings gestart", script_id, script)
        else:
            # Foutmelding logging
            print(f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen")
            logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen", script_id, script)
    else:
        # Foutmelding logging
        print(f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen")
        logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen", script_id, script)

    # Klant loop
    for klantnaam, (connection_string, type) in connection_dict.items():

        # Skip de klant als type niet gelijk is aan 1
        if type != 1:
            continue

        # Start logging
        logging(finn_it_connection_string, klantnaam, f"Ophalen divisiecodes gestart", script_id, script)
        
        # Ophalen tabel configuratie gegevens
        config_conn = connect_to_database(connection_string)
        if config_conn:    
            cursor = config_conn.cursor()
            table_config_dict = None
            for attempt in range(max_retries):
                try:
                    table_config_dict = fetch_table_configurations(cursor)
                    if table_config_dict:
                        break
                except Exception as e:
                    time.sleep(retry_delay)
        else:
            # Foutmelding logging
            print(f"FOUTMELDING | Verbinding met configuratie database mislukt voor: {klantnaam}")
            logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Verbinding met configuratie database mislukt", script_id, script)
            continue

        # Ophalen divisiecodes
        division_conn = connect_to_database(connection_string)
        if division_conn:
            cursor = division_conn.cursor()
            division_dict = None
            for attempt in range(max_retries):
                try:
                    division_dict = fetch_division_codes(cursor)
                    if division_dict:
                        break
                except Exception as e:
                    time.sleep(retry_delay)
            division_conn.close()
            # Succes logging
            logging(finn_it_connection_string, klantnaam, f"Ophalen divisiecodes gelukt", script_id, script)
        else:
            # Foutmelding logging
            print(f"FOUTMELDING | Verbinding met divisie database mislukt voor: {klantnaam}")
            logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Verbinding met divisie database mislukt", script_id, script)
            continue

        # Verbinding maken per divisie code
        for division_name, division_code in division_dict.items():
            # Start logging en print
            print(f"Begin GET Requests voor divisie: {division_name} ({division_code}) | {klantnaam}")
            logging(finn_it_connection_string, klantnaam, f"Start GET Requests voor nieuwe divisiecode", script_id, script, division_code)

            # Base URL
            url = f"https://start.exactonline.nl/api/v1/{division_code}/"
                    
            # Endpoints
            endpoints = {
                "Verkoopfacturen": f"bulk/SalesInvoice/SalesInvoices?$filter=InvoiceDate ge datetime'{start_date}'&$select=Currency,DeliverTo,Description,Division,InvoiceDate,InvoiceID,InvoiceNumber,InvoiceTo,OrderDate,OrderedBy,PaymentConditionDescription,Remarks,ShippingMethodDescription,StatusDescription,YourRef,StarterSalesInvoiceStatusDescription,SalesInvoiceLines/AmountDC,SalesInvoiceLines/CostCenterDescription,SalesInvoiceLines/CostUnitDescription,SalesInvoiceLines/DeliveryDate,SalesInvoiceLines/Description,SalesInvoiceLines/Description,SalesInvoiceLines/Discount,SalesInvoiceLines/EmployeeFullName,SalesInvoiceLines/GLAccount,SalesInvoiceLines/ID,SalesInvoiceLines/Item,SalesInvoiceLines/LineNumber,SalesInvoiceLines/Quantity,SalesInvoiceLines/SalesOrder,SalesInvoiceLines/SalesOrderLine,SalesInvoiceLines/UnitDescription,SalesInvoiceLines/UnitPrice,SalesInvoiceLines/VATAmountDC,SalesInvoiceLines/VATPercentage&$expand=SalesInvoiceLines",
                "VerkoopOrders": f"bulk/SalesOrder/SalesOrders?$filter=OrderDate ge datetime'{start_date}'&$select=ApprovalStatusDescription,Approved,ApproverFullName,Created,CreatorFullName,Currency,DeliverTo,Description,Division,InvoiceStatusDescription,InvoiceTo,OrderDate,OrderedBy,OrderID,OrderNumber,Remarks,ShippingMethodDescription,StatusDescription,YourRef,SalesOrderLines/AmountDC,SalesOrderLines/CostCenterDescription,SalesOrderLines/CostPriceFC,SalesOrderLines/CostUnitDescription,SalesOrderLines/DeliveryDate,SalesOrderLines/Description,SalesOrderLines/Discount,SalesOrderLines/ID,SalesOrderLines/Item,SalesOrderLines/LineNumber,SalesOrderLines/Quantity,SalesOrderLines/VATAmount,SalesOrderLines/VATPercentage&$expand=SalesOrderLines",
                "GrootboekMutaties": f"bulk/Financial/TransactionLines?$filter=FinancialYear ge {start_date[:4]}&$select=ID,Account,AmountDC,AmountVATFC,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,Currency,Date,Description,Division,EntryNumber,FinancialPeriod,FinancialYear,GLAccount,InvoiceNumber,OrderNumber,PaymentReference,VATPercentage,Type",
            }

            # Endpoint loop
            for tabel, endpoint in endpoints.items():
                for table, status in table_config_dict.items():
                    if table == tabel:
                        if status == 0:
                            print(f"Overslaan van GET Requests voor endpoint: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                            logging(finn_it_connection_string, klantnaam, f"Overslaan van GET Requests", script_id, script, division_code, tabel)
                            continue
                        else:                            
                            # Uitvoeren GET Request
                            df = get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel)

                            if df is None:
                                # Foutmelding logging
                                print(f"FOUTMELDING | Fout bij het ophalen van data voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het ophalen van data", script_id, script, division_code, tabel)
                                errors_occurred = True
                                break

                            elif df.empty:
                                # Geen data opgehaald, maar geen error
                                print(f"Geen data opgehaald voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                                logging(finn_it_connection_string, klantnaam, f"Geen data opgehaald", script_id, script, division_code, tabel)
                                continue

                            else:
                                # Succes logging
                                logging(finn_it_connection_string, klantnaam, f"Ophalen DataFrame gelukt", script_id, script, division_code, tabel)

                            # Data transformatie Verkoopfacturen
                            if tabel == "Verkoopfacturen":
                                # Start logging
                                logging(finn_it_connection_string, klantnaam, f"Start van data transformatie", script_id, script, division_code, tabel)

                                # Voer data transformatie uit
                                try:
                                    df = append_invoice_lines(df)

                                    print("Length of full Verkoopfacturen DataFrame: ", len(df))

                                    # Succes logging met totale lengte van dataframe erin verwerkt als len(df)
                                    logging(finn_it_connection_string, klantnaam, f"Data transformatie gelukt: volledige lengte DataFrame {len(df)} rijen", script_id, script, division_code, tabel)

                                except Exception as e:
                                    # Foutmelding logging
                                    print(f"FOUTMELDING | Fout bij het transformeren van verkoopfacturen | {division_name} ({division_code}) | {klantnaam}")
                                    logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het transformeren: {e}", script_id, script, division_code, tabel)
                                    print(e)

                            # Data transformatie VerkoopOrders
                            if tabel == "VerkoopOrders":
                                # Start logging
                                logging(finn_it_connection_string, klantnaam, f"Start van data transformatie", script_id, script, division_code, tabel)

                                # Voer data transformatie uit
                                try:
                                    df = append_order_lines(df)

                                    # Succes logging
                                    logging(finn_it_connection_string, klantnaam, f"Data transformatie gelukt", script_id, script, division_code, tabel)

                                except Exception as e:
                                    # Foutmelding logging
                                    print(f"FOUTMELDING | Fout bij het transformeren van verkooporders | {division_name} ({division_code}) | {klantnaam}")
                                    logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het transformeren: {e}", script_id, script, division_code, tabel)
                                    print(e)

                            # Start logging
                            logging(finn_it_connection_string, klantnaam, f"Start data mapping", script_id, script, division_code, tabel)

                            # Tabel mapping
                            mapping_dict = {
                                "GrootboekMutaties": GrootboekMutaties,
                                "Verkoopfacturen": Verkoopfacturen,
                                "VerkoopOrders": VerkoopOrders,
                            }

                            column_mapping = mapping_dict.get(tabel)
                            if column_mapping is None:
                                # Foutmelding logging en print
                                print(f"Geen kolom mapping gevonden voor tabel: {tabel}")
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen mapping gevonden", script_id, script, division_code, tabel)
                                continue

                            # Transform the DataFrame columns
                            df_transformed = transform_columns(df, column_mapping, division_code)

                            # Succes en start logging
                            logging(finn_it_connection_string, klantnaam, f"Data mapping succesvol", script_id, script, division_code, tabel)
                            logging(finn_it_connection_string, klantnaam, f"Start data type conversie", script_id, script, division_code, tabel)

                            # Column mapping
                            column_dictionary = {
                                "GrootboekMutaties": GrootboekMutatiesTyping,
                                "Verkoopfacturen": VerkoopfacturenTyping,
                                "VerkoopOrders": VerkoopOrdersTyping,
                            }
            
                            column_types = column_dictionary.get(tabel)
                            if column_types is None:
                                # Foutmelding logging en print
                                print(f"Geen data type mapping gevonden voor tabel: {tabel}")
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen data type mapping", script_id, script, division_code, tabel)
                                continue

                            # Transform the DataFrame column types
                            df_transformed = convert_column_types(df_transformed, column_types)

                            # Succes en start logging
                            logging(finn_it_connection_string, klantnaam, f"Data type conversie succesvol", script_id, script, division_code, tabel)
                            logging(finn_it_connection_string, klantnaam, f"Start mogelijk verwijderen rijen of complete tabel", script_id, script, division_code, tabel)

                            # Table modes for deleting rows or complete table
                            table_modes = {
                                "GrootboekMutaties": "mutaties",
                                "Verkoopfacturen": "facturen",
                                "VerkoopOrders": "orders",
                            }

                            table_mode = table_modes.get(tabel)
                            
                            if table_mode is None:
                                # Foutmelding logging en print
                                print(f"Geen actie gevonden voor tabel: {tabel}")
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen actie gevonden", script, division_code, tabel)
                                continue

                            # Clear the table
                            actie, rows_deleted = clear_table(connection_string, tabel, table_mode, start_date, division_code)

                            # Succes en start logging
                            logging(finn_it_connection_string, klantnaam, f"Totaal verwijderde rijen {rows_deleted}", script_id, script, division_code, tabel)
                            logging(finn_it_connection_string, klantnaam, f"Start toevoegen rijen naar database", script_id, script, division_code, tabel)

                            # Unieke kolom per tabel
                            unique_columns = {
                                "GrootboekMutaties": "ID",
                                "Verkoopfacturen": "FR_FactuurregelID",
                                "VerkoopOrders": "OR_OrderRegelID",
                            }
                            
                            # Unieke kolom ophalen voor de specifieke tabel
                            unique_column = unique_columns.get(tabel)
                            if unique_column is None:
                                # Foutmelding logging en print
                                print(f"Geen unieke kolom gevonden voor tabel: {tabel}")
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen unieke kolom gevonden", script_id, script, division_code, tabel)
                                continue

                            # Administratie kolom per tabel
                            administration_columns = {
                                "GrootboekMutaties": "AdministratieCode",
                                "Verkoopfacturen": "F_AdministratieCode",
                                "VerkoopOrders": "O_AdministratieCode",
                            }

                            # Administratie kolom ophalen voor de specifieke tabel
                            administration_column = administration_columns.get(tabel)
                            if administration_column is None:
                                # Foutmelding logging en print
                                print(f"Geen administratie kolom gevonden voor tabel: {tabel}")
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen administratie kolom gevonden", script_id, script, division_code, tabel)
                                continue

                            # Schrijf de DataFrame naar de database
                            try:
                                write_to_database(df_transformed, tabel, connection_string)
                                
                                # Succeslogging bij succes
                                print(f"Succesvol {len(df_transformed)} rijen toegevoegd aan de database voor tabel: {tabel}")
                                logging(finn_it_connection_string, klantnaam, f"Succesvol {len(df_transformed)} rijen toegevoegd aan de database", script_id, script, division_code, tabel)
                                    
                            except Exception as e:
                                # Foutmelding logging en print
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het toevoegen naar database | Foutmelding: {str(e)}", script_id, script, division_code, tabel)
                                print(f"Fout bij het toevoegen naar database: {e}")
                                errors_occurred = True

            # Succes logging
            logging(finn_it_connection_string, klantnaam, f"GET Requests succesvol afgerond", script_id, script, division_code, tabel)

        # Succes en start logging
        print(f"Sync succesvol afgerond voor klant: {klantnaam}")
        logging(finn_it_connection_string, klantnaam, f"Sync succesvol afgerond", script_id, script)

        # Succes logging
        print(f"Endpoints succesvol afgerond voor klant: {klantnaam}")
        logging(finn_it_connection_string, klantnaam, f"Script succesvol afgerond", script_id, script)

    # Totale tijdsduur script
    end_time = time.time()
    total_duration = end_time - start_time  # Tijd in seconden

    # Omzetten naar HH:MM:SS
    hours, remainder = divmod(total_duration, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_duration = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

    # Succes logging
    print(f"Script volledig afgerond in {formatted_duration}")
    logging(finn_it_connection_string, "Finn It", f"Script volledig afgerond in {formatted_duration}", script_id, script)