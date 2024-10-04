import pyodbc
from dotenv import load_dotenv
import os
import time
import requests
import re
from datetime import datetime, timedelta
from modules.logging import logging
from modules.config import fetch_all_connection_strings, fetch_configurations, fetch_division_codes, save_laatste_sync, save_reporting_year, fetch_table_configurations
from modules.tokens import get_new_tokens, save_refresh_token, save_access_token
from modules.database import connect_to_database, write_to_database, clear_table
from modules.type_mapping import convert_column_types, CrediteurenOpenstaandTyping, DebiteurenOpenstaandTyping, GrootboekRubriekTyping, GrootboekrekeningTyping, RelatiesTyping, RelatieKetenTyping, BudgetTyping, GrootboekMappingTyping, ReportingBalanceTyping, GrootboekMutatiesTyping, VoorraadTyping, ArtikelenTyping, ArtikelenExtraVeldenTyping, ArtikelGroepenTyping, VerkoopfacturenTyping, VerkoopkansenTyping, VerkoopOrdersTyping
from modules.table_mapping import transform_columns, CrediteurenOpenstaand, DebiteurenOpenstaand, GrootboekMutaties, GrootboekRubriek, Grootboekrekening, Relaties, RelatieKeten, Budget, GrootboekMapping, ReportingBalance, Voorraad, Artikelen, ArtikelenExtraVelden, ArtikelGroepen, Verkoopfacturen, VerkoopOrders, Verkoopkansen
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
    logging(finn_it_connection_string, klantnaam, f"Start GET Request voor tabel: {tabel} | {division_name} ({division_code})")
    logging(finn_it_connection_string, klantnaam, f"Ophalen configuratiegegevens voor tabel: {tabel} | {division_name} ({division_code})")

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
            logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het ophalen van de configuratiegegevens: {tabel} | {division_name} ({division_code})")
            return None
        else:
            # Succes logging
            logging(finn_it_connection_string, klantnaam, f"Configuratiegegevens succesvol opgehaald voor: {klantnaam}")
    else:
        # Foutmelding logging
        print(f"Fout bij het connecten met de database: {tabel} | {division_name} ({division_code}).")
        logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het connecten met de database: {tabel} | {division_name} ({division_code})")
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
                logging(finn_it_connection_string, klantnaam, f"Ophalen nieuwe access- en refresh token | {tabel} | {division_name} ({division_code})")
                
                # Ophalen nieuwe tokens
                oude_refresh_token = refresh_token
                client_id = client_id
                client_secret = client_secret
                new_access_token, new_refresh_token = get_new_tokens(oude_refresh_token, client_id, client_secret, finn_it_connection_string, klantnaam)

                if new_refresh_token:
                    save_refresh_token(connection_string, new_refresh_token)
                    refresh_token = new_refresh_token

                    # Succes logging
                    logging(finn_it_connection_string, klantnaam, f"Nieuwe refresh token successvol opgehaald en opgeslagen voor: {tabel} | {division_name} ({division_code})")

                else:
                    # Foutmelding logging
                    print(f"FOUTMELDING | Nieuwe refresh token niet kunnen ophalen voor: {tabel} | {division_name} ({division_code})")
                    logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Nieuwe refresh token niet kunnen ophalen voor: {tabel} | {division_name} ({division_code})")
                    return None

                # Nu kun je de nieuwe access en refresh tokens gebruiken voor je verzoeken
                if new_access_token and new_refresh_token:
                    # Voer hier je verzoek uit met de nieuwe access token
                    save_access_token(connection_string, new_access_token)
                    access_token = new_access_token

                    # Succes logging
                    print(f"Nieuwe refresh- en access token successvol opgehaald en opgeslagen voor: {tabel} | {division_name} ({division_code})")
                    logging(finn_it_connection_string, klantnaam, f"Nieuwe access token successvol opgehaald en opgeslagen voor: {tabel} | {division_name} ({division_code})")
                
                else:
                    # Foutmelding logging
                    logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Nieuwe access token niet kunnen ophalen voor: {tabel} | {division_name} ({division_code})")
                    return None
            
            else:
                print(f"Error {response.status_code}: {response.text}")
                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Exact Online API foutmelding voor: {tabel} | {division_name} ({division_code}) - Status code: {response.status_code}")
                return None

    # Maak een DataFrame van de verzamelde gegevens
    if data:
        df = pd.DataFrame(data)

        # Succes logging
        logging(finn_it_connection_string, klantnaam, f"Data succesvol opgehaald voor: {tabel} | {division_name} ({division_code})")

        return df
    
    else:
        df = pd.DataFrame()
        # Informatie logging
        print(f"Geen data opgehaald voor: {tabel} | {division_name} ({division_code})")
        logging(finn_it_connection_string, klantnaam, f"Geen data opgehaald voor: {tabel} | {division_name} ({division_code})")

        return df

if __name__ == "__main__":

    # Leg de starttijd vast
    start_time = time.time()

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

    # Start logging
    klantnaam = 'Finn It'
    logging(finn_it_connection_string, klantnaam, f"Script gestart")

    # Verbinding maken met database
    database_conn = connect_to_database(finn_it_connection_string)

    if database_conn:
        cursor = database_conn.cursor()

        # Ophalen connection_dict met retries
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
            logging(finn_it_connection_string, klantnaam, f"Ophalen connectiestrings gestart")
        else:
            # Foutmelding logging
            print(f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen")
            logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Ophalen connectiestrings mislukt na meerdere pogingen")
    else:
        # Foutmelding logging
        print(f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen")
        logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Verbinding met database mislukt na meerdere pogingen")

    # Klant loop
    for klantnaam, (connection_string, type) in connection_dict.items():
        # Skip de klant als type niet gelijk is aan 1
        if type != 1:
            continue # Ga naar de volgende klant als type niet gelijk is aan 1

        # Error tracking
        errors_occurred = False

        # Starttijd voor klant
        nieuwe_laatste_sync = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Start logging
        logging(finn_it_connection_string, klantnaam, f"Ophalen configuratie gegevens gestart voor: {klantnaam}")
        
        # Ophalen configuratie gegevens
        config_conn = connect_to_database(connection_string)

        if config_conn:
            cursor = config_conn.cursor()
            config_dict = None
            for attempt in range(max_retries):
                try:
                    config_dict = fetch_configurations(cursor)
                    if config_dict:
                        break
                except Exception as e:
                    time.sleep(retry_delay)

            if config_dict:
                laatste_sync = config_dict['Laatste_sync']
                print(laatste_sync)
                # Schuif laatste_sync 5 dagen terug
                if laatste_sync:
                    laatste_sync = (datetime.strptime(laatste_sync, "%Y-%m-%dT%H:%M:%S") - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%S")
                    print(laatste_sync)
                reporting_year = config_dict['ReportingYear']
                config_conn.close()
        
                # Succes en start logging
                logging(finn_it_connection_string, klantnaam, f"Ophalen configuratie gegevens gelukt voor: {klantnaam}")
                logging(finn_it_connection_string, klantnaam, f"Ophalen divisiecodes gestart voor: {klantnaam}")
            else:
                # Foutmelding logging
                print(f"FOUTMELDING | Ophalen configuratie gegevens mislukt voor: {klantnaam}")
                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Ophalen configuratie gegevens mislukt voor: {klantnaam}")
                continue
        
        # Ophalen tabel configuratie gegevens
        config_conn = connect_to_database(connection_string)

        if config_conn:    
            cursor = config_conn.cursor()
            table_config_dict = None
            for attempt in range(max_retries):
                try:
                    table_config_dict = fetch_table_configurations(cursor)
                    print(table_config_dict)
                    if table_config_dict:
                        break
                except Exception as e:
                    time.sleep(retry_delay)

        else:
            # Foutmelding logging
            print(f"FOUTMELDING | Verbinding met configuratie database mislukt voor: {klantnaam}")
            logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Verbinding met configuratie database mislukt voor: {klantnaam}")
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
            logging(finn_it_connection_string, klantnaam, f"Ophalen divisiecodes gelukt voor: {klantnaam}")
        else:
            # Foutmelding logging
            print(f"FOUTMELDING | Verbinding met divisie database mislukt voor: {klantnaam}")
            logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Verbinding met divisie database mislukt voor: {klantnaam}")
            continue

        # Verbinding maken per divisie code
        for division_name, division_code in division_dict.items():
            # Start logging en print
            print(f"Begin GET Requests voor divisie: {division_name} ({division_code}) | {klantnaam}")
            logging(finn_it_connection_string, klantnaam, f"Start GET Requests voor divisie: {division_name} ({division_code}) | {klantnaam}")

            url = f"https://start.exactonline.nl/api/v1/{division_code}/"
            
            # Bepaal of het een reset of reguliere sync is
            if laatste_sync:
                    huidige_datum = datetime.now()
                    laatste_sync_datum = datetime.strptime(laatste_sync, "%Y-%m-%dT%H:%M:%S")
                    verschil_in_jaren = (huidige_datum - laatste_sync_datum).days / 365

                    if verschil_in_jaren > 1.2:
                        print("Reset sync")
                        logging(finn_it_connection_string, klantnaam, f"Reset sync voor divisie: {division_name} ({division_code}) | {klantnaam}")
                        endpoints = {
                            "ArtikelenExtraVelden": f"read/logistics/ItemExtraField?$filter=Modified ge datetime'{laatste_sync}'",
                            "Grootboekrekening": f"bulk/financial/GLAccounts?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,BalanceSide,BalanceType,Code,Costcenter,CostcenterDescription,Costunit,CostunitDescription,Description,Division,Type,TypeDescription",
                            "Artikelen": f"bulk/Logistics/Items?&$select=ID,StandardSalesPrice,Class_01,Class_02,Class_03,Class_04,Class_05,Class_06,Class_07,Class_08,Class_09,Class_10,Code,CostPriceCurrency,CostPriceNew,CostPriceStandard,AverageCost,Created,Description,Division,EndDate,ExtraDescription,FreeBoolField_01,FreeBoolField_02,FreeBoolField_03,FreeBoolField_04,FreeBoolField_05,FreeDateField_01,FreeDateField_02,FreeDateField_03,FreeDateField_04,FreeDateField_05,FreeNumberField_01,FreeNumberField_02,FreeNumberField_03,FreeNumberField_04,FreeNumberField_05,FreeNumberField_06,FreeNumberField_07,FreeNumberField_08,FreeTextField_01,FreeTextField_02,FreeTextField_03,FreeTextField_04,FreeTextField_05,FreeTextField_06,FreeTextField_07,FreeTextField_08,FreeTextField_09,FreeTextField_10,ItemGroup,IsMakeItem,IsNewContract,IsOnDemandItem,IsPackageItem,IsPurchaseItem,IsSalesItem,IsSerialItem,IsStockItem,IsSubcontractedItem,IsTaxableItem,IsTime,IsWebshopItem,GrossWeight,NetWeight,NetWeightUnit,Notes,SalesVatCode,SalesVatCodeDescription,SecurityLevel,StartDate,StatisticalCode,Unit,UnitDescription,UnitType",
                            "ReportingBalance": f"financial/ReportingBalance?$filter=ReportingYear ge {reporting_year}&$select=ID,Amount,AmountCredit,AmountDebit,BalanceType,CostCenterDescription,CostUnitDescription,Count,Division,GLAccount,ReportingPeriod,ReportingYear",
                            "VerkoopOrders": f"bulk/SalesOrder/SalesOrders?&$select=ApprovalStatusDescription,Approved,ApproverFullName,Created,CreatorFullName,Currency,DeliverTo,Description,Division,InvoiceStatusDescription,InvoiceTo,OrderDate,OrderedBy,OrderID,OrderNumber,Remarks,ShippingMethodDescription,StatusDescription,YourRef,SalesOrderLines/AmountDC,SalesOrderLines/CostCenterDescription,SalesOrderLines/CostPriceFC,SalesOrderLines/CostUnitDescription,SalesOrderLines/DeliveryDate,SalesOrderLines/Description,SalesOrderLines/Discount,SalesOrderLines/ID,SalesOrderLines/Item,SalesOrderLines/LineNumber,SalesOrderLines/Quantity,SalesOrderLines/VATAmount,SalesOrderLines/VATPercentage&$expand=SalesOrderLines",
                            "Verkoopfacturen": f"bulk/SalesInvoice/SalesInvoices?&&$select=Currency,DeliverTo,Description,Division,InvoiceDate,InvoiceID,InvoiceNumber,InvoiceTo,OrderDate,OrderedBy,PaymentConditionDescription,Remarks,ShippingMethodDescription,StatusDescription,YourRef,StarterSalesInvoiceStatusDescription,SalesInvoiceLines/AmountDC,SalesInvoiceLines/CostCenterDescription,SalesInvoiceLines/CostUnitDescription,SalesInvoiceLines/DeliveryDate,SalesInvoiceLines/Description,SalesInvoiceLines/Description,SalesInvoiceLines/Discount,SalesInvoiceLines/EmployeeFullName,SalesInvoiceLines/GLAccount,SalesInvoiceLines/ID,SalesInvoiceLines/Item,SalesInvoiceLines/LineNumber,SalesInvoiceLines/Quantity,SalesInvoiceLines/SalesOrder,SalesInvoiceLines/SalesOrderLine,SalesInvoiceLines/UnitDescription,SalesInvoiceLines/UnitPrice,SalesInvoiceLines/VATAmountDC,SalesInvoiceLines/VATPercentage&$expand=SalesInvoiceLines",
                            "Voorraad": f"inventory/ItemWarehouses?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,CurrentStock,Division,Item,ItemEndDate,ItemStartDate,ItemUnit,ItemUnitDescription,MaximumStock,PlannedStockIn,PlannedStockOut,ProjectedStock,ReservedStock,SafetyStock,Warehouse,WarehouseCode,WarehouseDescription",
                            "ArtikelGroepen": f"logistics/ItemGroups?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,Code,Description,Division",
                            "Verkoopkansen": f"crm/Opportunities?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,Account,ActionDate,AmountDC,CloseDate,Created,CreatorFullName,Currency,Division,Name,OpportunityStageDescription,OpportunityStatus,Probability,OwnerFullName,SalesTypeDescription,ReasonCodeDescription,CampaignDescription,LeadSourceDescription,ContactFullName",
                            "RelatieKeten": f"crm/AccountClassifications?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,AccountClassificationNameDescription,Code,Description,Division",
                            "Relaties": f"bulk/CRM/Accounts?$filter=Modified ge datetime'{laatste_sync}'&$select=AccountManagerFullName,ActivitySector,ActivitySubSector,AddressLine1,BusinessType,City,Classification,Classification1,Classification2,Classification3,Classification4,Classification5,Classification6,Classification7,Classification8,ClassificationDescription,Code,CountryName,CustomerSince,Division,ID,IsPurchase,IsSupplier,Name,Parent,Postcode,SalesCurrencyDescription,StateName,Status",
                            "Budget": f"budget/Budgets?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,AmountDC,BudgetScenarioDescription,CostcenterDescription,CostunitDescription,Division,GLAccount,ItemDescription,ReportingPeriod,ReportingYear",
                            "CrediteurenOpenstaand": "read/financial/PayablesList?$select=HID,AccountId,Amount,CurrencyCode,Description,DueDate,EntryNumber,InvoiceDate,InvoiceNumber,YourRef",
                            "DebiteurenOpenstaand": "read/financial/ReceivablesList?$select=HID,AccountId,Amount,CurrencyCode,Description,DueDate,EntryNumber,InvoiceDate,InvoiceNumber,YourRef",
                            "GrootboekMapping": "financial/GLAccountClassificationMappings?$select=ID,Classification,Division,GLAccount,GLSchemeDescription",
                            "GrootboekRubriek": "bulk/Financial/GLClassifications?$select=Code,Description,Division,ID,Parent,TaxonomyNamespaceDescription",
                            "GrootboekMutaties": f"bulk/Financial/TransactionLines?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,Account,AmountDC,AmountVATFC,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,Currency,Date,Description,Division,EntryNumber,FinancialPeriod,FinancialYear,GLAccount,InvoiceNumber,OrderNumber,PaymentReference,VATPercentage,Type",
                        }
                    else:
                        print("Reguliere sync")
                        endpoints = {
                            "ArtikelenExtraVelden": f"read/logistics/ItemExtraField?$filter=Modified ge datetime'{laatste_sync}'",
                            "Grootboekrekening": f"bulk/financial/GLAccounts?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,BalanceSide,BalanceType,Code,Costcenter,CostcenterDescription,Costunit,CostunitDescription,Description,Division,Type,TypeDescription",
                            "Artikelen": f"Logistics/Items?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,StandardSalesPrice,Class_01,Class_02,Class_03,Class_04,Class_05,Class_06,Class_07,Class_08,Class_09,Class_10,Code,CostPriceCurrency,CostPriceNew,CostPriceStandard,AverageCost,Created,Description,Division,EndDate,ExtraDescription,FreeBoolField_01,FreeBoolField_02,FreeBoolField_03,FreeBoolField_04,FreeBoolField_05,FreeDateField_01,FreeDateField_02,FreeDateField_03,FreeDateField_04,FreeDateField_05,FreeNumberField_01,FreeNumberField_02,FreeNumberField_03,FreeNumberField_04,FreeNumberField_05,FreeNumberField_06,FreeNumberField_07,FreeNumberField_08,FreeTextField_01,FreeTextField_02,FreeTextField_03,FreeTextField_04,FreeTextField_05,FreeTextField_06,FreeTextField_07,FreeTextField_08,FreeTextField_09,FreeTextField_10,ItemGroup,IsMakeItem,IsNewContract,IsOnDemandItem,IsPackageItem,IsPurchaseItem,IsSalesItem,IsSerialItem,IsStockItem,IsSubcontractedItem,IsTaxableItem,IsTime,IsWebshopItem,GrossWeight,NetWeight,NetWeightUnit,Notes,SalesVatCode,SalesVatCodeDescription,SecurityLevel,StartDate,StatisticalCode,Unit,UnitDescription,UnitType",
                            "ReportingBalance": f"financial/ReportingBalance?$filter=ReportingYear ge {reporting_year}&$select=ID,Amount,AmountCredit,AmountDebit,BalanceType,CostCenterDescription,CostUnitDescription,Count,Division,GLAccount,ReportingPeriod,ReportingYear",
                            "VerkoopOrders": f"SalesOrder/SalesOrders?$filter=Modified ge datetime'{laatste_sync}'&$select=ApprovalStatusDescription,Approved,ApproverFullName,Created,CreatorFullName,Currency,DeliverTo,Description,Division,InvoiceStatusDescription,InvoiceTo,OrderDate,OrderedBy,OrderID,OrderNumber,Remarks,ShippingMethodDescription,StatusDescription,YourRef,SalesOrderLines/AmountDC,SalesOrderLines/CostCenterDescription,SalesOrderLines/CostPriceFC,SalesOrderLines/CostUnitDescription,SalesOrderLines/DeliveryDate,SalesOrderLines/Description,SalesOrderLines/Discount,SalesOrderLines/ID,SalesOrderLines/Item,SalesOrderLines/LineNumber,SalesOrderLines/Quantity,SalesOrderLines/VATAmount,SalesOrderLines/VATPercentage&$expand=SalesOrderLines",
                            "Verkoopfacturen": f"SalesInvoice/SalesInvoices?$filter=Modified ge datetime'{laatste_sync}'&&$select=Currency,DeliverTo,Description,Division,InvoiceDate,InvoiceID,InvoiceNumber,InvoiceTo,OrderDate,OrderedBy,PaymentConditionDescription,Remarks,ShippingMethodDescription,StatusDescription,YourRef,StarterSalesInvoiceStatusDescription,SalesInvoiceLines/AmountDC,SalesInvoiceLines/CostCenterDescription,SalesInvoiceLines/CostUnitDescription,SalesInvoiceLines/DeliveryDate,SalesInvoiceLines/Description,SalesInvoiceLines/Description,SalesInvoiceLines/Discount,SalesInvoiceLines/EmployeeFullName,SalesInvoiceLines/GLAccount,SalesInvoiceLines/ID,SalesInvoiceLines/Item,SalesInvoiceLines/LineNumber,SalesInvoiceLines/Quantity,SalesInvoiceLines/SalesOrder,SalesInvoiceLines/SalesOrderLine,SalesInvoiceLines/UnitDescription,SalesInvoiceLines/UnitPrice,SalesInvoiceLines/VATAmountDC,SalesInvoiceLines/VATPercentage&$expand=SalesInvoiceLines",
                            "Voorraad": f"inventory/ItemWarehouses?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,CurrentStock,Division,Item,ItemEndDate,ItemStartDate,ItemUnit,ItemUnitDescription,MaximumStock,PlannedStockIn,PlannedStockOut,ProjectedStock,ReservedStock,SafetyStock,Warehouse,WarehouseCode,WarehouseDescription",
                            "ArtikelGroepen": f"logistics/ItemGroups?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,Code,Description,Division",
                            "Verkoopkansen": f"crm/Opportunities?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,Account,ActionDate,AmountDC,CloseDate,Created,CreatorFullName,Currency,Division,Name,OpportunityStageDescription,OpportunityStatus,Probability,OwnerFullName,SalesTypeDescription,ReasonCodeDescription,CampaignDescription,LeadSourceDescription,ContactFullName",
                            "RelatieKeten": f"crm/AccountClassifications?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,AccountClassificationNameDescription,Code,Description,Division",
                            "Relaties": f"bulk/CRM/Accounts?$filter=Modified ge datetime'{laatste_sync}'&$select=AccountManagerFullName,ActivitySector,ActivitySubSector,AddressLine1,BusinessType,City,Classification,Classification1,Classification2,Classification3,Classification4,Classification5,Classification6,Classification7,Classification8,ClassificationDescription,Code,CountryName,CustomerSince,Division,ID,IsPurchase,IsSupplier,Name,Parent,Postcode,SalesCurrencyDescription,StateName,Status",
                            "Budget": f"budget/Budgets?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,AmountDC,BudgetScenarioDescription,CostcenterDescription,CostunitDescription,Division,GLAccount,ItemDescription,ReportingPeriod,ReportingYear",
                            "CrediteurenOpenstaand": "read/financial/PayablesList?$select=HID,AccountId,Amount,CurrencyCode,Description,DueDate,EntryNumber,InvoiceDate,InvoiceNumber,YourRef",
                            "DebiteurenOpenstaand": "read/financial/ReceivablesList?$select=HID,AccountId,Amount,CurrencyCode,Description,DueDate,EntryNumber,InvoiceDate,InvoiceNumber,YourRef",
                            "GrootboekMapping": "financial/GLAccountClassificationMappings?$select=ID,Classification,Division,GLAccount,GLSchemeDescription",
                            "GrootboekRubriek": "bulk/Financial/GLClassifications?$select=Code,Description,Division,ID,Parent,TaxonomyNamespaceDescription",
                            "GrootboekMutaties": f"bulk/Financial/TransactionLines?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,Account,AmountDC,AmountVATFC,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,Currency,Date,Description,Division,EntryNumber,FinancialPeriod,FinancialYear,GLAccount,InvoiceNumber,OrderNumber,PaymentReference,VATPercentage,Type",
                        }

            # Endpoint loop
            for tabel, endpoint in endpoints.items():
                for table, status in table_config_dict.items():
                    if table == tabel:
                        if status == 0:
                            print(f"Overslaan van GET Requests voor endpoint: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                            logging(finn_it_connection_string, klantnaam, f"Overslaan van GET Requests voor endpoint: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                            continue
                        else:
                            # Start logging
                            print(f"Start GET Requests voor endpoint: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                            logging(finn_it_connection_string, klantnaam, f"Start GET Requests voor endpoint: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                            
                            # Uitvoeren GET Request
                            df = get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel)

                            if df is None:
                                # Foutmelding logging
                                print(f"FOUTMELDING | Fout bij het ophalen van data voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het ophalen van data voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                                errors_occurred = True
                                break

                            elif df.empty:
                                # Geen data opgehaald, maar geen error
                                print(f"Geen data opgehaald voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                                logging(finn_it_connection_string, klantnaam, f"Geen data opgehaald voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                                continue

                            else:
                                # Succes logging
                                logging(finn_it_connection_string, klantnaam, f"Ophalen DataFrame gelukt voor: {tabel} | {division_name} ({division_code}) | {klantnaam}")

                            # Data transformatie Verkoopfacturen
                            if tabel == "Verkoopfacturen":
                                # Start logging
                                logging(finn_it_connection_string, klantnaam, f"Start van data transformatie verkoopfacturen | {division_name} ({division_code}) | {klantnaam}")

                                # Voer data transformatie uit
                                try:
                                    df = append_invoice_lines(df)

                                    # Succes logging
                                    logging(finn_it_connection_string, klantnaam, f"Data transformatie verkoopfacturen gelukt | {division_name} ({division_code}) | {klantnaam}")

                                except Exception as e:
                                    # Foutmelding logging
                                    print(f"FOUTMELDING | Fout bij het transformeren van verkoopfacturen | {division_name} ({division_code}) | {klantnaam}")
                                    logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het transformeren van verkoopfacturen: {e} | {division_name} ({division_code}) | {klantnaam}")
                                    print(e)

                            # Data transformatie VerkoopOrders
                            if tabel == "VerkoopOrders":
                                # Start logging
                                logging(finn_it_connection_string, klantnaam, f"Start van data transformatie verkooporders | {division_name} ({division_code}) | {klantnaam}")

                                # Voer data transformatie uit
                                try:
                                    df = append_order_lines(df)

                                    # Succes logging
                                    logging(finn_it_connection_string, klantnaam, f"Data transformatie verkooporders gelukt | {division_name} ({division_code}) | {klantnaam}")

                                except Exception as e:
                                    # Foutmelding logging
                                    print(f"FOUTMELDING | Fout bij het transformeren van verkooporders | {division_name} ({division_code}) | {klantnaam}")
                                    logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het transformeren van verkooporders: {e} | {division_name} ({division_code}) | {klantnaam}")
                                    print(e)

                            # Start logging
                            logging(finn_it_connection_string, klantnaam, f"Start data mapping voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")

                            # Tabel mapping
                            mapping_dict = {
                                "CrediteurenOpenstaand": CrediteurenOpenstaand,
                                "DebiteurenOpenstaand": DebiteurenOpenstaand,
                                "GrootboekMutaties": GrootboekMutaties,
                                "GrootboekRubriek": GrootboekRubriek,
                                "Grootboekrekening": Grootboekrekening,
                                "Relaties": Relaties,
                                "RelatieKeten": RelatieKeten,
                                "Budget": Budget,
                                "GrootboekMapping": GrootboekMapping,
                                "ReportingBalance": ReportingBalance,
                                "Voorraad": Voorraad,
                                "Artikelen": Artikelen,
                                "ArtikelenExtraVelden": ArtikelenExtraVelden,
                                "ArtikelGroepen": ArtikelGroepen,
                                "Verkoopfacturen": Verkoopfacturen,
                                "VerkoopOrders": VerkoopOrders,
                                "Verkoopkansen": Verkoopkansen
                            }

                            column_mapping = mapping_dict.get(tabel)
                            if column_mapping is None:
                                # Foutmelding logging en print
                                print(f"Geen kolom mapping gevonden voor tabel: {tabel}")
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen mapping gevonden voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                                continue

                            # Transform the DataFrame columns
                            df_transformed = transform_columns(df, column_mapping, division_code)

                            # Succes en start logging
                            logging(finn_it_connection_string, klantnaam, f"Data mapping succesvol voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                            logging(finn_it_connection_string, klantnaam, f"Start data type conversie voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")

                            # Column mapping
                            column_dictionary = {
                                "CrediteurenOpenstaand": CrediteurenOpenstaandTyping,
                                "DebiteurenOpenstaand": DebiteurenOpenstaandTyping,
                                "GrootboekRubriek": GrootboekRubriekTyping,
                                "Grootboekrekening": GrootboekrekeningTyping,
                                "Relaties": RelatiesTyping,
                                "RelatieKeten": RelatieKetenTyping,
                                "Budget": BudgetTyping,
                                "GrootboekMapping": GrootboekMappingTyping,
                                "GrootboekMutaties": GrootboekMutatiesTyping,
                                "ReportingBalance": ReportingBalanceTyping,
                                "Voorraad": VoorraadTyping,
                                "Artikelen": ArtikelenTyping,
                                "ArtikelenExtraVelden": ArtikelenExtraVeldenTyping,
                                "ArtikelGroepen": ArtikelGroepenTyping,
                                "Verkoopfacturen": VerkoopfacturenTyping,
                                "VerkoopOrders": VerkoopOrdersTyping,
                                "Verkoopkansen": VerkoopkansenTyping
                            }
            
                            column_types = column_dictionary.get(tabel)
                            if column_types is None:
                                # Foutmelding logging en print
                                print(f"Geen data type mapping gevonden voor tabel: {tabel}")
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen data type mapping gevonden voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                                continue

                            # Transform the DataFrame column types
                            df_transformed = convert_column_types(df_transformed, column_types)

                            # Succes en start logging
                            logging(finn_it_connection_string, klantnaam, f"Data type conversie succesvol voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                            logging(finn_it_connection_string, klantnaam, f"Start mogelijk verwijderen rijen of complete tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")

                            # Table modes for deleting rows or complete table
                            table_modes = {
                                "Voorraad": "none",
                                "Grootboekrekening": "none",
                                "GrootboekRubriek": "truncate",
                                "GrootboekMutaties": "none",
                                "CrediteurenOpenstaand": "truncate",
                                "DebiteurenOpenstaand": "truncate",
                                "Relaties": "none",
                                "RelatieKeten": "none",
                                "Budget": "none",
                                "GrootboekMapping": "truncate",
                                "ReportingBalance": "reporting_year",
                                "Artikelen": "none",
                                "ArtikelenExtraVelden": "none",
                                "ArtikelGroepen": "none",
                                "Verkoopfacturen": "none",
                                "VerkoopOrders": "none",
                                "Verkoopkansen": "none"
                            }

                            table_mode = table_modes.get(tabel)
                            print(f"Table mode: {table_mode}")
                            
                            if table_mode is None:
                                # Foutmelding logging en print
                                print(f"Geen actie gevonden voor tabel: {tabel}")
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen actie gevonden voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                                continue

                            # Clear the table
                            actie = clear_table(connection_string, tabel, table_mode, reporting_year, division_code)

                            # Succes en start logging
                            logging(finn_it_connection_string, klantnaam, f"Rijen verwijderd voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                            logging(finn_it_connection_string, klantnaam, f"Start toevoegen rijen naar database voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")

                            # Unieke kolom per tabel
                            unique_columns = {
                                "Voorraad": "ID",
                                "Grootboekrekening": "ID",
                                "GrootboekRubriek": "ID",
                                "GrootboekMutaties": "ID",
                                "CrediteurenOpenstaand": "ID",
                                "DebiteurenOpenstaand": "ID",
                                "Relaties": "ID",
                                "RelatieKeten": "ID",
                                "Budget": "ID",
                                "GrootboekMapping": "ID",
                                "ReportingBalance": "ID",
                                "Artikelen": "ID",
                                "ArtikelenExtraVelden": "ArtikelID",
                                "ArtikelGroepen": "ID",
                                "Verkoopfacturen": "FR_FactuurregelID",
                                "VerkoopOrders": "OR_OrderRegelID",
                                "Verkoopkansen": "VerkoopkansID"
                            }
                            
                            # Unieke kolom ophalen voor de specifieke tabel
                            unique_column = unique_columns.get(tabel)
                            if unique_column is None:
                                # Foutmelding logging en print
                                print(f"Geen unieke kolom gevonden voor tabel: {tabel}")
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen unieke kolom gevonden voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                                continue

                            # Administratie kolom per tabel
                            administration_columns = {
                                "Voorraad": "AdministratieCode",
                                "Grootboekrekening": "AdministratieCode",
                                "GrootboekRubriek": "AdministratieCode",
                                "GrootboekMutaties": "AdministratieCode",
                                "CrediteurenOpenstaand": "AdministratieCode",
                                "DebiteurenOpenstaand": "AdministratieCode",
                                "Relaties": "AdministratieCode",
                                "RelatieKeten": "AdministratieCode",
                                "Budget": "AdministratieCode",
                                "GrootboekMapping": "AdministratieCode",
                                "ReportingBalance": "AdministratieCode",
                                "Artikelen": "AdministratieCode",
                                "ArtikelenExtraVelden": "AdministratieCode",
                                "ArtikelGroepen": "AdministratieCode",
                                "Verkoopfacturen": "F_AdministratieCode",
                                "VerkoopOrders": "O_AdministratieCode",
                                "Verkoopkansen": "AdministratieCode"
                                
                            }

                            # Administratie kolom ophalen voor de specifieke tabel
                            administration_column = administration_columns.get(tabel)
                            if administration_column is None:
                                # Foutmelding logging en print
                                print(f"Geen administratie kolom gevonden voor tabel: {tabel}")
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen administratie kolom gevonden voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                                continue

                            # Schrijf de DataFrame naar de database
                            try:
                                write_to_database(df_transformed, tabel, connection_string, unique_column, administration_column, table_mode, laatste_sync)
                                
                                # Succeslogging bij succes
                                logging(finn_it_connection_string, klantnaam, f"Toeschrijven rijen naar database succesvol afgerond voor tabel: {tabel} | {division_name} ({division_code}) | {klantnaam}")
                                    
                            except Exception as e:
                                # Foutmelding logging en print
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het toevoegen naar database voor tabel: {tabel} | {division_name} ({division_code}) - Foutmelding: {str(e)}")
                                print(f"Fout bij het toevoegen naar database: {e}")
                                errors_occurred = True

            # Succes logging
            logging(finn_it_connection_string, klantnaam, f"GET Requests succesvol afgerond voor divisie: {division_name} ({division_code}) | {klantnaam}")

        # Succes en start logging
        print(f"Sync succesvol afgerond voor klant: {klantnaam}")
        logging(finn_it_connection_string, klantnaam, f"Sync succesvol afgerond voor klant: {klantnaam}")

        if not errors_occurred:
            logging(finn_it_connection_string, klantnaam, f"Creëren nieuwe laatste sync en reporting year voor klant: {klantnaam}")

            try:    
                # Update laatste sync en reporting year
                save_laatste_sync(connection_string, nieuwe_laatste_sync)
                save_reporting_year(connection_string)

                # Succes logging
                print(f"Laatste sync en reporting year succesvol geüpdate voor klant: {klantnaam}")
                logging(finn_it_connection_string, klantnaam, f"Laatste sync en reporting year succesvol geüpdate voor klant: {klantnaam}")

            except Exception as e:
                # Foutmelding logging en print
                print(f"Fout bij het toevoegen naar database: {e}")
                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij updaten laatste_sync en reporting year voor klant: {klantnaam} - Foutmelding: {str(e)}")
        else:
            # Logging dat er fouten zij opgetreden en dat de laatste sync niet is geupdate
            print(f"Er zijn fouten opgetreden voor klant: {klantnaam}, laatste_sync wordt niet geüpdate")
            logging(finn_it_connection_string, klantnaam, f"Er zijn fouten opgetreden voor klant: {klantnaam}, laatste_sync wordt niet geüpdate")

        # Succes logging
        print(f"Endpoints succesvol afgerond voor klant: {klantnaam}")
        logging(finn_it_connection_string, klantnaam, f"Script succesvol afgerond voor klant: {klantnaam}")

    # Totale tijdsduur script
    end_time = time.time()
    total_duration = end_time - start_time  # Tijd in seconden

    # Omzetten naar HH:MM:SS
    hours, remainder = divmod(total_duration, 3600)
    minutes, seconds = divmod(remainder, 60)
    formatted_duration = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

    # Succes logging
    print(f"Script volledig afgerond in {formatted_duration}")
    logging(finn_it_connection_string, "Finn It", f"Script volledig afgerond in {formatted_duration}")