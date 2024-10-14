from dotenv import load_dotenv
import os
import time
from datetime import datetime
from modules.logging import logging
from modules.config import fetch_all_connection_strings, fetch_configurations, fetch_division_codes, save_laatste_sync, save_reporting_year, fetch_table_configurations, fetch_script_id
from modules.database import connect_to_database, write_to_database, clear_table
from modules.type_mapping import convert_column_types, CrediteurenOpenstaandTyping, DebiteurenOpenstaandTyping, GrootboekRubriekTyping, GrootboekrekeningTyping, RelatiesTyping, RelatieKetenTyping, BudgetTyping, GrootboekMappingTyping, ReportingBalanceTyping, GrootboekMutatiesTyping, VoorraadTyping, ArtikelenTyping, ArtikelenExtraVeldenTyping, ArtikelGroepenTyping, VerkoopfacturenTyping, VerkoopkansenTyping, VerkoopOrdersTyping
from modules.table_mapping import transform_columns, CrediteurenOpenstaand, DebiteurenOpenstaand, GrootboekMutaties, GrootboekRubriek, Grootboekrekening, Relaties, RelatieKeten, Budget, GrootboekMapping, ReportingBalance, Voorraad, Artikelen, ArtikelenExtraVelden, ArtikelGroepen, Verkoopfacturen, VerkoopOrders, Verkoopkansen
from modules.data_transformation import append_invoice_lines, append_order_lines
from modules.get_request import get_request

if __name__ == "__main__":

    # Definiëren van script
    script = "Wijzigingen"

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

        # Error tracking
        errors_occurred = False

        # Starttijd voor klant
        nieuwe_laatste_sync = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # Start logging
        logging(finn_it_connection_string, klantnaam, f"Ophalen configuratie gegevens gestart", script_id, script)
        
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
                reporting_year = config_dict['ReportingYear']
                config_conn.close()
        
                # Succes en start logging
                logging(finn_it_connection_string, klantnaam, f"Ophalen configuratie gegevens gelukt", script_id, script)
                logging(finn_it_connection_string, klantnaam, f"Ophalen divisiecodes gestart", script_id, script)
            else:
                # Foutmelding logging
                print(f"FOUTMELDING | Ophalen configuratie gegevens mislukt", script_id, script)
                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Ophalen configuratie gegevens mislukt", script_id, script)
                continue
        
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
            print(f"FOUTMELDING | Verbinding met configuratie database mislukt", script_id, script)
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
            print(f"FOUTMELDING | Verbinding met divisie database mislukt", script_id, script)
            logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Verbinding met divisie database mislukt", script_id, script)
            continue

        # Verbinding maken per divisie code
        for division_name, division_code in division_dict.items():
            # Start logging en print
            print(f"Begin GET Requests voor divisie: {division_name} ({division_code}) | {klantnaam}")
            logging(finn_it_connection_string, klantnaam, f"Start GET Requests voor nieuwe divisie", script_id, script, division_code)

            url = f"https://start.exactonline.nl/api/v1/{division_code}/"
            
            # Bepaal of het een reset of reguliere sync is
            if laatste_sync:
                    huidige_datum = datetime.now()
                    laatste_sync_datum = datetime.strptime(laatste_sync, "%Y-%m-%dT%H:%M:%S")
                    verschil_in_jaren = (huidige_datum - laatste_sync_datum).days / 365

                    if verschil_in_jaren > 1.2:
                        print("Reset sync")
                        logging(finn_it_connection_string, klantnaam, f"Reset sync", script_id, script, division_code)
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
                            logging(finn_it_connection_string, klantnaam, f"Overslaan van GET Requests", script_id, script, division_code, tabel)
                            continue
                        else:
                            # Uitvoeren GET Request
                            df = get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel, script_id, script)

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

                                    # Succes logging
                                    logging(finn_it_connection_string, klantnaam, f"Data transformatie gelukt", script_id, script, division_code, tabel)

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
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen mapping gevonden", script_id, script, division_code, tabel)
                                continue

                            # Transform the DataFrame columns
                            df_transformed = transform_columns(df, column_mapping, division_code)

                            # Succes en start logging
                            logging(finn_it_connection_string, klantnaam, f"Data mapping succesvol", script_id, script, division_code, tabel)
                            logging(finn_it_connection_string, klantnaam, f"Start data type conversie", script_id, script, division_code, tabel)

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
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen data type mapping gevonden", script_id, script, division_code, tabel)
                                continue

                            # Transform the DataFrame column types
                            df_transformed = convert_column_types(df_transformed, column_types)

                            # Succes en start logging
                            logging(finn_it_connection_string, klantnaam, f"Data type conversie succesvol", script_id, script, division_code, tabel)
                            logging(finn_it_connection_string, klantnaam, f"Start mogelijk verwijderen rijen of complete tabel", script_id, script, division_code, tabel)

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
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen actie gevonden", script_id, script, division_code, tabel)
                                continue

                            # Clear the table
                            actie, rows_deleted = clear_table(connection_string, tabel, table_mode, reporting_year, division_code)

                            # Succes en start logging
                            logging(finn_it_connection_string, klantnaam, f"Totaal verwijderde rijen {rows_deleted}", script_id, script, division_code, tabel)
                            logging(finn_it_connection_string, klantnaam, f"Start toevoegen rijen naar database", script_id, script, division_code, tabel)

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
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen unieke kolom gevonden", script_id, script, division_code, tabel)
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
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen administratie kolom gevonden", script_id, script, division_code, tabel)
                                continue

                            # Schrijf de DataFrame naar de database
                            try:
                                write_to_database(df_transformed, tabel, connection_string, unique_column, administration_column, table_mode, laatste_sync)
                                
                                # Succeslogging bij succes
                                logging(finn_it_connection_string, klantnaam, f"Succesvol {len(df)} rijen toegevoegd aan de database", script_id, script, division_code, tabel)
                                    
                            except Exception as e:
                                # Foutmelding logging en print
                                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het toevoegen naar database | Foutmelding: {str(e)}", script_id, script, division_code, tabel)
                                print(f"Fout bij het toevoegen naar database: {e}")
                                errors_occurred = True

            # Succes logging
            logging(finn_it_connection_string, klantnaam, f"GET Requests succesvol afgerond deze divisie", script_id, script, division_code)

        # Succes en start logging
        print(f"Sync succesvol afgerond voor klant: {klantnaam}")
        logging(finn_it_connection_string, klantnaam, f"Sync succesvol afgerond", script_id, script)

        if not errors_occurred:
            logging(finn_it_connection_string, klantnaam, f"Creëren nieuwe laatste sync en reporting year", script_id, script)

            try:    
                # Update laatste sync en reporting year
                save_laatste_sync(connection_string, nieuwe_laatste_sync)
                save_reporting_year(connection_string)

                # Succes logging
                print(f"Laatste sync en reporting year succesvol geüpdate voor klant: {klantnaam}")
                logging(finn_it_connection_string, klantnaam, f"Laatste sync en reporting year succesvol geüpdate", script_id, script)

            except Exception as e:
                # Foutmelding logging en print
                print(f"Fout bij het toevoegen naar database: {e}")
                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij updaten laatste_sync en reporting year: {str(e)}", script_id, script)
        else:
            # Logging dat er fouten zij opgetreden en dat de laatste sync niet is geupdate
            print(f"Er zijn fouten opgetreden voor klant: {klantnaam}, laatste_sync wordt niet geüpdate")
            logging(finn_it_connection_string, klantnaam, f"Er zijn fouten opgetreden, laatste_sync wordt niet geüpdate", script_id, script)

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