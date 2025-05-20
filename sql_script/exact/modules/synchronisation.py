from datetime import datetime, timedelta
import logging

class SyncFormatManager:
    def __init__(self, config_manager):
        self.config = config_manager
        self.logger = config_manager.logger
        self.script_name = config_manager.script_name

    def _create_reset_endpoints(self, laatste_sync, reporting_year):
        """Endpoints voor een reset sync (laatste sync > 2 jaar geleden)"""
        return {
            "Documenten": f"bulk/Documents/Documents?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,CategoryDescription,Division,DocumentDate,DocumentViewUrl,FinancialTransactionEntryID,HasEmptyBody,Modified,SalesInvoiceNumber,Type",
            "Offertes": f"bulk/CRM/Quotations?$select=QuotationID,QuotationNumber,VersionNumber,SalesPersonFullName,Currency,Description,StatusDescription,OrderAccount,Opportunity,QuotationDate,ClosingDate,CloseDate,DeliveryDate,Remarks,YourRef,AmountDC,QuotationLines/ID,QuotationLines/AmountDC,QuotationLines/CostCenterDescription,QuotationLines/CostUnitDescription,QuotationLines/Description,QuotationLines/Discount,QuotationLines/Item,QuotationLines/LineNumber,QuotationLines/Quantity,QuotationLines/UnitDescription,QuotationLines/UnitPrice,QuotationLines/VATAmountFC,QuotationLines/VATPercentage&$expand=QuotationLines",
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
            "Relaties": f"bulk/CRM/Accounts?$filter=Modified ge datetime'{laatste_sync}'&$select=AccountManagerFullName,ActivitySector,ActivitySubSector,AddressLine1,BusinessType,City,Classification,Classification1,Classification2,Classification3,Classification4,Classification5,Classification6,Classification7,Classification8,ClassificationDescription,Code,CountryName,CustomerSince,Division,ID,IsPurchase,IsSupplier,Name,Parent,Postcode,StateName,Status",
            "Budget": f"budget/Budgets?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,AmountDC,BudgetScenarioDescription,CostcenterDescription,CostunitDescription,Division,GLAccount,ItemDescription,ReportingPeriod,ReportingYear",
            "CrediteurenOpenstaand": "read/financial/PayablesList?$select=HID,AccountId,Amount,CurrencyCode,Description,DueDate,EntryNumber,InvoiceDate,InvoiceNumber,YourRef",
            "DebiteurenOpenstaand": "read/financial/ReceivablesList?$select=HID,AccountId,Amount,CurrencyCode,Description,DueDate,EntryNumber,InvoiceDate,InvoiceNumber,YourRef",
            "GrootboekMapping": "financial/GLAccountClassificationMappings?$select=ID,Classification,Division,GLAccount,GLSchemeDescription",
            "GrootboekRubriek": "bulk/Financial/GLClassifications?$select=Code,Description,Division,ID,Parent,TaxonomyNamespaceDescription",
            "GrootboekMutaties": f"bulk/Financial/TransactionLines?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,Account,AmountDC,AmountVATFC,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,Currency,Date,Description,Division,EntryNumber,FinancialPeriod,FinancialYear,GLAccount,InvoiceNumber,OrderNumber,PaymentReference,VATPercentage,Type,Document",
        }

    def _create_regular_endpoints(self, laatste_sync, reporting_year):
        """Endpoints voor een reguliere sync (laatste sync < 2 jaar geleden)"""
        return {
            "Documenten": f"documents/Documents?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,CategoryDescription,Division,DocumentDate,DocumentViewUrl,FinancialTransactionEntryID,HasEmptyBody,Modified,SalesInvoiceNumber,Type",
            "Offertes": f"crm/Quotations?$filter=Modified ge datetime'{laatste_sync}'&$select=QuotationID,QuotationNumber,VersionNumber,SalesPersonFullName,Currency,Description,StatusDescription,OrderAccount,Opportunity,QuotationDate,ClosingDate,CloseDate,DeliveryDate,Remarks,YourRef,AmountDC,QuotationLines/ID,QuotationLines/AmountDC,QuotationLines/CostCenterDescription,QuotationLines/CostUnitDescription,QuotationLines/Description,QuotationLines/Discount,QuotationLines/Item,QuotationLines/LineNumber,QuotationLines/Quantity,QuotationLines/UnitDescription,QuotationLines/UnitPrice,QuotationLines/VATAmountFC,QuotationLines/VATPercentage&$expand=QuotationLines",
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
            "Relaties": f"bulk/CRM/Accounts?$filter=Modified ge datetime'{laatste_sync}'&$select=AccountManagerFullName,ActivitySector,ActivitySubSector,AddressLine1,BusinessType,City,Classification,Classification1,Classification2,Classification3,Classification4,Classification5,Classification6,Classification7,Classification8,ClassificationDescription,Code,CountryName,CustomerSince,Division,ID,IsPurchase,IsSupplier,Name,Parent,Postcode,StateName,Status",
            "Budget": f"budget/Budgets?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,AmountDC,BudgetScenarioDescription,CostcenterDescription,CostunitDescription,Division,GLAccount,ItemDescription,ReportingPeriod,ReportingYear",
            "CrediteurenOpenstaand": "read/financial/PayablesList?$select=HID,AccountId,Amount,CurrencyCode,Description,DueDate,EntryNumber,InvoiceDate,InvoiceNumber,YourRef",
            "DebiteurenOpenstaand": "read/financial/ReceivablesList?$select=HID,AccountId,Amount,CurrencyCode,Description,DueDate,EntryNumber,InvoiceDate,InvoiceNumber,YourRef",
            "GrootboekMapping": "financial/GLAccountClassificationMappings?$select=ID,Classification,Division,GLAccount,GLSchemeDescription",
            "GrootboekRubriek": "bulk/Financial/GLClassifications?$select=Code,Description,Division,ID,Parent,TaxonomyNamespaceDescription",
            "GrootboekMutaties": f"bulk/Financial/TransactionLines?$filter=Modified ge datetime'{laatste_sync}'&$select=ID,Account,AmountDC,AmountVATFC,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,Currency,Date,Description,Division,EntryNumber,FinancialPeriod,FinancialYear,GLAccount,InvoiceNumber,OrderNumber,PaymentReference,VATPercentage,Type,Document",
        }

    def define_sync_format(self, laatste_sync, reporting_year):
        """
        Bepaalt het sync formaat op basis van de laatste synchronisatie datum of script type.
        
        Args:
            laatste_sync: Laatste synchronisatie datum in ISO formaat
            reporting_year: Jaar waarover gerapporteerd moet worden
            
        Returns:
            Dictionary met endpoints voor de synchronisatie
        """
        # Scenario 1: Volledige sync (gebruik get_endpoints)
        if self.script_name == 'Volledig':
            logging.info("Volledige sync wordt uitgevoerd")
            return self.get_endpoints()
            
        # Scenario 2: Geen laatste sync datum (gebruik reset endpoints)
        if not laatste_sync:
            logging.info("Geen laatste sync datum gevonden, reset sync wordt uitgevoerd")
            return self._create_reset_endpoints(laatste_sync, reporting_year)

        # Scenario 3: Bepaal of we reset of reguliere sync moeten gebruiken
        huidige_datum = datetime.now()
        laatste_sync_datum = datetime.strptime(laatste_sync, "%Y-%m-%dT%H:%M:%S")
        verschil_in_jaren = (huidige_datum - laatste_sync_datum).days / 365

        is_reset_sync = verschil_in_jaren > 2
        logging.info(f"{'Reset' if is_reset_sync else 'Reguliere'} sync wordt uitgevoerd")
        
        # Gebruik reset of reguliere endpoints afhankelijk van is_reset_sync
        if is_reset_sync:
            return self._create_reset_endpoints(laatste_sync, reporting_year)
        else:
            return self._create_regular_endpoints(laatste_sync, reporting_year)
        
    def get_endpoints(self):
        """Retourneert endpoints voor een volledige sync"""
        last_year = datetime.now() - timedelta(days=365)
        start_date = last_year.replace(day=1, month=1, hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")   

        endpoints = {
            "Documenten": f"bulk/Documents/Documents?$filter=Modified ge datetime'{start_date}'&$select=ID,CategoryDescription,Division,DocumentDate,DocumentViewUrl,FinancialTransactionEntryID,HasEmptyBody,Modified,SalesInvoiceNumber,Type",
            "Verkoopfacturen": f"bulk/SalesInvoice/SalesInvoices?$filter=InvoiceDate ge datetime'{start_date}'&$select=Currency,DeliverTo,Description,Division,InvoiceDate,InvoiceID,InvoiceNumber,InvoiceTo,OrderDate,OrderedBy,PaymentConditionDescription,Remarks,ShippingMethodDescription,StatusDescription,YourRef,StarterSalesInvoiceStatusDescription,SalesInvoiceLines/AmountDC,SalesInvoiceLines/CostCenterDescription,SalesInvoiceLines/CostUnitDescription,SalesInvoiceLines/DeliveryDate,SalesInvoiceLines/Description,SalesInvoiceLines/Description,SalesInvoiceLines/Discount,SalesInvoiceLines/EmployeeFullName,SalesInvoiceLines/GLAccount,SalesInvoiceLines/ID,SalesInvoiceLines/Item,SalesInvoiceLines/LineNumber,SalesInvoiceLines/Quantity,SalesInvoiceLines/SalesOrder,SalesInvoiceLines/SalesOrderLine,SalesInvoiceLines/UnitDescription,SalesInvoiceLines/UnitPrice,SalesInvoiceLines/VATAmountDC,SalesInvoiceLines/VATPercentage&$expand=SalesInvoiceLines",
            "VerkoopOrders": f"bulk/SalesOrder/SalesOrders?$filter=OrderDate ge datetime'{start_date}'&$select=ApprovalStatusDescription,Approved,ApproverFullName,Created,CreatorFullName,Currency,DeliverTo,Description,Division,InvoiceStatusDescription,InvoiceTo,OrderDate,OrderedBy,OrderID,OrderNumber,Remarks,ShippingMethodDescription,StatusDescription,YourRef,SalesOrderLines/AmountDC,SalesOrderLines/CostCenterDescription,SalesOrderLines/CostPriceFC,SalesOrderLines/CostUnitDescription,SalesOrderLines/DeliveryDate,SalesOrderLines/Description,SalesOrderLines/Discount,SalesOrderLines/ID,SalesOrderLines/Item,SalesOrderLines/LineNumber,SalesOrderLines/Quantity,SalesOrderLines/VATAmount,SalesOrderLines/VATPercentage&$expand=SalesOrderLines",
            "GrootboekMutaties": f"bulk/Financial/TransactionLines?$filter=FinancialYear ge {start_date[:4]}&$select=ID,Account,AmountDC,AmountVATFC,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,Currency,Date,Description,Division,EntryNumber,FinancialPeriod,FinancialYear,GLAccount,InvoiceNumber,OrderNumber,PaymentReference,VATPercentage,Type,Document",
        }
        
        return endpoints