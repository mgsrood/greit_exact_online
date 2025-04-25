from datetime import datetime, timedelta

def get_endpoints():
    last_year = datetime.now() - timedelta(days=365)
    start_date = last_year.replace(day=1, month=1, hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")   

    endpoints = {
        "Verkoopfacturen": f"bulk/SalesInvoice/SalesInvoices?$filter=InvoiceDate ge datetime'{start_date}'&$select=Currency,DeliverTo,Description,Division,InvoiceDate,InvoiceID,InvoiceNumber,InvoiceTo,OrderDate,OrderedBy,PaymentConditionDescription,Remarks,ShippingMethodDescription,StatusDescription,YourRef,StarterSalesInvoiceStatusDescription,SalesInvoiceLines/AmountDC,SalesInvoiceLines/CostCenterDescription,SalesInvoiceLines/CostUnitDescription,SalesInvoiceLines/DeliveryDate,SalesInvoiceLines/Description,SalesInvoiceLines/Description,SalesInvoiceLines/Discount,SalesInvoiceLines/EmployeeFullName,SalesInvoiceLines/GLAccount,SalesInvoiceLines/ID,SalesInvoiceLines/Item,SalesInvoiceLines/LineNumber,SalesInvoiceLines/Quantity,SalesInvoiceLines/SalesOrder,SalesInvoiceLines/SalesOrderLine,SalesInvoiceLines/UnitDescription,SalesInvoiceLines/UnitPrice,SalesInvoiceLines/VATAmountDC,SalesInvoiceLines/VATPercentage&$expand=SalesInvoiceLines",
        "VerkoopOrders": f"bulk/SalesOrder/SalesOrders?$filter=OrderDate ge datetime'{start_date}'&$select=ApprovalStatusDescription,Approved,ApproverFullName,Created,CreatorFullName,Currency,DeliverTo,Description,Division,InvoiceStatusDescription,InvoiceTo,OrderDate,OrderedBy,OrderID,OrderNumber,Remarks,ShippingMethodDescription,StatusDescription,YourRef,SalesOrderLines/AmountDC,SalesOrderLines/CostCenterDescription,SalesOrderLines/CostPriceFC,SalesOrderLines/CostUnitDescription,SalesOrderLines/DeliveryDate,SalesOrderLines/Description,SalesOrderLines/Discount,SalesOrderLines/ID,SalesOrderLines/Item,SalesOrderLines/LineNumber,SalesOrderLines/Quantity,SalesOrderLines/VATAmount,SalesOrderLines/VATPercentage&$expand=SalesOrderLines",
        "GrootboekMutaties": f"bulk/Financial/TransactionLines?$filter=FinancialYear ge {start_date[:4]}&$select=ID,Account,AmountDC,AmountVATFC,CostCenter,CostCenterDescription,CostUnit,CostUnitDescription,Currency,Date,Description,Division,EntryNumber,FinancialPeriod,FinancialYear,GLAccount,InvoiceNumber,OrderNumber,PaymentReference,VATPercentage,Type",
    }
    
    return endpoints