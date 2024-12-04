from ex_modules.log import log
import pandas as pd

def append_invoice_lines(df):
    # Creëer een factuurregel lijst
    factuurregels = []

    # Variabele om de laatste factuurgegevens bij te houden
    huidige_factuur = {}
    
    # Itereer door de rijen van de dataset
    for index, row in df.iterrows():
        if pd.notna(row['InvoiceID']):
            # Dit is een factuur, sla de relevante gegevens op
            huidige_factuur = {
                'Currency': row['Currency'],
                'DeliverTo': row['DeliverTo'],
                'Invoice_Description': row['Description'],
                'Division': row['Division'],
                'InvoiceDate': row['InvoiceDate'],
                'InvoiceID': row['InvoiceID'],
                'InvoiceNumber': row['InvoiceNumber'],
                'InvoiceTo': row['InvoiceTo'],
                'OrderDate': row['OrderDate'],
                'OrderedBy': row['OrderedBy'],
                'PaymentConditionDescription': row['PaymentConditionDescription'],
                'Remarks': row['Remarks'],
                'ShippingMethodDescription': row['ShippingMethodDescription'],
                'StatusDescription': row['StatusDescription'],
                'YourRef': row['YourRef'],
                'StarterSalesInvoiceStatusDescription': row['StarterSalesInvoiceStatusDescription'],
            }
        else:
            # Dit is een factuurregel, voeg de factuurgegevens toe en sla op
            factuurregel = row.copy()  # Kopieer de huidige factuurregel
            for key, value in huidige_factuur.items():
                factuurregel[key] = value  # Voeg de factuurgegevens toe aan de factuurregel
            factuurregels.append(factuurregel)
    
    # Maak een DataFrame van de factuurregels
    df = pd.DataFrame(factuurregels)

    return df

def append_order_lines(df):
    # Een lege rij voor alle orderregels
    orderregels = []

    # Variabele om de laatste ordergegevens bij te houden
    huidige_order = {}

    # Itereer door de rijen van de dataset
    for index, row in df.iterrows():
        if pd.notna(row['OrderID']):
            # Dit is een order, sla de relevante gegevens op
            huidige_order = {
                'ApprovalStatusDescription': row['ApprovalStatusDescription'],
                'Approved': row['Approved'],
                'ApproverFullName': row['ApproverFullName'],
                'Created': row['Created'],
                'CreatorFullName': row['CreatorFullName'],
                'Currency': row['Currency'],
                'DeliverTo': row['DeliverTo'],
                'Order_Description': row['Description'],
                'Division': row['Division'],
                'InvoiceStatusDescription': row['InvoiceStatusDescription'],
                'InvoiceTo': row['InvoiceTo'],
                'OrderDate': row['OrderDate'],
                'OrderedBy': row['OrderedBy'],
                'OrderID': row['OrderID'],
                'OrderNumber': row['OrderNumber'],
                'Remarks': row['Remarks'],
                'ShippingMethodDescription': row['ShippingMethodDescription'],
                'StatusDescription': row['StatusDescription'],
                'YourRef': row['YourRef'],
            }
        else:
            # Dit is een orderregel, voeg de ordergegevens toe en sla op
            orderregel = row.copy()  # Kopieer de huidige orderregel
            for key, value in huidige_order.items():
                orderregel[key] = value  # Voeg de ordergegevens toe aan de orderregel
            orderregels.append(orderregel)
    
    # Maak een DataFrame van de orderregels
    df = pd.DataFrame(orderregels)

    return df

def append_quotation_lines(df):
    # Een lege rij voor alle offerteregels
    offerte_regels = []

    # Variabele om de laatste offertegegevens bij te houden
    huidige_offerte = {}

    # Itereer door de rijen van de dataset
    for index, row in df.iterrows():
        if pd.notna(row['QuotationID']):
            # Dit is een offerte, sla de relevante gegevens op
            huidige_offerte = {
                'QuotationID': row['QuotationID'],
                'QuotationNumber': row['QuotationNumber'],
                'VersionNumber': row['VersionNumber'],
                'SalesPersonFullName': row['SalesPersonFullName'],
                'Currency': row['Currency'],
                'Quotation_Description': row['Description'],  
                'StatusDescription': row['StatusDescription'],
                'OrderAccount': row['OrderAccount'],
                'Opportunity': row['Opportunity'],
                'QuotationDate': row['QuotationDate'],
                'ClosingDate': row['ClosingDate'],
                'CloseDate': row['CloseDate'],
                'DeliveryDate': row['DeliveryDate'],
                'Remarks': row['Remarks'],
                'YourRef': row['YourRef'],          
                }
        else:
            # Dit is een offerteregel, voeg de offertegegevens toe en sla op
            offerteregel = row.copy()  # Kopieer de huidige offerteregel
            for key, value in huidige_offerte.items():
                offerteregel[key] = value  # Voeg de offertegegevens toe aan de offerteregel
            offerte_regels.append(offerteregel)
    
    # Maak een DataFrame van de orderregels
    df = pd.DataFrame(offerte_regels)

    return df
    
def apply_appending_functions(df, tabel, klantnaam, finn_it_connection_string, script_id, script, division_code, division_name):
    
    # Start log
    log(finn_it_connection_string, klantnaam, f"Start van data transformatie", script_id, script, division_code, tabel)

    try:
        if tabel == "Verkoopfacturen":
            df = append_invoice_lines(df)
        elif tabel == "VerkoopOrders":
            df = append_order_lines(df)
        elif tabel == "Offertes":
            df = append_quotation_lines(df)
        else:
            # Geen actie als tabel niet herkend wordt
            log(finn_it_connection_string, klantnaam, f"Onbekend tabel type, geen actie uitgevoerd", script_id, script, division_code, tabel)
            return df

        # Succes log
        log(finn_it_connection_string, klantnaam, f"Data transformatie gelukt", script_id, script, division_code, tabel)

    except Exception as e:
        # Foutmelding log
        print(f"FOUTMELDING | Fout bij het transformeren van {tabel} | {division_name} ({division_code}) | {klantnaam}")
        log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het transformeren: {e}", script_id, script, division_code, tabel)
        print(e)
        return df

    return df