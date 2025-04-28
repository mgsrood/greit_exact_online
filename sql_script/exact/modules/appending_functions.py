import pandas as pd
import logging

class DataTransformer:
    def __init__(self, config_manager):
        self.config = config_manager
        self.klant = config_manager.klant
        self.script_name = config_manager.script_name
        self.script_id = config_manager.script_id
        self.connection_string = config_manager.connection_string
        self.auth_method = config_manager.auth_method
        self.tenant_id = config_manager.tenant_id
        self.client_id = config_manager.client_id
        self.client_secret = config_manager.client_secret

    def _append_invoice_lines(self, df):
        """Transformeer factuurregels door factuurgegevens toe te voegen aan elke regel"""
        factuurregels = []
        huidige_factuur = {}
        
        for _, row in df.iterrows():
            if pd.notna(row['InvoiceID']):
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
                factuurregel = row.copy()
                factuurregel.update(huidige_factuur)
                factuurregels.append(factuurregel)
        
        return pd.DataFrame(factuurregels)

    def _append_order_lines(self, df):
        """Transformeer orderregels door ordergegevens toe te voegen aan elke regel"""
        orderregels = []
        huidige_order = {}

        for _, row in df.iterrows():
            if pd.notna(row['OrderID']):
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
                orderregel = row.copy()
                orderregel.update(huidige_order)
                orderregels.append(orderregel)
        
        return pd.DataFrame(orderregels)

    def _append_quotation_lines(self, df):
        """Transformeer offerteregels door offertegegevens toe te voegen aan elke regel"""
        offerte_regels = []
        huidige_offerte = {}

        for _, row in df.iterrows():
            if pd.notna(row['QuotationID']):
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
                offerteregel = row.copy()
                offerteregel.update(huidige_offerte)
                offerte_regels.append(offerteregel)
        
        return pd.DataFrame(offerte_regels)

    def transform_data(self, df, tabel, division_code, division_name):
        """
        Transformeer de data op basis van het tabel type
        
        Args:
            df: DataFrame met de te transformeren data
            tabel: Type van de tabel (Verkoopfacturen, VerkoopOrders, Offertes)
            division_code: Code van de divisie
            division_name: Naam van de divisie
            
        Returns:
            Getransformeerde DataFrame
        """
        # Start log
        logging.info(f"Start van data transformatie")

        try:
            if tabel == "Verkoopfacturen":
                df = self._append_invoice_lines(df)
            elif tabel == "VerkoopOrders":
                df = self._append_order_lines(df)
            elif tabel == "Offertes":
                df = self._append_quotation_lines(df)
            else:
                # Geen actie als tabel niet herkend wordt
                logging.info(f"Tabel niet geschikt voor appending functies, geen actie uitgevoerd")
                return df

            # Succes log
            logging.info(f"Data transformatie gelukt")

        except Exception as e:
            # Foutmelding log
            error_msg = f"FOUTMELDING | Fout bij het transformeren van {tabel} | {division_name} ({division_code}) | {self.klant}"
            logging.error(error_msg)
            logging.error(f"FOUTMELDING | Fout bij het transformeren: {e}")
            print(e)
            return df

        return df