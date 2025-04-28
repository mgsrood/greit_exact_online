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
        
        logging.info(f"Aantal rijen in input DataFrame: {len(df)}")
        logging.info(f"Kolommen in input DataFrame: {df.columns.tolist()}")
        
        for index, row in df.iterrows():
            logging.debug(f"Verwerken order {index}: OrderID = {row.get('OrderID')}")
            
            # Basis ordergegevens
            order_data = {
                'ApprovalStatusDescription': row.get('ApprovalStatusDescription'),
                'Approved': row.get('Approved'),
                'ApproverFullName': row.get('ApproverFullName'),
                'Created': row.get('Created'),
                'CreatorFullName': row.get('CreatorFullName'),
                'Currency': row.get('Currency'),
                'DeliverTo': row.get('DeliverTo'),
                'Order_Description': row.get('Description'),
                'Division': row.get('Division'),
                'InvoiceStatusDescription': row.get('InvoiceStatusDescription'),
                'InvoiceTo': row.get('InvoiceTo'),
                'OrderDate': row.get('OrderDate'),
                'OrderedBy': row.get('OrderedBy'),
                'OrderID': row.get('OrderID'),
                'OrderNumber': row.get('OrderNumber'),
                'Remarks': row.get('Remarks'),
                'ShippingMethodDescription': row.get('ShippingMethodDescription'),
                'StatusDescription': row.get('StatusDescription'),
                'YourRef': row.get('YourRef'),
            }
            
            # Verwerk SalesOrderLines
            sales_lines = row.get('SalesOrderLines', {}).get('results', [])
            logging.debug(f"Aantal orderregels gevonden: {len(sales_lines)}")
            
            if not sales_lines:
                logging.warning(f"Geen orderregels gevonden voor order {order_data.get('OrderID')}")
                continue
                
            for line in sales_lines:
                line_data = {
                    'AmountDC': line.get('AmountDC'),
                    'CostCenterDescription': line.get('CostCenterDescription'),
                    'CostPriceFC': line.get('CostPriceFC'),
                    'CostUnitDescription': line.get('CostUnitDescription'),
                    'DeliveryDate': line.get('DeliveryDate'),
                    'Description': line.get('Description'),
                    'Discount': line.get('Discount'),
                    'ID': line.get('ID'),
                    'Item': line.get('Item'),
                    'LineNumber': line.get('LineNumber'),
                    'Quantity': line.get('Quantity'),
                    'VATAmount': line.get('VATAmount'),
                    'VATPercentage': line.get('VATPercentage')
                }
                
                # Combineer order en regel data
                combined_data = {**order_data, **line_data}
                orderregels.append(combined_data)
        
        result_df = pd.DataFrame(orderregels)
        
        return result_df

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