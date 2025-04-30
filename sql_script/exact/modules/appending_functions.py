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
        
        for _, row in df.iterrows():
            # Basis factuurgegevens
            factuur_data = {
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
            
            # Verwerk SalesInvoiceLines
            sales_lines = row.get('SalesInvoiceLines', {}).get('results', [])
            logging.debug(f"Aantal factuurregels gevonden: {len(sales_lines)}")
            
            if not sales_lines:
                logging.warning(f"Geen factuurregels gevonden voor factuur {factuur_data.get('InvoiceID')}")
                continue
                
            for line in sales_lines:
                line_data = {
                    'AmountDC': line.get('AmountDC'),
                    'CostCenterDescription': line.get('CostCenterDescription'),
                    'CostUnitDescription': line.get('CostUnitDescription'),
                    'DeliveryDate': line.get('DeliveryDate'),
                    'Description': line.get('Description'),
                    'Discount': line.get('Discount'),
                    'EmployeeFullName': line.get('EmployeeFullName'),
                    'GLAccount': line.get('GLAccount'),
                    'ID': line.get('ID'),
                    'Item': line.get('Item'),
                    'LineNumber': line.get('LineNumber'),
                    'Quantity': line.get('Quantity'),
                    'SalesOrder': line.get('SalesOrder'),
                    'SalesOrderLine': line.get('SalesOrderLine'),
                    'UnitDescription': line.get('UnitDescription'),
                    'UnitPrice': line.get('UnitPrice'),
                    'VATAmountDC': line.get('VATAmountDC'),
                    'VATPercentage': line.get('VATPercentage')
                }
                
                # Combineer factuur en regel data
                combined_data = {**factuur_data, **line_data}
                factuurregels.append(combined_data)
        
        result_df = pd.DataFrame(factuurregels)
        logging.info(f"Aantal factuurregels na transformatie: {len(result_df)}")
        return result_df

    def _append_order_lines(self, df):
        """Transformeer orderregels door ordergegevens toe te voegen aan elke regel"""
        orderregels = []
        
        logging.info(f"Aantal rijen in input DataFrame: {len(df)}")
        logging.info(f"Kolommen in input DataFrame: {df.columns.tolist()}")
        
        for _, row in df.iterrows():
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
        logging.info(f"Aantal orderregels na transformatie: {len(result_df)}")
        return result_df

    def _append_quotation_lines(self, df):
        """Transformeer offerteregels door offertegegevens toe te voegen aan elke regel"""
        offerte_regels = []
        
        for _, row in df.iterrows():
            # Basis offertegegevens
            offerte_data = {
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
            
            # Verwerk QuotationLines
            quotation_lines = row.get('QuotationLines', {}).get('results', [])
            logging.debug(f"Aantal offerteregels gevonden: {len(quotation_lines)}")
            
            if not quotation_lines:
                logging.warning(f"Geen offerteregels gevonden voor offerte {offerte_data.get('QuotationID')}")
                continue
                
            for line in quotation_lines:
                line_data = {
                    'AmountDC': line.get('AmountDC'),
                    'CostCenterDescription': line.get('CostCenterDescription'),
                    'CostUnitDescription': line.get('CostUnitDescription'),
                    'Description': line.get('Description'),
                    'Discount': line.get('Discount'),
                    'ID': line.get('ID'),
                    'Item': line.get('Item'),
                    'LineNumber': line.get('LineNumber'),
                    'Quantity': line.get('Quantity'),
                    'UnitDescription': line.get('UnitDescription'),
                    'UnitPrice': line.get('UnitPrice'),
                    'VATAmountFC': line.get('VATAmountFC'),
                    'VATPercentage': line.get('VATPercentage')
                }
                
                # Combineer offerte en regel data
                combined_data = {**offerte_data, **line_data}
                offerte_regels.append(combined_data)
        
        result_df = pd.DataFrame(offerte_regels)
        logging.info(f"Aantal offerteregels na transformatie: {len(result_df)}")
        return result_df

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