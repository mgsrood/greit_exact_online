from dataclasses import dataclass
import pandas as pd
import logging

@dataclass
class ColumnMappingConfig:
    """Configuratie class voor kolom mappings."""
    mappings = None

    def __post_init__(self):
        self.mappings = {
            "CrediteurenOpenstaand": {
                'HID': 'ID',
                'Division': 'AdministratieCode',
                'AccountId': 'RelatieID',
                'CurrencyCode': 'Valuta',
                'Amount': 'Bedrag',
                'Description': 'Omschrijving',
                'EntryNumber': 'BoekstukNummer',
                'InvoiceNumber': 'FactuurNummer',
                'YourRef': 'Referentie',
                'InvoiceDate': 'FactuurDatum',
                'DueDate': 'VervalDatum',
            },
            "DebiteurenOpenstaand": {
                'HID': 'ID',
                'Division': 'AdministratieCode',
                'AccountId': 'RelatieID',
                'CurrencyCode': 'Valuta',
                'Amount': 'Bedrag',
                'Description': 'Omschrijving',
                'EntryNumber': 'BoekstukNummer',
                'InvoiceNumber': 'FactuurNummer',
                'YourRef': 'Referentie',
                'InvoiceDate': 'FactuurDatum',
                'DueDate': 'VervalDatum',
            },
            "GrootboekMutaties": {
                'ID': 'ID',
                'Division': 'AdministratieCode',
                'GLAccount': 'GrootboekID',
                'Account': 'RelatieID',
                'Date': 'Datum',
                'FinancialYear': 'Boekjaar',
                'FinancialPeriod': 'BoekPeriode',
                'Description': 'Omschrijving',
                'EntryNumber': 'BoekstukNummer',
                'InvoiceNumber': 'FactuurNummer',
                'OrderNumber': 'OrderNummer',
                'PaymentReference': 'Referentie',
                'Currency': 'Valuta',
                'AmountDC': 'Bedrag',
                'AmountVATFC': 'BTWBedrag',
                'VATPercentage': 'BTWPercentage',
                'Type': 'Type',
                'CostCenter': 'KostenplaatsID',
                'CostCenterDescription': 'Kostenplaats',
                'CostUnit': 'KostenDragerID',
                'CostUnitDescription': 'KostenDrager',
            },
            "GrootboekRubriek": {
                'ID': 'ID',
                'Division': 'AdministratieCode',
                'Code': 'RubriekCode',
                'Description': 'Naam',
                'Parent': 'ParentID',
                'TaxonomyNamespaceDescription': 'GrootboekSchema'
            },
            "Grootboekrekening": {
                'ID': 'ID',
                'Division': 'AdministratieCode',
                'Code': 'Grootboekrekeningen',
                'Description': 'Grootboek',
                'BalanceType': 'BalansType',
                'BalanceSide': 'DebetCredit',
                'Type': 'GrootboekIndelingID',
                'TypeDescription': 'GrootboekIndeling',
                'Costcenter': 'KostenplaatsID',
                'CostcenterDescription': 'Kostenplaats',
                'Costunit': 'KostenDragerID',
                'CostunitDescription': 'KostenDrager'
            },
            "Relaties": {
                'ID': 'ID',
                'Division': 'AdministratieCode',
                'Code': 'RelatieCode',
                'CustomerSince': 'KlantSinds',
                'Name': 'KlantNaam',
                'AddressLine1': 'Adres',
                'Postcode': 'Postcode',
                'City': 'Plaats',
                'StateName': 'Provincie',
                'CountryName': 'Land',
                'AccountManagerFullName': 'Relatiebeheerder',
                'BusinessType': 'BusinessType',
                'ActivitySector': 'BusinessSector',
                'ActivitySubSector': 'BusinessSubSector',
                'Classification': 'RelatieGroep',
                'Classification1': 'RelatieGroepID1',
                'Classification2': 'RelatieGroepID2',
                'Classification3': 'RelatieGroepID3',
                'Classification4': 'RelatieGroepID4',
                'Classification5': 'RelatieGroepID5',
                'Classification6': 'RelatieGroepID6',
                'Classification7': 'RelatieGroepID7',
                'Classification8': 'RelatieGroepID8',
                'ClassificationDescription': 'RelatieGroepOmschrijving',
                'Parent': 'Parent',
                'IsPurchase': 'IsSupplier',
                'IsSupplier': 'IsPurchase',
                'Status': 'Status'
            },
            "RelatieKeten": {
                'ID': 'ID',
                'Division': 'AdministratieCode',
                'AccountClassificationNameDescription': 'ClassificatieNaam',
                'Code': 'Code',
                'Description': 'Omschrijving'
            },
            "Budget": {
                'ID': 'ID',
                'AmountDC': 'Bedrag',
                'BudgetScenarioDescription': 'BudgetScenario',
                'CostcenterDescription': 'Kostenplaats',
                'CostunitDescription': 'Kostendrager',
                'Division': 'AdministratieCode',
                'GLAccount': 'GrootboekID',
                'ItemDescription': 'Artikel',
                'ReportingPeriod': 'Boekperiode',
                'ReportingYear': 'Boekjaar'
            },
            "GrootboekMapping": {
                'ID': 'ID',
                'Classification': 'GrootboekRubriekID',
                'Division': 'AdministratieCode',
                'GLAccount': 'GrootboekID',
                'GLSchemeDescription': 'GrootboekMappingSchema'
            },
            "ReportingBalance": {
                'ID': 'ID',
                'Amount': 'Bedrag',
                'AmountCredit': 'BedragCredit',
                'AmountDebit': 'BedragDebit',
                'BalanceType': 'BalansType',
                'CostCenterDescription': 'KostenPlaats',
                'CostUnitDescription': 'KostenDrager',
                'Count': 'AantalMutaties',
                'Division': 'AdministratieCode',
                'GLAccount': 'GrootboekID',
                'ReportingPeriod': 'ReportingPeriod',
                'ReportingYear': 'ReportingYear'
            },
            "Voorraad": {
                'ID': 'ID',
                'CurrentStock': 'HuidigeVoorraad',
                'Division': 'AdministratieCode',
                'Item': 'ArtikelID',
                'ItemEndDate': 'ArtikelEindDatum',
                'ItemStartDate': 'ArtikelStartDatum',
                'ItemUnit': 'ArtikelEenheid',
                'ItemUnitDescription': 'ArtikelEenheidOmschrijving',
                'MaximumStock': 'MaximaleVoorraad',
                'PlannedStockIn': 'GeplandeInkomendeVoorraad',
                'PlannedStockOut': 'GeplandeUitgaandeVoorraad',
                'ProjectedStock': 'VerwachteVoorraad',
                'ReservedStock': 'GereserveerdeVoorraad',
                'SafetyStock': 'Veiligheidsvoorraad',
                'Warehouse': 'MagazijnID',
                'WarehouseCode': 'MagazijnCode',
                'WarehouseDescription': 'MagazijnOmschrijving'
            },
            "Artikelen": {
                'ID': 'ID',
                'StandardSalesPrice': 'StandaardVerkoopprijs',
                'Class_01': 'Categorie1',
                'Class_02': 'Categorie2',
                'Class_03': 'Categorie3',
                'Class_04': 'Categorie4',
                'Class_05': 'Categorie5',
                'Class_06': 'Categorie6',
                'Class_07': 'Categorie7',
                'Class_08': 'Categorie8',
                'Class_09': 'Categorie9',
                'Class_10': 'Categorie10',
                'Code': 'Artikelcode',
                'CostPriceCurrency': 'Valuta',
                'CostPriceNew': 'NieuweKostprijs',
                'CostPriceStandard': 'StandaardKostprijs',
                'AverageCost': 'GemiddeldeKostprijs',
                'Created': 'AangemaaktOp',
                'Description': 'Artikelomschrijving',
                'Division': 'AdministratieCode',
                'EndDate': 'Einddatum',
                'ExtraDescription': 'ExtraOmschrijving',
                'FreeBoolField_01': 'VrijBooleanVeld1',
                'FreeBoolField_02': 'VrijBooleanVeld2',
                'FreeBoolField_03': 'VrijBooleanVeld3',
                'FreeBoolField_04': 'VrijBooleanVeld4',
                'FreeBoolField_05': 'VrijBooleanVeld5',
                'FreeDateField_01': 'VrijDatumVeld1',
                'FreeDateField_02': 'VrijDatumVeld2',
                'FreeDateField_03': 'VrijDatumVeld3',
                'FreeDateField_04': 'VrijDatumVeld4',
                'FreeDateField_05': 'VrijDatumVeld5',
                'FreeNumberField_01': 'VrijNummerVeld1',
                'FreeNumberField_02': 'VrijNummerVeld2',
                'FreeNumberField_03': 'VrijNummerVeld3',
                'FreeNumberField_04': 'VrijNummerVeld4',
                'FreeNumberField_05': 'VrijNummerVeld5',
                'FreeNumberField_06': 'VrijNummerVeld6',
                'FreeNumberField_07': 'VrijNummerVeld7',
                'FreeNumberField_08': 'VrijNummerVeld8',
                'FreeTextField_01': 'VrijTekstVeld1',
                'FreeTextField_02': 'VrijTekstVeld2',
                'FreeTextField_03': 'VrijTekstVeld3',
                'FreeTextField_04': 'VrijTekstVeld4',
                'FreeTextField_05': 'VrijTekstVeld5',
                'FreeTextField_06': 'VrijTekstVeld6',
                'FreeTextField_07': 'VrijTekstVeld7',
                'FreeTextField_08': 'VrijTekstVeld8',
                'FreeTextField_09': 'VrijTekstVeld9',
                'FreeTextField_10': 'VrijTekstVeld10',
                'ItemGroup': 'Artikelgroep',
                'IsMakeItem': 'IsMaakartikel',
                'IsNewContract': 'IsNieuwContract',
                'IsOnDemandItem': 'IsOpAanvraagArtikel',
                'IsPackageItem': 'IsVerpakkingArtikel',
                'IsPurchaseItem': 'IsInkoopArtikel',
                'IsSalesItem': 'IsVerkoopArtikel',
                'IsSerialItem': 'IsSerieArtikel',
                'IsStockItem': 'IsVoorraadArtikel',
                'IsSubcontractedItem': 'IsOnderaannemerArtikel',
                'IsTaxableItem': 'IsBelastingArtikel',
                'IsTime': 'IsTijdArtikel',
                'IsWebshopItem': 'IsWebshopArtikel',
                'GrossWeight': 'BrutoGewicht',
                'NetWeight': 'NettoGewicht',
                'NetWeightUnit': 'NettoGewichtEenheid',
                'Notes': 'Notities',
                'SalesVatCode': 'BTWcode',
                'SalesVatCodeDescription': 'BTWcodeBeschrijving',
                'SecurityLevel': 'Beveiligingsniveau',
                'StartDate': 'Startdatum',
                'StatisticalCode': 'GoederenCode',
                'Unit': 'Eenheid',
                'UnitDescription': 'EenheidBeschrijving',
                'UnitType': 'EenheidType'
            },
            "ArtikelenExtraVelden": {
                'ItemID': 'ArtikelID',
                'Modified': 'GewijzigdOp',
                'Number': 'Nummer',
                'Description': 'Omschrijving',
                'Value': 'Waarde',
                'Division': 'AdministratieCode'
            },
            "ArtikelGroepen": {
                'ID': 'ID',
                'Code': 'Code',
                'Description': 'Omschrijving',
                'Division': 'AdministratieCode'
            },
            "Verkoopfacturen": {
                'Currency': 'F_Valuta',
                'DeliverTo': 'F_VersturenNaarID',
                'Invoice_Description': 'F_Omschrijving',
                'Division': 'F_AdministratieCode',
                'InvoiceDate': 'F_Factuurdatum',
                'InvoiceID': 'F_FactuurID',
                'InvoiceNumber': 'F_Factuurnummer',
                'InvoiceTo': 'F_FactuurNaarID',
                'OrderDate': 'F_Orderdatum',
                'OrderedBy': 'F_BesteldDoorID',
                'PaymentConditionDescription': 'F_BetaalconditieBeschrijving',
                'Remarks': 'F_Opmerkingen',
                'ShippingMethodDescription': 'F_VerzendmethodeBeschrijving',
                'StatusDescription': 'F_StatusBeschrijving',
                'YourRef': 'F_UwReferentie',
                'StarterSalesInvoiceStatusDescription': 'F_StartVerkoopfactuurStatus',
                'AmountDC': 'FR_Bedrag',
                'CostCenterDescription': 'FR_Kostenplaats',
                'CostUnitDescription': 'FR_Kostendrager',
                'DeliveryDate': 'FR_Leverdatum',
                'Description': 'FR_Omschrijving',
                'Discount': 'FR_Korting',
                'EmployeeFullName': 'FR_WerknemerNaam',
                'GLAccount': 'FR_GrootboekrekeningID',
                'ID': 'FR_FactuurregelID',
                'Item': 'FR_ArtikelID',
                'LineNumber': 'FR_Regelnummer',
                'Quantity': 'FR_Aantal',
                'SalesOrder': 'FR_VerkooporderID',
                'SalesOrderLine': 'FR_VerkooporderRegelID',
                'UnitDescription': 'FR_EenheidBeschrijving',
                'UnitPrice': 'FR_Eenheidsprijs',
                'VATAmountDC': 'FR_BTWBedrag',
                'VATPercentage': 'FR_BTWPercentage'
            },
            "VerkoopOrders": {
                'ApprovalStatusDescription': 'O_ApprovalStatusBeschrijving',
                'Approved': 'O_Goedgekeurd',
                'ApproverFullName': 'O_GoedgekeurdDoorNaam',
                'Created': 'O_Aangemaakt',
                'CreatorFullName': 'O_AangemaaktDoorNaam',
                'Currency': 'O_Valuta',
                'DeliverTo': 'O_VersturenNaarID',
                'Order_Description': 'O_Omschrijving',
                'Division': 'O_AdministratieCode',
                'InvoiceStatusDescription': 'O_FactuurStatusBeschrijving',
                'InvoiceTo': 'O_FactuurNaarID',
                'OrderDate': 'O_Orderdatum',
                'OrderedBy': 'O_BesteldDoorNaam',
                'OrderID': 'O_OrderID',
                'OrderNumber': 'O_Ordernummer',
                'Remarks': 'O_Opmerkingen',
                'ShippingMethodDescription': 'O_VerzendmethodeBeschrijving',
                'StatusDescription': 'O_StatusBeschrijving',
                'YourRef': 'O_UwReferentie',
                'AmountDC': 'OR_Bedrag',
                'CostCenterDescription': 'OR_Kostenplaats',
                'CostPriceFC': 'OR_Kostprijs',
                'CostUnitDescription': 'OR_Kostendrager',
                'DeliveryDate': 'OR_Leverdatum',
                'Description': 'OR_Omschrijving',
                'Discount': 'OR_Korting',
                'ID': 'OR_OrderRegelID',
                'Item': 'OR_ArtikelID',
                'LineNumber': 'OR_Regelnummer',
                'Quantity': 'OR_Aantal',
                'VATAmount': 'OR_BTWBedrag',
                'VATPercentage': 'OR_BTWPercentage'
            },
            "Verkoopkansen": {
                'ID': 'VerkoopkansID',
                'Account': 'AccountID',
                'ActionDate': 'Actiedatum',
                'AmountDC': 'Bedrag',
                'CloseDate': 'Sluitingsdatum',
                'Created': 'Aangemaakt',
                'CreatorFullName': 'AangemaaktDoorNaam',
                'Currency': 'Valuta',
                'Division': 'AdministratieCode',
                'Name': 'Naam',
                'OpportunityStageDescription': 'FaseBeschrijving',
                'OpportunityStatus': 'Status',
                'Probability': 'Kans',
                'OwnerFullName': 'EigenaarNaam',
                'SalesTypeDescription': 'VerkooptypeBeschrijving',
                'ReasonCodeDescription': 'RedenBeschrijving',
                'CampaignDescription': 'CampagneBeschrijving',
                'LeadSourceDescription': 'LeadBronBeschrijving',
                'ContactFullName': 'ContactpersoonNaam'
            },
            "Offertes": {
                'Division': 'O_AdministratieCode',
                'QuotationID': 'O_OfferteID',
                'QuotationNumber': 'O_Offertenummer',
                'VersionNumber': 'O_Versie',
                'ID': 'OR_OfferteRegelID',
                'SalesPersonFullName': 'O_Medewerker',
                'Currency': 'O_Valuta',
                'Quotation_Description': 'O_Omschrijving',
                'StatusDescription': 'O_Status',
                'OrderAccount': 'O_RelatieID',
                'Opportunity': 'O_VerkoopKansID',
                'QuotationDate': 'O_OfferteDatum',
                'ClosingDate': 'O_VervalDatum',
                'CloseDate': 'O_EindDatum',
                'DeliveryDate': 'O_AfleverDatum',
                'Remarks': 'O_Opmerkingen',
                'YourRef': 'O_UwReferentie',
                'AmountDC': 'OR_Bedrag',
                'CostCenterDescription': 'OR_Kostenplaats',
                'CostUnitDescription': 'OR_Kostendrager',
                'Discount': 'OR_Korting',
                'Item': 'OR_ArtikelID',
                'LineNumber': 'OR_Regelnummer',
                'Quantity': 'OR_Aantal',
                'UnitDescription': 'OR_Eenheid',
                'UnitPrice': 'OR_PrijsPerEenheid',
                'VATAmountFC': 'OR_BTWBedrag',
                'VATPercentage': 'OR_BTWPercentage',
                'Description': 'OR_Omschrijving',
            },
            "Divisions": {
                'Division': 'Division',
                'Entiteit': 'Entiteit',
                'CustomerCode': 'CustomerCode',
                'CustomerName': 'CustomerName',
                'State': 'State',
                'City': 'City',
                'HID': 'HID',
                'CompanySizeDescription': 'CompanySizeDescription'
            }
        }
        # Voeg alle bestaande mappings toe aan self.mappings

    def get_mapping(self, table_name):
        """Haal de kolom mapping op voor een specifieke tabel.
        
        Args:
            table_name: Naam van de tabel waarvoor mapping nodig is
            
        Returns:
            Dict met kolom mappings of None als tabel niet bestaat
        """
        return self.mappings.get(table_name)

def transform_columns(df, column_mapping, division_code):
    """Transform kolommen van een DataFrame volgens de gegeven mapping.
    
    Args:
        df: Input DataFrame
        column_mapping: Dictionary met oude->nieuwe kolomnamen
        division_code: Divisie code die gebruikt moet worden
        
    Returns:
        Getransformeerde DataFrame
    
    Raises:
        ValueError: Als verplichte kolommen ontbreken
    """
    try:
        # Voor lege DataFrames
        if df.empty:
            logging.info("Lege DataFrame ontvangen, retourneer lege DataFrame met correcte kolommen")
            return pd.DataFrame(columns=list(column_mapping.values()))

        # Voeg division toe als het ontbreekt
        if 'Division' not in df.columns:
            df['Division'] = division_code
            logging.debug(f"Division kolom toegevoegd met waarde: {division_code}")

        # Hernoem kolommen
        df = df.rename(columns=column_mapping)
        
        # Controleer en herorden kolommen
        new_columns = list(column_mapping.values())
        missing_columns = [col for col in new_columns if col not in df.columns]
        
        if missing_columns:
            error_msg = f"Ontbrekende kolommen in DataFrame: {', '.join(missing_columns)}"
            logging.error(error_msg)
            raise ValueError(error_msg)

        return df

    except Exception as e:
        logging.error(f"Fout bij transformeren van kolommen: {str(e)}")
        raise

def apply_column_mapping(df, table_name, division_code, config=None):
    """Pas kolom mapping toe op een DataFrame.
    
    Args:
        df: Input DataFrame
        table_name: Naam van de tabel
        division_code: Divisie code
        config: Optionele mapping configuratie
        
    Returns:
        Getransformeerde DataFrame of None bij fouten
    """
    try:
        if config is None:
            config = ColumnMappingConfig()
        
        logging.info(f"Start kolom mapping voor tabel: {table_name}")
        
        column_mapping = config.get_mapping(table_name)
        if column_mapping is None:
            logging.error(f"Geen kolom mapping gevonden voor tabel: {table_name}")
            return None

        df_transformed = transform_columns(df, column_mapping, division_code)
        
        logging.info(f"Kolom mapping succesvol toegepast voor tabel: {table_name}")
        return df_transformed

    except Exception as e:
        logging.error(f"Fout bij toepassen kolom mapping: {str(e)}")
        return None