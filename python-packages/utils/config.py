import logging

class ConfigManager:
    def __init__(self, spark, klant=None, applicatie_naam=None):
        """
        Args:
            spark: SparkSession object
            klant: klantnaam (str)
            applicatie_naam: applicatienaam (str)
        """
        self.spark = spark
        self.klant = klant
        self.applicatie_naam = applicatie_naam
        self.logger = logging.getLogger("ConfigManager")
    
    def load_administraties(self):
        """
        Laad actieve administraties voor de klant uit Delta tabel.
        """
        try:
            df_admin = self.spark.read.format("delta").load("Files/Delta/Administratie")
            df_admin = df_admin.filter((col("KlantID") == self.klant) & (col("Actief") == True))
            return df_admin
        except Exception as e:
            self.logger.error(f"Fout bij laden administraties: {e}")
            return None
    
    def load_tabellen_applicatie(self):
        """
        Laad tabellen van de applicatie uit Delta tabel.
        """
        try:
            df_tabellen = self.spark.read.format("delta").load("Files/Delta/Tabellen")
            df_tabellen = df_tabellen.filter(col("ApplicatieNaam") == self.applicatie_naam)
            return df_tabellen
        except Exception as e:
            self.logger.error(f"Fout bij laden tabellen applicatie: {e}")
            return None
    
    def load_tabel_mapping(self, tabel_id):
        """
        Laad tabel mapping voor gegeven tabel ID.
        """
        try:
            df_mapping = self.spark.read.format("delta").load("Files/Delta/TabellenMapping")
            df_mapping = df_mapping.filter(col("TabelID") == tabel_id)
            return df_mapping
        except Exception as e:
            self.logger.error(f"Fout bij laden tabel mapping: {e}")
            return None
    
    def load_tabel_admin_koppeling(self, administratie_id):
        """
        Laad actieve tabel admin koppelingen voor een administratie.
        """
        try:
            df_koppeling = self.spark.read.format("delta").load("Files/Delta/TabelAdminKoppeling")
            df_koppeling = df_koppeling.filter((col("AdministratieID") == administratie_id) & (col("Actief") == True))
            return df_koppeling
        except Exception as e:
            self.logger.error(f"Fout bij laden tabel admin koppelingen: {e}")
            return None
    
    def get_secret(self, keyvault_url, secret_name):
        """
        Haal een secret op uit Azure Key Vault via mssparkutils.credentials.
        
        Args:
            keyvault_url: URL van Key Vault (string)
            secret_name: Naam van de secret (string)
        """
        try:
            from mssparkutils import credentials
            secret_value = credentials.getSecret(keyvault_url, secret_name)
            return secret_value
        except Exception as e:
            self.logger.error(f"Fout bij ophalen secret '{secret_name}': {e}")
            return None
    
    def get_endpoint(self, df_applicatie_tabellen, tabel_naam):
        """
        Haal endpoint op voor een tabel uit df_applicatie_tabellen.
        """
        endpoint_row = df_applicatie_tabellen.filter(col("TabelNaam") == tabel_naam).select("Endpoint").collect()
        if endpoint_row:
            return endpoint_row[0]["Endpoint"]
        else:
            self.logger.warning(f"Geen endpoint gevonden voor tabelnaam {tabel_naam}")
            return None