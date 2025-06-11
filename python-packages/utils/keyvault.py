import json
from notebookutils import mssparkutils

class KeyVaultError(RuntimeError):
    pass

class SecretManager:
    KEYVAULT_URL = 'https://finnitkeyvault.vault.azure.net/'

    def __init__(self, klantnaam, doeldatabasevorm, applicatienaam, klantid):
        self.klantnaam = klantnaam
        self.doeldatabasevorm = doeldatabasevorm.upper()
        self.applicatienaam = applicatienaam
        self.klantid = klantid

    def _get_secret(self, secret_name):
        try:
            return mssparkutils.credentials.getSecret(self.KEYVAULT_URL, secret_name)
        except Exception as e:
            raise KeyVaultError(f"Secret '{secret_name}' kon niet worden opgehaald: {e}")

    def _get_database_for_klant(self):
        secret_name = "DatabasesKlanten"
        database_dict_str = self._get_secret(secret_name)
        try:
            database_dict = json.loads(database_dict_str)
        except json.JSONDecodeError as e:
            raise KeyVaultError(f"Secret '{secret_name}' kon niet als JSON worden geïnterpreteerd: {e}")

        for db_name, db_info in database_dict.get("Databases", {}).items():
            klanten = db_info.get("Klanten", [])
            if self.klantnaam in klanten:
                return db_name

        raise KeyVaultError(f"Klantnaam '{self.klantnaam}' niet gevonden in database dictionary.")

    def _get_connection_string(self):
        secret_name = f"Connectiestring{self.applicatienaam}{self.klantnaam}"
        return self._get_secret(secret_name)

    def _get_mei_credentials(self, database):
        tenant_id = self._get_secret(f"TenantID{database}")
        client_id = self._get_secret(f"ClientID{database}")
        client_secret = self._get_secret(f"ClientSecret{database}")
        return {
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret
        }
        
    def _get_base_url(self):
        secret_name = f"APIBaseURL{self.applicatienaam}{self.klantnaam}"
        return self._get_secret(secret_name)
    
    def _get_bearer_token(self):
        secret_name = f"BearerToken{self.applicatienaam}{self.klantnaam}"
        return self._get_secret(secret_name)

    def secrets_ophalen(self):
        result = {}

        result['connection_string'] = self._get_connection_string()
        database = self._get_database_for_klant()
        result['database'] = database
        result['base_url'] = self._get_base_url()
        result['bearer_token'] = self._get_bearer_token()

        if self.doeldatabasevorm == 'AZURESQLMEI':
            mei_creds = self._get_mei_credentials(database)
            result.update(mei_creds)

        result.update({
            'klantnaam': self.klantnaam,
            'doeldatabasevorm': self.doeldatabasevorm,
            'applicatienaam': self.applicatienaam,
            'klantid': self.klantid
        })

        return result
