import pandas as pd
import requests
import logging

class SyncFormatManager:
    def __init__(self, config_manager):
        self.config = config_manager
        self.logger = config_manager.logger
        self.script_name = config_manager.script_name

    def _regular_connectors(self,laatste_sync):
        """Retourneert de AFAS connectors met eventuele filters."""
        return {
            "Divisions": "Finnit_Divisions",
            "GrootboekMutaties": f"Finnit_Grootboekmutaties?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
            "Grootboekrekening": "Finnit_Grootboekrekening",
            "GrootboekRubriek": "Finnit_GrootboekRubriek",
            "Budget": "Finnit_Budget",
            "BudgetProjecten": "Finnit_BudgetProjecten",
            "Urenregistratie": "Finnit_Urenregistratie",
            "Verlof": "Finnit_Verlof",
            "VerzuimUren": "Finnit_VerzuimUren",
            "VerzuimVerloop": "Finnit_VerzuimVerloop",
            "Medewerkers": "Finnit_Medewerkers",
            "Projecten": "Finnit_Projecten",
            "Relaties": "Finnit_Relaties",
            "Contracten": "Finnit_Contracten",
            "Abonnementen": "Finnit_Abonnementen?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
            "CaseLogging": "Finnit_CaseLogging?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
            "Dossiers": "Finnit_Dossiers?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
            "Forecasts": "Finnit_Forecasts?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
            "Nacalculatie": "Finnit_Nacalculatie?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
            "Roosters": "Finnit_Roosters?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2"
        }

    def _full_connectors(self, laatste_sync):
        """Retourneert de AFAS connectors met eventuele filters."""
        return {
            "GrootboekMutaties": f"Finnit_Grootboekmutaties?filterfieldids=Boekjaar&filtervalues={laatste_sync}&operatortypes=2",
        }
        
    def return_connectors(self, laatste_sync):
        if self.script_name == "Volledig":
            logging.info("Volledige sync wordt uitgevoerd")
            return self._full_connectors(laatste_sync)
        else:
            logging.info("Reguliere sync wordt uitgevoerd")
            return self._regular_connectors(laatste_sync)
        

def get_request(api_string, token, endpoint):
    """Voert een GET request uit naar de AFAS API en retourneert de data als DataFrame."""
    headers = {
        "Authorization": f"AfasToken {token}",
        "Content-Type": "application/json"
    }
    
    url = f"{api_string}{endpoint}"
    params = {"skip": -1, "take": -1}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        rows = data.get("rows", [])
        
        if not rows:
            return pd.DataFrame()
            
        df = pd.DataFrame(rows)
        logging.info(f"Totaal aantal rijen opgehaald: {len(rows)}")
        return df
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Fout bij het ophalen van data van AFAS: {str(e)}")
        return None

def execute_get_request(api_string, token, connector, klantnaam, table):
    """Voert een GET request uit en logt het resultaat."""
    logging.info(f"Start GET Request voor {table}")
    
    df = get_request(api_string, token, connector)
    
    if df is None:
        error_msg = f"FOUTMELDING | Fout bij het ophalen van data voor tabel: {table}"
        logging.error(f"{error_msg} | {klantnaam}")
        return None, True
        
    if df.empty:
        info_msg = f"Geen data opgehaald voor tabel: {table}"
        logging.info(f"{info_msg} | {klantnaam}")
        return df, False
        
    success_msg = f"Ophalen DataFrame gelukt voor tabel: {table}"
    logging.info(f"{success_msg} | {klantnaam}")
    return df, False