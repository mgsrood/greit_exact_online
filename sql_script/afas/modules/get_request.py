from datetime import datetime
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
            "Forecasts": f"Finnit_Forecasts?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
            "Divisions": "Finnit_Divisions",
            "GrootboekMutaties": f"Finnit_Grootboekmutaties?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
            "Grootboekrekening": "Finnit_Grootboekrekening",
            "GrootboekRubriek": "Finnit_GrootboekRubriek",
            "Budget": "Finnit_Budget",
            "Projecten": "Finnit_Projecten",
            "Abonnementen": f"Finnit_Abonnementen",
            "Relaties": "Finnit_Relaties",
            "Nacalculatie": f"Finnit_Nacalculatie?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
            "BudgetProjecten": "Finnit_BudgetProjecten",
            "Urenregistratie": "Finnit_Urenregistratie",
            "Verlof": "Finnit_Verlof",
            "VerzuimUren": "Finnit_VerzuimUren",
            "VerzuimVerloop": "Finnit_VerzuimVerloop",
            "Medewerkers": "Finnit_Medewerkers",
            "Contracten": "Finnit_Contracten",
            "CaseLogging": f"Finnit_CaseLogging?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
            "Dossiers": f"Finnit_Dossiers?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
            "Forecasts": f"Finnit_Forecasts?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
            "Roosters": f"Finnit_Roosters?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2"
            }

    def _full_connectors(self):
        """Retourneert de AFAS connectors met eventuele filters."""
        
        last_year = datetime.now().year - 1
        
        return {
            "GrootboekMutaties": f"Finnit_Grootboekmutaties?filterfieldids=Boekjaar&filtervalues={last_year}&operatortypes=2",
        }
        
    def return_connectors(self, laatste_sync):
        if self.script_name == "Volledig":
            logging.info("Volledige sync wordt uitgevoerd")
            return self._full_connectors()
        else:
            huidige_datum = datetime.now()
            laatste_sync_datum = datetime.strptime(laatste_sync, "%Y-%m-%dT%H:%M:%S")
            verschil_in_jaren = (huidige_datum - laatste_sync_datum).days / 365

            is_reset_sync = verschil_in_jaren > 2
            logging.info(f"{'Volledige' if is_reset_sync else 'Reguliere'} sync wordt uitgevoerd")
            
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
        logging.info(f"Request URL: {url}")
        logging.info(f"Request params: {params}")
        response = requests.get(url, headers=headers, params=params)
        logging.info(f"Response status code: {response.status_code}")
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
        if hasattr(e, 'response') and e.response is not None:
            logging.error(f"Response content: {e.response.text}")
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