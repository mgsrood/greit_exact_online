from utils.env_config import setup_environment
from utils.log import start_log, end_log
from utils.config import ConfigManager
from exact.exact import exact
from nmbrs.nmbrs import nmbrs
from afas.afas import afas
import logging

# Laad environment configuratie
db_config = setup_environment()

# Configuratie manager initialiseren
config_manager = ConfigManager(
    connection_string=db_config["connection_string"],
    auth_method=db_config["auth_method"],
    tenant_id=db_config.get("tenant_id"),
    client_id=db_config.get("client_id"),
    client_secret=db_config.get("client_secret"),
    script_name=db_config.get("script_name")
)

# Logger setup - haalt automatisch de volgende script ID op
script_id = config_manager.setup_logger(
    klant=db_config["klant_naam"]
)
start_time = start_log()

# Starten script
try:
    # Connectie strings ophalen
    connection_dict = config_manager.get_connection_strings()
    
    for klant, (connection_string, type, applicatie) in connection_dict.items():

        # Alleen type 1 verwerken
        if type != 1:
            continue
        
        # Update de klantnaam voor logging
        config_manager.update_klant(klant)
            
        """# Alleen applicatie Exact verwerken
        if applicatie == "Exact":
            exact(connection_string, config_manager, klant)
        
        if applicatie == "AFAS" and klant == "Ternair":
            afas(connection_string, config_manager, klant)"""
        
        # Beuk            
        if applicatie == "Nmbrs":
            nmbrs(connection_string, config_manager, klant)
    
    # Einde script
    end_log(start_time)
    
except Exception as e:
    logging.error(f"Fout tijdens uitvoeren script: {e}")
    raise
