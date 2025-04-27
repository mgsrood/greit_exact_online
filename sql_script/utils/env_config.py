import os
from dotenv import load_dotenv

class EnvConfig:
    """
    Klasse voor het beheren van environment variabelen met validatie.
    """
    # Verplichte variabelen per authenticatiemethode
    REQUIRED_VARS = {
        "SQL": [
            "DB_DRIVER",
            "DB_SERVER",
            "DB_DATABASE",
            "DB_AUTH_METHOD",
            "DB_USERNAME",
            "DB_PASSWORD",
            "KLANT_NAAM"
        ],
        "MEI": [
            "DB_DRIVER",
            "DB_SERVER",
            "DB_DATABASE",
            "DB_AUTH_METHOD",
            "DB_TENANT_ID",
            "DB_CLIENT_ID",
            "DB_CLIENT_SECRET",
            "KLANT_NAAM"
        ]
    }

    def __init__(self, env_file=None):
        """
        Initialiseer de environment configuratie.
        
        Args:
            env_file: Optioneel pad naar .env bestand. Als None, wordt .env in de huidige directory gebruikt.
        """
        # Laad environment variabelen
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()

    def build_connection_string(self, env_vars):
        """
        Bouw de connection string op basis van de authenticatiemethode.
        """
        base_connection = (
            f"DRIVER={env_vars['DB_DRIVER']};"
            f"SERVER={env_vars['DB_SERVER']};"
            f"DATABASE={env_vars['DB_DATABASE']};"
        )

        if env_vars["DB_AUTH_METHOD"].upper() == "SQL":
            connection_string = (
                f"{base_connection}"
                f"UID={env_vars['DB_USERNAME']};"
                f"PWD={env_vars['DB_PASSWORD']};"
            )
        else:  # MEI
            connection_string = base_connection

        # Voeg algemene opties toe
        connection_string += "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"

        return connection_string

    def validate_environment(self):
        """
        Valideer de environment variabelen en retourneer een dictionary met de waarden.
        
        Returns:
            Dictionary met de gevalideerde environment variabelen
            
        Raises:
            ValueError: Als verplichte variabelen ontbreken
        """
        # Haal de authenticatiemethode op
        auth_method = os.getenv("DB_AUTH_METHOD", "SQL").upper()
        if auth_method not in ["SQL", "MEI"]:
            raise ValueError(f"Ongeldige authenticatiemethode: {auth_method}. Moet 'SQL' of 'MEI' zijn.")

        # Controleer verplichte variabelen
        missing_vars = []
        env_vars = {}

        for var in self.REQUIRED_VARS[auth_method]:
            value = os.getenv(var)
            if not value:
                missing_vars.append(var)
            else:
                env_vars[var] = value

        if missing_vars:
            raise ValueError(
                f"Ontbrekende verplichte environment variabelen voor {auth_method} authenticatie: "
                f"{', '.join(missing_vars)}"
            )

        return env_vars

    def get_database_config(self):
        """
        Haal de database configuratie op uit de environment variabelen.
        
        Returns:
            Dictionary met database configuratie parameters
        """
        env_vars = self.validate_environment()
        connection_string = self.build_connection_string(env_vars)
        
        config = {
            "connection_string": connection_string,
            "auth_method": env_vars["DB_AUTH_METHOD"],
            "klant_naam": env_vars["KLANT_NAAM"],
            "script_name": os.getenv("SCRIPT_NAME")
        }

        # Voeg MEI-specifieke parameters toe indien nodig
        if env_vars["DB_AUTH_METHOD"].upper() == "MEI":
            config.update({
                "tenant_id": env_vars["DB_TENANT_ID"],
                "client_id": env_vars["DB_CLIENT_ID"],
                "client_secret": env_vars["DB_CLIENT_SECRET"]
            })

        return config

def setup_environment(env_file=None):
    """
    Helper functie om de environment configuratie op te zetten.
    
    Args:
        env_file: Optioneel pad naar .env bestand
        
    Returns:
        Dictionary met database configuratie parameters
    """
    config = EnvConfig(env_file)
    return config.get_database_config() 