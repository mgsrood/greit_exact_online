from utils.database_connection import connect_to_database
from utils.log import setup_logging, start_log, end_log
from datetime import datetime
import pyodbc
import time
import logging
import os

class ConfigManager:
    def __init__(self, connection_string, auth_method="SQL", tenant_id=None, client_id=None, client_secret=None, script_name="Wijzigingen"):
        self.connection_string = connection_string
        self.auth_method = auth_method
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.logger = None
        self.klant = None
        self.script_name = script_name
        self.script_id = None

    def setup_logger(self, klant):
        """Configureer de logger voor deze instantie en haal automatisch de volgende script ID op"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT MAX(ScriptID) FROM Logging')
                    highest_script_id = cursor.fetchone()[0]
                    script_id = (highest_script_id or 0) + 1
                    
                    # Configureer de logger met de nieuwe script ID
                    self.logger = setup_logging(
                        conn_str=self.connection_string,
                        klant=klant,
                        script=self.script_name,
                        script_id=script_id,
                        auth_method=self.auth_method,
                        tenant_id=self.tenant_id,
                        client_id=self.client_id,
                        client_secret=self.client_secret
                    )
                    self.klant = klant
                    self.script_id = script_id
                    
                    return script_id
        except Exception as e:
            logging.error(f"Fout bij ophalen script ID: {e}")
            return None

    def get_connection(self, connection_string=None):
        """
        Maak een database connectie met de opgegeven connectiestring of de standaard connectiestring.
        
        Args:
            connection_string: Optionele alternatieve connectiestring. Als None, wordt de standaard gebruikt.
        """
        conn_str = connection_string if connection_string else self.connection_string
        return connect_to_database(
            conn_str,
            self.auth_method,
            self.tenant_id,
            self.client_id,
            self.client_secret
        )

    def get_connection_strings(self):
        """Haal alle connectiestrings op uit de database"""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT * FROM Klanten')
                    rows = cursor.fetchall()
                    connection_dict = {row[1]: (row[2], row[3], row[4]) for row in rows}
                    
                    logging.info(f"Configuratie dictionary opgehaald voor {self.klant}")
                    return connection_dict
        except Exception as e:
            logging.error(f"FOUTMELDING | Ophalen connectiestrings mislukt voor {self.klant}: {e}")
            return None

    def get_configurations(self, connection_string):
        """
        Haal alle configuraties op uit de database met de opgegeven connectiestring.
    
        Args:
            connection_string: De te gebruiken connectiestring voor deze configuratie-ophaling.
        """
        try:
            with self.get_connection(connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT * FROM Config')
                    rows = cursor.fetchall()
                    config_dict = {row[1]: row[2] for row in rows}
                    
                    logging.info(f"Ophalen configuratie gegevens gelukt voor {self.klant}")
                    return config_dict
        except Exception as e:
            logging.error(f"FOUTMELDING | Ophalen configuratie gegevens mislukt voor {self.klant}: {e}")
            return None

    def get_table_configurations(self, connection_string):
        """
        Haal tabel configuratie gegevens op uit de database.
        
        Args:
            connection_string: De te gebruiken connectiestring voor deze configuratie-ophaling.
        """
        logging.info(f"Ophalen tabel configuratie gegevens gestart voor {self.klant}")
        
        try:
            with self.get_connection(connection_string) as conn:
                with conn.cursor() as cursor:
                    table_config_dict = self.fetch_table_configurations(cursor)
                    if table_config_dict:
                        logging.info(f"Ophalen tabel configuratie gegevens gelukt voor {self.klant}")
                        return table_config_dict
                    
            logging.error(f"Ophalen tabel configuratie gegevens mislukt voor {self.klant}")
            return None
                    
        except Exception as e:
            logging.error(f"FOUTMELDING | Ophalen tabel configuratie gegevens mislukt voor {self.klant}: {e}")
            return None

    def fetch_table_configurations(self, cursor):
        """
        Haal tabel configuraties op uit de database.
        
        Args:
            cursor: Database cursor om de query mee uit te voeren.
        """
        cursor.execute('SELECT * FROM TabellenConfig')
        rows = cursor.fetchall()
        return {row[1]: row[2] for row in rows}

    def get_division_codes(self, connection_string):
        """Haal alle divisie codes op uit de database met retry logica"""
        logging.info(f"Ophalen divisiecodes gestart voor {self.klant}")
        
        max_retries = 3
        retry_delay = 5
        
        try:
            with self.get_connection(connection_string) as conn:
                with conn.cursor() as cursor:
                    division_dict = None
                    
                    for attempt in range(max_retries):
                        try:
                            cursor.execute('SELECT * FROM Divisions')
                            rows = cursor.fetchall()
                            division_dict = {entry[2]: (entry[1], entry[9], entry[10]) for entry in rows}
                            
                            if division_dict:
                                logging.info(f"Ophalen divisiecodes gelukt voor {self.klant}")
                                return division_dict
                        except Exception as e:
                            logging.info(f"Poging {attempt + 1} mislukt voor {self.klant}: {e}")
                            time.sleep(retry_delay)
                    
                    logging.error(f"Ophalen divisiecodes mislukt na {max_retries} pogingen voor {self.klant}")
                    return None
                    
        except Exception as e:
            logging.error(f"FOUTMELDING | Verbinding met divisie database mislukt voor {self.klant}: {e}")
            return None

    def update_last_sync(self, connection_string, laatste_sync):
        """Update de laatste synchronisatie timestamp"""
        try:
            with self.get_connection(connection_string) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('UPDATE Config SET Waarde = ? WHERE Config = ?', 
                                 (laatste_sync, 'Laatste_sync'))
                    conn.commit()
                    logging.info("Laatste sync succesvol bijgewerkt")
        except Exception as e:
            logging.error(f"Fout bij bijwerken laatste sync: {e}")

    def update_reporting_year(self, connection_string):
        """Update het rapportage jaar naar vorig jaar"""
        try:
            with self.get_connection(connection_string) as conn:
                with conn.cursor() as cursor:
                    current_year = datetime.now().year
                    last_year = current_year - 1
                    cursor.execute('UPDATE Config SET Waarde = ? WHERE Config = ?', 
                                 (last_year, 'ReportingYear'))
                    conn.commit()
                    logging.info("Reporting Year succesvol bijgewerkt")
        except Exception as e:
            logging.error(f"Fout bij bijwerken reporting year: {e}")

    def update_klant(self, nieuwe_klant):
        """
        Update de klantnaam voor logging doeleinden.
        
        Args:
            nieuwe_klant: De nieuwe klantnaam die gebruikt moet worden in de logging.
        """
        self.klant = nieuwe_klant
        if self.logger:
            self.logger.customer = nieuwe_klant
            
    def set_logging_context(self, administratiecode=None, tabel=None):
        """
        Stel de context in voor de volgende log entry.
        
        Args:
            administratiecode: De administratiecode voor de log entry
            tabel: De tabelnaam voor de log entry
        """
        if self.logger:
            self.logger.set_context(administratiecode=administratiecode, tabel=tabel)
            
    def fetch_environment_variables(self, cursor):
        """Haal omgevings configuratie gegevens op uit de database"""
        cursor.execute('SELECT * FROM Omgeving')
        rows = cursor.fetchall()
        return {row[1]: (row[0], row[2], row[3], row[4], row[5]) for row in rows}

    def create_environment_dict(self,connection_string):
        """
        Haal omgevings configuratie gegevens op uit de database.
        
        Args:
            connection_string: De te gebruiken connectiestring voor deze configuratie-ophaling.
        """
        logging.info(f"Ophalen omgevings configuratie gegevens gestart voor {self.klant}")
        
        # Verbinding opzetten
        try:
            with self.get_connection(connection_string) as conn:
                with conn.cursor() as cursor:
                    environ_dict = self.fetch_environment_variables(cursor)
                    if environ_dict:
                        logging.info(f"Ophalen omgevings configuratie gegevens gelukt voor {self.klant}")
                        return environ_dict
                    
            logging.error(f"Ophalen omgevings configuratie gegevens mislukt voor {self.klant}")
            return None
                    
        except Exception as e:
            logging.error(f"FOUTMELDING | Ophalen omgevings configuratie gegevens mislukt voor {self.klant}: {e}")
            return None