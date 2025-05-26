from datetime import timedelta, datetime
import logging
import pyodbc
import time
import sys
from .database_connection import connect_to_database

class DatabaseLogHandler(logging.Handler):
    """
    Logging handler die direct naar de database schrijft voor start, eind en error logs.
    """
    def __init__(self, conn_str, customer, script, script_id, auth_method="SQL", tenant_id=None, client_id=None, client_secret=None):
        super().__init__()
        self.conn_str = conn_str
        self.customer = customer
        self.script = script
        self.script_id = script_id
        self.auth_method = auth_method
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.administratiecode = None
        self.tabel = None

    def set_context(self, administratiecode=None, tabel=None):
        """
        Stel de context in voor de volgende log entry.
        """
        self.administratiecode = administratiecode
        self.tabel = tabel

    def emit(self, record):
        try:
            log_message = self.format(record)
            log_message = log_message.split('-')[-1].strip()
            created_at = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')
            log_level = record.levelname
            
            # Gebruik de connect_to_database functie met de juiste authenticatie
            with connect_to_database(
                self.conn_str,
                self.auth_method,
                self.tenant_id,
                self.client_id,
                self.client_secret
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """INSERT INTO Logging 
                           (Klantnaam, Actie, Datumtijd, Administratiecode, Tabel, Script, ScriptID, Niveau) 
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (self.customer, log_message, created_at, 
                         self.administratiecode, self.tabel, self.script, self.script_id, log_level)
                    )
                    conn.commit()
        except Exception as e:
            print(f"Fout bij schrijven naar database: {e}")

class CustomFormatter(logging.Formatter):
    """
    Custom formatter die extra informatie toevoegt aan het logbericht.
    """
    def __init__(self, script, script_id):
        super().__init__()
        self.script = script
        self.script_id = script_id

    def format(self, record):
        # Voeg script en script_id toe aan het record
        record.script = self.script
        record.script_id = self.script_id
        return super().format(record)

def setup_logging(conn_str, klant, script, script_id, auth_method="SQL", tenant_id=None, client_id=None, client_secret=None):
    """
    Configureer logging met database logging voor start, eind en errors,
    en terminal logging voor alle berichten tijdens het testen.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Maak en configureer de database handler
    db_handler = DatabaseLogHandler(
        conn_str=conn_str,
        customer=klant,
        script=script,
        script_id=script_id,
        auth_method=auth_method,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', 
                                datefmt='%Y-%m-%d %H:%M:%S')
    db_handler.setFormatter(formatter)
    logger.addHandler(db_handler)

    # Voeg terminal logging toe voor testen
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return db_handler

def start_log():
    """Log de start van een script uitvoering"""
    start_time = time.time()
    current_time = datetime.now()
    formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
    logging.info(f"Script gestart")
    return start_time

def end_log(start_time):
    """Log de eindtijd en duratie van een script uitvoering"""
    end_time = time.time()
    total_time = timedelta(seconds=(end_time - start_time))
    total_time_str = str(total_time).split('.')[0]
    logging.info(f"Script volledig afgerond in {total_time_str}")

def set_logging_context(administratiecode=None, tabel=None):
    """
    Stel de context in voor de volgende log entry.
    
    Args:
        administratiecode: De administratiecode voor de log entry
        tabel: De tabelnaam voor de log entry
    """
    # Zoek de DatabaseLogHandler in de handlers
    for handler in logging.getLogger().handlers:
        if isinstance(handler, DatabaseLogHandler):
            handler.set_context(administratiecode=administratiecode, tabel=tabel)
            break