from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from zoneinfo import ZoneInfo
import logging
import time
import sys

LOG_TABLE_NAME = "logging"

class TimezoneFormatter(logging.Formatter):
    """
    Custom formatter die de logtijd converteert naar een specifieke tijdzone.
    """
    def __init__(self, fmt, datefmt=None, tz_name="Europe/Amsterdam"):
        super().__init__(fmt, datefmt=datefmt)
        self.tz = ZoneInfo(tz_name)

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=self.tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()

class LakehouseLogHandler(logging.Handler):
    """
    Logging handler die direct naar de database schrijft voor start, eind en error logs.
    """
    def __init__(self, customer, script, script_id, lakehouse_table_name=LOG_TABLE_NAME):
        super().__init__()
        self.lakehouse_table_name = lakehouse_table_name
        self.customer = customer
        self.script = script
        self.script_id = script_id
        self.administratiecode = None
        self.administratienaam = None
        self.tabel = None
        self.applicatienaam = None
        self.klantid = None

    def set_context(self, applicatienaam=None, klantid=None, administratiecode=None, administratienaam=None, tabel=None):
        """
        Stel de context in voor de volgende log entry.
        """
        self.administratiecode = administratiecode
        self.administratienaam = administratienaam
        self.tabel = tabel
        self.applicatienaam = applicatienaam
        self.klantid = klantid

    def _write_to_lakehouse(self, log_entry_data):
        """
        Schrijft een enkele log entry naar de gespecificeerde Lakehouse tabel.

        Args:
            log_entry_data: Een lijst met een enkele tuple die de log data bevat.
        """
        try:
            spark = SparkSession.builder.getOrCreate()

            # Schema exact laten overeenkomen met de doeltabel in het Lakehouse
            schema = StructType([
                StructField("KlantID", IntegerType(), True),
                StructField("KlantNaam", StringType(), True),
                StructField("AdministratieID", IntegerType(), True),
                StructField("AdministratieNaam", StringType(), True),
                StructField("ApplicatieNaam", StringType(), True),
                StructField("Tabel", StringType(), True),
                StructField("DatumTijd", TimestampType(), True), # Nullable=True voor veiligheid
                StructField("Actie", StringType(), True),
                StructField("TypeActie", StringType(), True),
                StructField("ScriptType", StringType(), True),
                StructField("ScriptID", IntegerType(), True)
            ])
            
            log_df = spark.createDataFrame(log_entry_data, schema=schema)

            log_df.write.format("delta").mode("append").saveAsTable(self.lakehouse_table_name)

        except Exception as e:
            print(f"Fout bij schrijven naar Lakehouse: {e}")
            print(f"Log data: {log_entry_data}")

    def emit(self, record):
        try:
            log_message = self.format(record)
            log_message = log_message.split(' - ')[-1].strip()
            created_at_dt = datetime.fromtimestamp(record.created, tz=ZoneInfo("Europe/Amsterdam"))
            log_level = record.levelname # INFO, WARNING, ERROR etc.

            # Loggin entry data
            log_entry = [(
                self.klantid,                       # KlantID
                self.customer,              # KlantNaam
                self.administratiecode,     # AdministratieID
                self.administratienaam,     # AdministratieNaam
                self.applicatienaam,        # ApplicatieNaam
                self.tabel,                 # Tabel
                created_at_dt,              # DatumTijd
                log_message,                # Actie
                log_level,                  # TypeActie
                self.script,                # ScriptType
                self.script_id              # ScriptID
            )]

            self._write_to_lakehouse(log_entry)
        except Exception:
            self.handleError(record)

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

def setup_logging(klant, script, script_id=None, lakehouse_table_name=LOG_TABLE_NAME):
    """
    Configureer logging met Lakehouse logging en terminal logging.
    
    Als een script_id wordt meegegeven, wordt dat ID gebruikt. 
    Anders wordt het hoogste ID uit de logtabel + 1 gebruikt.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Verwijder eventuele bestaande handlers om dubbele logs te voorkomen.
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # Als er geen script_id is meegegeven, bepaal het dan zelf.
    if script_id is None:
        try:
            spark = SparkSession.builder.getOrCreate()
            
            # Controleer of de tabel bestaat in de huidige Lakehouse-database
            if spark.catalog.tableExists(lakehouse_table_name):
                result_df = spark.sql(f"SELECT MAX(ScriptID) as max_id FROM {lakehouse_table_name}")
                max_id = result_df.collect()[0]['max_id']
                if max_id is not None:
                    script_id = max_id + 1
                else:
                    script_id = 1 # Eerste entry in de tabel
            else:
                script_id = 1 # Tabel bestaat nog niet

        except Exception as e:
            print(f"WAARSCHUWING: Kon geen nieuw ScriptID ophalen. Fout: {e}")
            script_id = 0 # Fallback
    
    # Maak en configureer de Lakehouse handler
    lakehouse_handler = LakehouseLogHandler(
        customer=klant,
        script=script,
        script_id=script_id,
        lakehouse_table_name=lakehouse_table_name
    )
    # Gebruik de TimezoneFormatter voor correcte tijdweergave in console logs
    formatter = TimezoneFormatter('%(asctime)s - %(levelname)s - %(message)s', 
                                datefmt='%Y-%m-%d %H:%M:%S')
    lakehouse_handler.setFormatter(formatter)
    logger.addHandler(lakehouse_handler)

    # Voeg terminal logging toe (handig voor debuggen in de notebook)
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return lakehouse_handler, script_id

def start_log():
    """Log de start van een script uitvoering"""
    start_time = time.time()
    logging.info(f"Script gestart")
    return start_time

def end_log(start_time):
    """Log de eindtijd en duratie van een script uitvoering"""
    end_time = time.time()
    total_time = timedelta(seconds=(end_time - start_time))
    total_time_str = str(total_time).split('.')[0]
    logging.info(f"Script volledig afgerond in {total_time_str}")

def set_logging_context(applicatienaam=None, klantid=None, administratiecode=None, administratienaam=None, tabel=None):
    """
    Stel de context in voor de volgende log entry.
    
    Args:
        applicatienaam: De applicatienaam voor de log entry
        klantid: De klantid voor de log entry
        administratiecode: De administratiecode voor de log entry
        administratienaam: De administratienaam voor de log entry
        tabel: De tabelnaam voor de log entry
    """
    # Zoek de LakehouseLogHandler in de handlers
    for handler in logging.getLogger().handlers:
        if isinstance(handler, LakehouseLogHandler):
            handler.set_context(applicatienaam, klantid, administratiecode, administratienaam, tabel)
            break