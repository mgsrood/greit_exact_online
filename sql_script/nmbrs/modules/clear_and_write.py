# Voeg de root van het project toe aan de Python path
import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
sys.path.append(project_root)

from greit_exact_online.sql_script.utils.database_connection import get_azure_sql_access_token
from greit_exact_online.sql_script.utils.env_config import EnvConfig
from sqlalchemy import create_engine, event, text
from dataclasses import dataclass
from datetime import datetime
import logging
import urllib
import time

@dataclass
class TableConfig:
    mode: str = None
    unique_columns: list = None
    filter_column: str = None

@dataclass
class TableConfigManager:
    configs = None

    def __post_init__(self):
        self.configs = {
            "Bedrijven": TableConfig(
                mode="truncate",
                unique_columns=["BedrijfID"],
                filter_column=""
            ),
            "Looncomponenten": TableConfig(
                mode="truncate",
                unique_columns=["BedrijfID", "WerknemerID", "LooncomponentID"],
                filter_column=["BedrijfID", "Jaar"]
            )
        }

def create_engine_with_auth(connection_string, auth_method="SQL", token_struct=None):
    """Maak een SQLAlchemy engine met de juiste authenticatie methode.
    
    Args:
        connection_string: De ODBC connection string
        auth_method: De authenticatie methode ("SQL" of "MEI")
        token_struct: Het token voor MEI authenticatie (alleen nodig bij MEI)
    
    Returns:
        Een SQLAlchemy engine met de juiste authenticatie configuratie
    """

    engine_url = f"mssql+pyodbc:///?odbc_connect={connection_string}"
    if auth_method.upper() == "MEI":
        if not token_struct:
            raise ValueError("Token is vereist voor MEI authenticatie")
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        connect_args = {'attrs_before': {SQL_COPT_SS_ACCESS_TOKEN: token_struct}}
        return create_engine(engine_url, connect_args=connect_args)
    elif auth_method.upper() == "SQL":
        return create_engine(engine_url)
    else:
        raise ValueError(f"Ongeldige authenticatie methode: {auth_method}. Gebruik 'SQL' of 'MEI'")

def clear_data(engine, table, config, company_id=None, jaar=None, laatste_sync=None):
    """Verwijder data uit een tabel op basis van de configuratie."""
    try:
        with engine.connect() as connection:
            # Controleer laatste_sync als die is meegegeven
            if laatste_sync:
                try:
                    huidige_datum = datetime.now()
                    laatste_sync_datum = datetime.strptime(laatste_sync, "%Y-%m-%dT%H:%M:%S")
                    verschil_in_dagen = (huidige_datum - laatste_sync_datum).days
                    verschil_in_jaren = verschil_in_dagen / 365.0
                    
                    if verschil_in_jaren > 1.2:
                        logging.info(f"Laatste sync is meer dan een jaar geleden, overschakelen naar selectieve delete voor {table} indien filters aanwezig, anders truncate")
                        params_sync = {}
                        where_clauses_sync = []
                        if company_id is not None:
                            where_clauses_sync.append("BedrijfID = :company_id")
                            params_sync["company_id"] = company_id
                        if jaar is not None and isinstance(config.filter_column, list) and "Jaar" in config.filter_column:
                             where_clauses_sync.append("Jaar = :jaar")
                             params_sync["jaar"] = jaar

                        if where_clauses_sync:
                            query_sync = f"DELETE FROM {table} WHERE {' AND '.join(where_clauses_sync)}"
                            result_sync = connection.execute(text(query_sync), params_sync)
                            rows_deleted = result_sync.rowcount
                            logging.info(f"Verwijderd {rows_deleted} rijen uit {table} (लाTSTE SYNC > 1.2 JAAR) gebaseerd op filter: {params_sync}")
                        else:
                            connection.execute(text(f"TRUNCATE TABLE {table}"))
                            logging.info(f"Tabel {table} succesvol getruncate (लाTSTE SYNC > 1.2 JAAR, geen specifieke filters). ")
                            rows_deleted = 0 # Assuming truncate doesn't return rowcount consistently
                        
                        connection.commit()
                        return rows_deleted
                except (ValueError, TypeError) as e:
                    logging.info(f"Kon laatste_sync niet verwerken: {e}. Gebruik standaard operatie.")
            
            # Standaard operatie op basis van mode
            if config.mode == 'truncate':
                params = {}
                where_clauses = []

                if company_id is not None:
                    if isinstance(config.filter_column, list) and "BedrijfID" in config.filter_column:
                        where_clauses.append("BedrijfID = :company_id")
                        params["company_id"] = company_id
                    elif config.filter_column == "BedrijfID" or (not isinstance(config.filter_column, list) and not config.filter_column):
                        where_clauses.append("BedrijfID = :company_id")
                        params["company_id"] = company_id
                
                if jaar is not None and isinstance(config.filter_column, list) and "Jaar" in config.filter_column:
                    where_clauses.append("Jaar = :jaar")
                    params["jaar"] = jaar

                if where_clauses:
                    query = f"DELETE FROM {table} WHERE {' AND '.join(where_clauses)}"
                    result = connection.execute(text(query), params)
                    rows_deleted = result.rowcount
                    logging.info(f"Verwijderd {rows_deleted} rijen uit {table} gebaseerd op filter: {params}")
                else:
                    connection.execute(text(f"TRUNCATE TABLE {table}"))
                    logging.info(f"Tabel {table} succesvol getruncate (geen specifieke filters toegepast).")
                    rows_deleted = 0 

                connection.commit()
                return rows_deleted
            else:
                logging.info(f"Geen actie ondernomen voor tabel {table} (mode: {config.mode})")
                return 0
    except Exception as e:
        logging.error(f"Fout bij het verwijderen van data uit {table}: {str(e)}")
        # Log de volledige stack trace voor meer details
        import traceback
        logging.error(f"Stack trace: {traceback.format_exc()}")
        raise

def write_data(engine, df, table, config, laatste_sync=None):
    """Schrijf data naar een tabel met de juiste configuratie."""
    try:
        logging.info(f"Start schrijven data naar tabel {table} met {len(df)} rijen")
        
        with engine.connect() as connection:
            temp_table_name = None
            try:
                if config.mode == 'truncate':
                    # Voor truncate mode, gebruik simpele insert
                    df.to_sql(table, engine, index=False, if_exists="append", schema="dbo")
                    logging.info(f"DataFrame succesvol toegevoegd aan de tabel: {table} ({len(df)} rijen)")
                    return
                    
                if config.mode == 'none' and laatste_sync:
                    try:
                        huidige_datum = datetime.now()
                        laatste_sync_datum = datetime.strptime(laatste_sync, "%Y-%m-%dT%H:%M:%S")
                        verschil_in_dagen = (huidige_datum - laatste_sync_datum).days
                        verschil_in_jaren = verschil_in_dagen / 365.0
                        
                        if verschil_in_jaren > 1.2:
                            logging.info(f"Laatste sync is meer dan een jaar geleden, overschakelen naar simpele insert voor {table}")
                            df.to_sql(table, engine, index=False, if_exists="append", schema="dbo")
                            logging.info(f"DataFrame succesvol toegevoegd aan de tabel: {table} ({len(df)} rijen)")
                            return
                    except (ValueError, TypeError) as e:
                        logging.info(f"Kon laatste_sync niet verwerken: {e}. Gebruik standaard MERGE operatie.")
                
                # Alleen voor mode 'none' met geldige laatste_sync, gebruik MERGE
                temp_table_name = f"temp_table_{int(time.time())}"
                logging.info(f"Maak tijdelijke tabel {temp_table_name} aan")
                df.to_sql(temp_table_name, engine, index=False, if_exists="replace", schema="dbo")
                
                # Bouw MERGE query
                on_clause = " AND ".join([f"target.{col} = source.{col}" for col in config.unique_columns])
                if config.filter_column:
                    on_clause += f" AND target.{config.filter_column} = source.{config.filter_column}"
                
                update_set = ", ".join([f"target.{col} = source.{col}" 
                                      for col in df.columns 
                                      if col not in config.unique_columns and 
                                      col != config.filter_column])
                
                merge_query = f"""
                MERGE {table} AS target
                USING (SELECT * FROM {temp_table_name}) AS source
                ON ({on_clause})
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(df.columns)})
                    VALUES ({', '.join([f'source.{col}' for col in df.columns])});
                """
                
                logging.info(f"Uitvoeren MERGE query voor tabel {table}")
                result = connection.execute(text(merge_query))
                connection.commit()
                
                logging.info(f"DataFrame succesvol toegevoegd/bijgewerkt in de tabel: {table} ({len(df)} rijen)")
                
            except Exception as e:
                connection.rollback()
                logging.error(f"Fout bij het toevoegen naar de database: {str(e)}")
                # Log de volledige stack trace voor meer details
                import traceback
                logging.error(f"Stack trace: {traceback.format_exc()}")
                raise
                
            finally:
                if temp_table_name:
                    try:
                        logging.info(f"Verwijderen tijdelijke tabel {temp_table_name}")
                        connection.execute(text(f"DROP TABLE {temp_table_name}"))
                        connection.commit()
                        logging.info(f"Tijdelijke tabel {temp_table_name} succesvol verwijderd.")
                    except Exception as e:
                        logging.error(f"Fout bij het verwijderen van de tijdelijke tabel {temp_table_name}: {str(e)}")
                        connection.rollback()
                        
    except Exception as e:
        logging.error(f"Fout bij het maken van database verbinding: {str(e)}")
        # Log de volledige stack trace voor meer details
        import traceback
        logging.error(f"Stack trace: {traceback.format_exc()}")
        raise

def apply_table_clearing(connection_string, table, company_id=None, jaar=None, laatste_sync=None, config_manager=None):
    """Pas tabel clearing toe met logging."""

    try:
        if config_manager is None:
            config_manager = TableConfigManager()
            
        config = config_manager.configs.get(table)
        if not config:
            logging.error(f"Geen configuratie gevonden voor tabel: {table}")
            return False
            
        logging.info(f"Start mogelijk verwijderen rijen of complete tabel: {table}")
        
        # Haal database configuratie op
        env_config = EnvConfig()
        db_config = env_config.get_database_config()
        
        # Genereer token struct voor MEI authenticatie
        token_struct = None
        if db_config["auth_method"].upper() == "MEI":
            token_struct = get_azure_sql_access_token(
                db_config["tenant_id"],
                db_config["client_id"],
                db_config["client_secret"]
            )

        engine = create_engine_with_auth(
            connection_string,
            db_config["auth_method"],
            token_struct
        )
        
        rows_deleted = clear_data(engine, table, config, company_id, jaar, laatste_sync)
        return rows_deleted > 0
    except Exception as e:
        logging.error(f"Fout bij het verwijderen van rijen of leegmaken van de tabel: {str(e)}")
        return False

def apply_table_writing(df, connection_string, table, laatste_sync=None, config_manager=None):
    """Pas tabel schrijven toe met logging."""
    try:
        if config_manager is None:
            config_manager = TableConfigManager()
            
        config = config_manager.configs.get(table)
        if not config:
            logging.error(f"Geen configuratie gevonden voor tabel: {table}")
            return False
            
        logging.info(f"Start toevoegen rijen naar database: {table}")
        
        # Haal database configuratie op
        env_config = EnvConfig()
        db_config = env_config.get_database_config()
        
        # Genereer token struct voor MEI authenticatie
        token_struct = None
        if db_config["auth_method"].upper() == "MEI":
            token_struct = get_azure_sql_access_token(
                db_config["tenant_id"],
                db_config["client_id"],
                db_config["client_secret"]
            )

        engine = create_engine_with_auth(
            connection_string,
            db_config["auth_method"],
            token_struct
        )
        
        write_data(engine, df, table, config, laatste_sync)
        return True
    except Exception as e:
        logging.error(f"Fout bij het toevoegen naar database: {str(e)}")
        return False