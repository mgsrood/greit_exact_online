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
    administration_column: str = None

@dataclass
class TableConfigManager:
    configs = None

    def __post_init__(self):
        self.configs = {
            "Grootboekrekening": TableConfig(
                mode="truncate",
                unique_columns=["OmgevingID", "GrootboekID"],
                administration_column="Administratie_Code"
            ),
            "GrootboekRubriek": TableConfig(
                mode="truncate",
                unique_columns=["OmgevingID", "Grootboek_RubriekID"],
                administration_column=""
            ),
            "GrootboekMutaties": TableConfig(
                mode="none",
                unique_columns=["OmgevingID", "Boekjaar", "Code_Dagboek", "Nummer_Journaalpost", "Volgnummer_Journaalpost"],
                administration_column="Administratie_Code"
            ),
            "Budget": TableConfig(
                mode="truncate",
                unique_columns=["OmgevingID", "Budget_Scenario_Code", "GrootboekID", "Boekjaar", "Boekperiode"],
                administration_column="Administratie_Code"
            ),
            "Divisions": TableConfig(
                mode="truncate",
                unique_columns=["OmgevingID"],
                administration_column="Administratie_Code"
            ),
            "Medewerkers": TableConfig(
                mode="truncate",
                unique_columns=["OmgevingID", "Administratie_Code", "GUID"],
                administration_column="Administratie_Code"
            ),
            "Relaties": TableConfig(
                mode="truncate",
                unique_columns=["OmgevingID", "Naam"],
                administration_column=""
            ),
            "Projecten": TableConfig(
                mode="truncate",
                unique_columns=["OmgevingID", "Administratie_Code", "ProjectID"],
                administration_column="Administratie_Code"
            ),
            "Urenregistratie": TableConfig(
                mode="truncate",
                unique_columns=["OmgevingID", "Administratie_Code", "Urenregistratie_GUID"],
                administration_column="Administratie_Code"
            ),
            "Verlof": TableConfig(
                mode="truncate",
                unique_columns=[],
                administration_column="Administratie_Code"
            ),
            "VerzuimVerloop": TableConfig(
                mode="truncate",
                unique_columns=["OmgevingID", "Verzuimmelding_GUID"],
                administration_column=""
            ),
            "VerzuimUren": TableConfig(
                mode="truncate",
                unique_columns=[],
                administration_column="Administratie_Code"
            ),
            "Contracten": TableConfig(
                mode="truncate",
                unique_columns=["OmgevingID", "Administratie_Code", "Medewerker_GUID", "Arbeids_ContractID"],
                administration_column="Administratie_Code"
            ),
            "Abonnementen": TableConfig(
                mode="none",
                unique_columns=["OmgevingID", "Administratie_Code", "AbonnementID"],
                administration_column="Administratie_Code"
            ),
            "CaseLogging": TableConfig(
                mode="none",
                unique_columns=["OmgevingID", "Administratie_Code", "Logregel"],
                administration_column="Administratie_Code"
            ),
            "Dossiers": TableConfig(
                mode="none",
                unique_columns=["OmgevingID", "Administratie_Code", "DossierID", "Dossier_Nummer"],
                administration_column="Administratie_Code"
            ),
            "Forecasts": TableConfig(
                mode="none",
                unique_columns=["OmgevingID", "Administratie_Code", "ForecastID"],
                administration_column="Administratie_Code"
            ),
            "Roosters": TableConfig(
                mode="none",
                unique_columns=[],
                administration_column="Administratie_Code"
            ),
            "Nacalculatie": TableConfig(
                mode="none",
                unique_columns=["OmgevingID", "Administratie_Code", "NacalculatieGUID"],
                administration_column="Administratie_Code"
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
    if auth_method.upper() == "MEI":
        if not token_struct:
            raise ValueError("Token is vereist voor MEI authenticatie")
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        connect_args = {'attrs_before': {SQL_COPT_SS_ACCESS_TOKEN: token_struct}}
        return create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}", connect_args=connect_args)
    elif auth_method.upper() == "SQL":
        return create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}")
    else:
        raise ValueError(f"Ongeldige authenticatie methode: {auth_method}. Gebruik 'SQL' of 'MEI'")

def clear_data(engine, table, config, omgeving_id, laatste_sync):
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
                        logging.info(f"Laatste sync is meer dan een jaar geleden, overschakelen naar volledige truncate voor {table}")
                        if omgeving_id is not None:
                            result = connection.execute(
                                text(f"DELETE FROM {table} WHERE OmgevingID = :omgeving_id"),
                                {"omgeving_id": omgeving_id}
                            )
                            rows_deleted = result.rowcount
                            logging.info(f"Verwijderd {rows_deleted} rijen voor omgeving {omgeving_id} uit {table}")
                        else:
                            connection.execute(text(f"TRUNCATE TABLE {table}"))
                            logging.info(f"Tabel {table} succesvol getruncate")
                        connection.commit()
                        return rows_deleted if omgeving_id is not None else 0
                except (ValueError, TypeError) as e:
                    logging.info(f"Kon laatste_sync niet verwerken: {e}. Gebruik standaard operatie.")
            
            # Standaard operatie op basis van mode en omgeving_id
            if config.mode == 'truncate':
                if omgeving_id is not None:
                    result = connection.execute(
                        text(f"DELETE FROM {table} WHERE OmgevingID = :omgeving_id"),
                        {"omgeving_id": omgeving_id}
                    )
                    rows_deleted = result.rowcount
                    logging.info(f"Verwijderd {rows_deleted} rijen voor omgeving {omgeving_id} uit {table}")
                else:
                    connection.execute(text(f"TRUNCATE TABLE {table}"))
                    logging.info(f"Tabel {table} succesvol getruncate")
                    rows_deleted = 0
                
                connection.commit()
                return rows_deleted
            else:
                logging.info(f"Geen actie ondernomen voor tabel {table} (mode: {config.mode})")
                return 0
    except Exception as e:
        logging.error(f"Fout bij het verwijderen van data uit {table}: {str(e)}")
        raise

def write_data(engine, df, table, config, laatste_sync):
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
                if config.administration_column:
                    on_clause += f" AND target.{config.administration_column} = source.{config.administration_column}"
                
                update_set = ", ".join([f"target.{col} = source.{col}" 
                                      for col in df.columns 
                                      if col not in config.unique_columns and 
                                      col != config.administration_column])
                
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

def apply_table_clearing(connection_string, table, omgeving_id, laatste_sync, config_manager=None):
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
        
        rows_deleted = clear_data(engine, table, config, omgeving_id, laatste_sync)
        return rows_deleted > 0
    except Exception as e:
        logging.error(f"Fout bij het verwijderen van rijen of leegmaken van de tabel: {str(e)}")
        return False

def apply_table_writing(df, connection_string, table, laatste_sync, config_manager=None):
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