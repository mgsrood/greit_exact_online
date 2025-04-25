# Voeg de root van het project toe aan de Python path
import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))
sys.path.append(project_root)

# Importeer de benodigde modules
from greit_exact_online.sql_script.utils.database_connection import get_azure_sql_access_token
from greit_exact_online.sql_script.utils.env_config import EnvConfig
from sqlalchemy import create_engine, event, text
from dataclasses import dataclass
from datetime import datetime
import logging
import time

@dataclass
class TableConfig:
    """Configuratie voor tabel operaties."""
    mode: str = None
    unique_columns: list = None
    administration_column: str = None

@dataclass
class TableConfigManager:
    """Beheerder voor tabel configuraties."""
    configs = None

    def __post_init__(self):
        self.configs = {
            "Voorraad": TableConfig(
                mode="none",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "Grootboekrekening": TableConfig(
                mode="none",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "GrootboekRubriek": TableConfig(
                mode="truncate",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "GrootboekMutaties": TableConfig(
                mode="none",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "CrediteurenOpenstaand": TableConfig(
                mode="truncate",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "DebiteurenOpenstaand": TableConfig(
                mode="truncate",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "Relaties": TableConfig(
                mode="none",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "RelatieKeten": TableConfig(
                mode="none",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "Budget": TableConfig(
                mode="none",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "GrootboekMapping": TableConfig(
                mode="truncate",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "ReportingBalance": TableConfig(
                mode="reporting_year",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "Artikelen": TableConfig(
                mode="none",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "ArtikelenExtraVelden": TableConfig(
                mode="none",
                unique_columns=["ArtikelID", "Nummer"],
                administration_column="AdministratieCode"
            ),
            "ArtikelGroepen": TableConfig(
                mode="none",
                unique_columns=["ID"],
                administration_column="AdministratieCode"
            ),
            "Verkoopfacturen": TableConfig(
                mode="none",
                unique_columns=["FR_FactuurregelID"],
                administration_column="F_AdministratieCode"
            ),
            "VerkoopOrders": TableConfig(
                mode="none",
                unique_columns=["OR_OrderRegelID"],
                administration_column="O_AdministratieCode"
            ),
            "Verkoopkansen": TableConfig(
                mode="none",
                unique_columns=["VerkoopkansID"],
                administration_column="AdministratieCode"
            ),
            "Offertes": TableConfig(
                mode="none",
                unique_columns=["O_Versie", "OR_OfferteRegelID"],
                administration_column="O_AdministratieCode"
            ),
            "Divisions": TableConfig(
                mode="truncate",
                unique_columns=["Division"],
                administration_column=""
            )
        }

    def get_config(self, table_name):
        """Haal configuratie op voor een specifieke tabel."""
        return self.configs.get(table_name)

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

def clear_data(engine, table, config, division_code=None, reporting_year=None, laatste_sync=None):
    """Leeg een tabel volgens de gegeven configuratie."""
    try:
        with engine.connect() as connection:
            try:
                # Controleer laatste_sync als die is meegegeven
                if laatste_sync:
                    try:
                        huidige_datum = datetime.now()
                        laatste_sync_datum = datetime.strptime(laatste_sync, "%Y-%m-%dT%H:%M:%S")
                        verschil_in_dagen = (huidige_datum - laatste_sync_datum).days
                        verschil_in_jaren = verschil_in_dagen / 365.0
                        
                        if verschil_in_jaren > 1.2:
                            logging.info(f"Laatste sync is meer dan een jaar geleden, overschakelen naar volledige truncate voor {table}")
                            if division_code is not None:
                                result = connection.execute(
                                    text(f"DELETE FROM {table} WHERE {config.administration_column} = :division_code"),
                                    {"division_code": division_code}
                                )
                                rows_deleted = result.rowcount
                            else:
                                result = connection.execute(text(f"TRUNCATE TABLE {table}"))
                                rows_deleted = 0  # TRUNCATE geeft geen rowcount
                            connection.commit()
                            logging.info(f"Tabel {table} succesvol geleegd voor divisie {division_code}.")
                            return rows_deleted
                    except (ValueError, TypeError) as e:
                        logging.warning(f"Kon laatste_sync niet verwerken: {e}. Gebruik standaard operatie.")

                # Log foreign key constraints
                if table == "VerkoopOrders":
                    fk_query = """
                    SELECT 
                        OBJECT_NAME(parent_object_id) AS ReferencingTable,
                        OBJECT_NAME(referenced_object_id) AS ReferencedTable,
                        name AS ConstraintName
                    FROM sys.foreign_keys
                    WHERE referenced_object_id = OBJECT_ID('VerkoopOrders')
                    """
                    fk_result = connection.execute(text(fk_query))
                    fk_constraints = fk_result.fetchall()
                    
                    if fk_constraints:
                        logging.info(f"Foreign key constraints gevonden voor VerkoopOrders:")
                        for constraint in fk_constraints:
                            logging.info(f"- {constraint.ReferencingTable} verwijst naar VerkoopOrders via {constraint.ConstraintName}")
                    
                    # Controleer of er records zijn die verwijzen naar VerkoopOrders
                    for constraint in fk_constraints:
                        check_query = f"""
                        SELECT COUNT(*) as Count
                        FROM {constraint.ReferencingTable}
                        WHERE EXISTS (
                            SELECT 1 FROM VerkoopOrders
                            WHERE VerkoopOrders.OR_OrderRegelID = {constraint.ReferencingTable}.FR_VerkooporderRegelID
                            OR VerkoopOrders.O_OrderID = {constraint.ReferencingTable}.FR_VerkooporderID
                        )
                        """
                        count_result = connection.execute(text(check_query))
                        count = count_result.scalar()
                        if count > 0:
                            logging.warning(f"Waarschuwing: {count} records in {constraint.ReferencingTable} verwijzen naar VerkoopOrders")

                if config.mode == 'truncate':
                    if division_code is not None:
                        # Log het aantal te verwijderen records
                        count_query = text(f"SELECT COUNT(*) FROM {table} WHERE {config.administration_column} = :division_code")
                        count_result = connection.execute(count_query, {"division_code": division_code})
                        record_count = count_result.scalar()
                        logging.info(f"Aantal te verwijderen records in {table}: {record_count}")
                        
                        result = connection.execute(
                            text(f"DELETE FROM {table} WHERE {config.administration_column} = :division_code"),
                            {"division_code": division_code}
                        )
                    else:
                        # Log het aantal te verwijderen records
                        count_query = text(f"SELECT COUNT(*) FROM {table}")
                        count_result = connection.execute(count_query)
                        record_count = count_result.scalar()
                        logging.info(f"Aantal te verwijderen records in {table}: {record_count}")
                        
                        result = connection.execute(text(f"DELETE FROM {table}"))
                elif config.mode == 'reporting_year' and reporting_year is not None and division_code is not None:
                    result = connection.execute(
                        text(f"DELETE FROM {table} WHERE ReportingYear >= :year AND {config.administration_column} = :division_code"),
                        {"year": reporting_year, "division_code": division_code}
                    )
                else:
                    logging.info(f"Geen actie ondernomen voor tabel {table}")
                    return 0

                rows_deleted = result.rowcount
                connection.commit()
                
                if rows_deleted > 0:
                    logging.info(f"Tabel {table} succesvol geleegd, {rows_deleted} rijen verwijderd.")
                else:
                    logging.info(f"Tabel {table} is leeg, geen rijen verwijderd.")
                    
                return rows_deleted
                
            except Exception as e:
                connection.rollback()
                logging.error(f"Fout bij het leegmaken van tabel {table}: {str(e)}")
                # Log de volledige stack trace voor meer details
                import traceback
                logging.error(f"Stack trace: {traceback.format_exc()}")
                return 0
                
    except Exception as e:
        logging.error(f"Fout bij het maken van database verbinding voor tabel {table}: {str(e)}")
        return 0

def apply_table_clearing(connection_string, table, division_code=None, reporting_year=None, laatste_sync=None, config_manager=None):
    """Pas tabel leegmaak operatie toe.
    
    Args:
        table: De naam van de tabel
        reporting_year: Het rapportagejaar (optioneel)
        division_code: De divisie code (optioneel)
        laatste_sync: De laatste sync datum (optioneel)
        config_manager: De configuratie manager (optioneel)
    """
    try:
        if config_manager is None:
            config_manager = TableConfigManager()

        logging.info(f"Start mogelijk verwijderen rijen of complete tabel: {table}")
        
        config = config_manager.get_config(table)
        if config is None:
            logging.error(f"Geen configuratie gevonden voor tabel: {table}")
            return False

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
        
        rows_deleted = clear_data(engine, table, config, division_code, reporting_year, laatste_sync)
        return rows_deleted > 0

    except Exception as e:
        logging.error(f"Fout bij het verwijderen van rijen of leegmaken van de tabel: {str(e)}")
        return False
    
def write_data(engine, df, table, config, laatste_sync=None):
    """Schrijf data naar een tabel met de juiste configuratie."""
    try:
        @event.listens_for(engine, "before_cursor_execute")
        def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True

        with engine.connect() as connection:
            temp_table_name = None
            try:
                if config.mode == 'none' and laatste_sync:
                    huidige_datum = datetime.now()
                    laatste_sync_datum = datetime.strptime(laatste_sync, "%Y-%m-%dT%H:%M:%S")
                    verschil_in_jaren = (huidige_datum - laatste_sync_datum).days / 365

                    if verschil_in_jaren > 1.2:
                        logging.info(f"Laatste sync is meer dan een jaar geleden, overschakelen naar simpele insert voor tabel: {table}")
                        df.to_sql(table, engine, index=False, if_exists="append", schema="dbo")
                    else:
                        temp_table_name = f"temp_table_{int(time.time())}"
                        df.to_sql(temp_table_name, engine, index=False, if_exists="replace", schema="dbo")

                        on_clause = " AND ".join([f"target.{col} = source.{col}" for col in config.unique_columns])
                        merge_query = f"""
                        MERGE {table} AS target
                        USING (SELECT * FROM {temp_table_name}) AS source
                        ON ({on_clause} AND target.{config.administration_column} = source.{config.administration_column})
                        WHEN MATCHED THEN
                            UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in df.columns if col not in config.unique_columns and col != config.administration_column])}
                        WHEN NOT MATCHED THEN
                            INSERT ({', '.join(df.columns)})
                            VALUES ({', '.join([f'source.{col}' for col in df.columns])});
                        """
                        connection.execute(text(merge_query))
                else:
                    df.to_sql(table, engine, index=False, if_exists="append", schema="dbo")

                logging.info(f"DataFrame succesvol toegevoegd/bijgewerkt in de tabel: {table}")
                return True

            except Exception as e:
                logging.error(f"Fout bij het toevoegen naar de database: {str(e)}")
                return False

            finally:
                if temp_table_name:
                    try:
                        connection.execute(text(f"DROP TABLE {temp_table_name}"))
                        connection.commit()
                        logging.info(f"Tijdelijke tabel {temp_table_name} succesvol verwijderd.")
                    except Exception as e:
                        logging.error(f"Fout bij het verwijderen van de tijdelijke tabel {temp_table_name}: {str(e)}")

    except Exception as e:
        logging.error(f"Fout bij het maken van database verbinding: {str(e)}")
        return False

def apply_table_writing(connection_string, df, table, laatste_sync=None, config_manager=None):
    """Pas tabel schrijf operatie toe.
    
    Args:
        df: Het DataFrame met de data
        table: De naam van de tabel
        laatste_sync: De laatste sync datum (optioneel)
        config_manager: De configuratie manager (optioneel)
    """
    try:
        if config_manager is None:
            config_manager = TableConfigManager()

        logging.info(f"Start toevoegen rijen naar database: {table}")
        
        config = config_manager.get_config(table)
        if config is None:
            logging.error(f"Geen configuratie gevonden voor tabel: {table}")
            return False

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
        
        success = write_data(engine, df, table, config, laatste_sync)
        
        if success:
            logging.info(f"Succesvol {len(df)} rijen toegevoegd aan de database")
        return success

    except Exception as e:
        logging.error(f"Fout bij het toevoegen naar database: {str(e)}")
        return False