from sqlalchemy import create_engine, event, text
from datetime import datetime, timedelta
from afas_modules.log import log
import sqlalchemy
import urllib
import pyodbc
import time

def connect_to_database(connection_string):
    # Retries en delays
    max_retries = 3
    retry_delay = 5
    
    # Pogingen doen om connectie met database te maken
    for attempt in range(max_retries):
        try:
            conn = pyodbc.connect(connection_string)
            return conn
        except Exception as e:
            print(f"Fout bij poging {attempt + 1} om verbinding te maken: {e}")
            if attempt < max_retries - 1:  # Wacht alleen als er nog pogingen over zijn
                time.sleep(retry_delay)
    
    # Als het na alle pogingen niet lukt, return None
    print("Kan geen verbinding maken met de database na meerdere pogingen.")
    return None

def write_to_database(df, tabel, connection_string, unique_columns, division_column, mode, laatste_sync):

    db_params = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={db_params}")

    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True

    with engine.connect() as connection:
        temp_table_name = None
        try:
            if mode == 'none':
                # Controleer of laatste_sync meer dan een jaar geleden is
                skip_merge = False
                if laatste_sync:
                    huidige_datum = datetime.now()
                    laatste_sync_datum = datetime.strptime(laatste_sync, "%Y-%m-%dT%H:%M:%S")
                    verschil_in_jaren = (huidige_datum - laatste_sync_datum).days / 365

                    if verschil_in_jaren > 1.2:
                        skip_merge = True
                        print(f"Laatste sync is meer dan een jaar geleden ({laatste_sync}), overschakelen naar simpele insert voor tabel: {tabel}")
                if skip_merge:
                    # Schrijf direct naar de database toe
                    df.to_sql(tabel, engine, index=False, if_exists="append", schema="dbo")
                else:
                    # Maak een unieke naam voor de tijdelijke fysieke tabel
                    temp_table_name = f"temp_table_{int(time.time())}"
                    
                    # Laad de data in de tijdelijke fysieke tabel
                    df.to_sql(temp_table_name, engine, index=False, if_exists="replace", schema="dbo")

                    # Constructeer de ON clause voor de MERGE-query
                    unique_conditions = [f"target.{col} = source.{col}" for col in unique_columns]
                    if division_column:
                        unique_conditions.append(f"target.{division_column} = source.{division_column}")
                    on_clause = " AND ".join(unique_conditions)
                    
                    # Constructeer de UPDATE SET clause voor de MERGE-query
                    update_set = ", ".join([f"target.{col} = source.{col}" for col in df.columns if col not in unique_columns and col != division_column])
                    
                    # Gebruik daarna een MERGE-query om de data te synchroniseren met de doel-tabel
                    merge_query = f"""
                    MERGE {tabel} AS target
                    USING (SELECT * FROM {temp_table_name}) AS source
                    ON ({on_clause})
                    WHEN MATCHED THEN
                        UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN
                        INSERT ({', '.join(df.columns)})
                        VALUES ({', '.join([f'source.{col}' for col in df.columns])});
                    """
                    connection.execute(text(merge_query))
            else:
                # Andere modes blijven ongewijzigd
                df.to_sql(tabel, engine, index=False, if_exists="append", schema="dbo")
        except Exception as e:
            print(f"Fout bij het toevoegen naar de database: {e}")
        finally:
            if temp_table_name:
                try:
                    connection.execute(text(f"DROP TABLE {temp_table_name}"))
                    connection.commit()  # Forceer een commit na het verwijderen van de tabel
                    print(f"Tijdelijke tabel {temp_table_name} succesvol verwijderd.")
                except Exception as e:
                    print(f"Fout bij het verwijderen van de tijdelijke tabel {temp_table_name}: {e}")

    print(f"DataFrame succesvol toegevoegd/bijgewerkt in de tabel: {tabel}")

def clear_table(connection_string, table, mode, omgeving_id):
    
    last_year = datetime.now().year - 1
    
    try:
        # Maak verbinding met de database
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        rows_deleted = 0
        
        if mode == 'truncate':
            # Probeer de tabel leeg te maken met TRUNCATE TABLE
            try:
                cursor.execute(f"DELETE FROM {table} WHERE OmgevingID = ? AND Boekjaar >= ?", omgeving_id, last_year)
                rows_deleted = cursor.rowcount
                print("Rows deleted:", rows_deleted)
            except pyodbc.Error as e:
                print(f"DELETE FROM {table} failed: {e}")
        elif mode == 'none':
            # Doe niets
            print(f"Geen actie ondernomen voor tabel {table}.")
        
        # Commit de transactie
        connection.commit()
        print(f"Actie '{mode}' succesvol uitgevoerd voor tabel {table}.")
    except pyodbc.Error as e:
        print(f"Fout bij het uitvoeren van de actie '{mode}' voor tabel {table}: {e}")
    finally:
        # Sluit de cursor en verbinding
        cursor.close()
        connection.close()
    
    return rows_deleted

def apply_table_clearing(connection_string, finn_it_connection_string, klantnaam, script_id, script, tabel, omgeving_id):
    
    log(finn_it_connection_string, klantnaam, f"Start mogelijk verwijderen rijen of complete tabel", script_id, script, omgeving_id, tabel)

    # Table modes for deleting rows or complete table
    table_modes = {
        "GrootboekMutaties": "truncate",
    }

    table_mode = table_modes.get(tabel)
    print(f"Table mode: {table_mode}")
    
    if table_mode is None:
        # Foutmelding log en print
        print(f"Geen actie gevonden voor tabel: {tabel}")
        log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen actie gevonden", script_id, script, omgeving_id, tabel)
        return False

    try:
        # Clear the table
        rows_deleted = clear_table(connection_string, tabel, table_mode, omgeving_id)

        # Succes en start log
        log(finn_it_connection_string, klantnaam, f"Totaal verwijderde rijen {rows_deleted}", script_id, script, omgeving_id, tabel)
        
        return True
    
    except Exception as e:
        print(f"Fout bij het verwijderen van rijen of leegmaken van de tabel: {e}")
        log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het verwijderen van rijen of leegmaken van de tabel: {e}", script_id, script, omgeving_id, tabel)
        return False

def apply_table_writing(df, connection_string, finn_it_connection_string, klantnaam, script_id, script, table, laatste_sync, omgeving_id):
    
    log(finn_it_connection_string, klantnaam, f"Start toevoegen rijen naar database", script_id, script, omgeving_id, table)

    # Table modes for deleting rows or complete table
    table_modes = {
        "GrootboekMutaties": "truncate",
    }

    table_mode = table_modes.get(table)

    # Unieke kolom per tabel
    unique_columns = {
        "Grootboekrekening": ["OmgevingID", "GrootboekID"],
        "GrootboekRubriek": ["OmgevingID", "Grootboek_RubriekID"],
        "GrootboekMutaties": ["OmgevingID", "Boekjaar", "Code_Dagboek", "Nummer_Journaalpost", "Volgnummer_Journaalpost"],
        "Budget": ["OmgevingID", "Budget_Scenario_Code", "GrootboekID", "Boekjaar", "Boekperiode"],
        "Divisions": ["OmgevingID"],
    }
    
    # Unieke kolom ophalen voor de specifieke tabel
    unique_column = unique_columns.get(table)
    if unique_column is None:
        # Foutmelding log en print
        print(f"Geen unieke kolom gevonden voor tabel: {table}")
        log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen unieke kolom gevonden", script_id, script, omgeving_id, table)
        return False

    # Administratie kolom per tabel
    administration_columns = {
        "GrootboekMutaties": "Administratie_Code",
    }

    # Administratie kolom ophalen voor de specifieke tabel
    administration_column = administration_columns.get(table)
    if administration_column is None:
        # Foutmelding log en print
        print(f"Geen administratie kolom gevonden voor tabel: {table}")
        log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Geen administratie kolom gevonden", script_id, script, omgeving_id, table)
        return False
    
    # Schrijf de DataFrame naar de database
    try:
        write_to_database(df, table, connection_string, unique_column, administration_column, table_mode, laatste_sync)
        
        # Succeslog bij succes
        log(finn_it_connection_string, klantnaam, f"Succesvol {len(df)} rijen toegevoegd aan de database", script_id, script, omgeving_id, table)
        
        return True
        
    except Exception as e:
        # Foutmelding log en print
        log(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het toevoegen naar database | Foutmelding: {str(e)}", script_id, script, omgeving_id, table)
        print(f"Fout bij het toevoegen naar database: {e}")
        return False