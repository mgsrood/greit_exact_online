import pyodbc
import time
from datetime import datetime
import sqlalchemy
from sqlalchemy import create_engine, event, text
import urllib

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

def write_to_database(df, tabel, connection_string, unique_column, division_column, mode, laatste_sync):
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

                    # Gebruik daarna een MERGE-query om de data te synchroniseren met de doel-tabel
                    merge_query = f"""
                    MERGE {tabel} AS target
                    USING (SELECT * FROM {temp_table_name}) AS source
                    ON (target.{unique_column} = source.{unique_column} AND target.{division_column} = source.{division_column})
                    WHEN MATCHED THEN
                        UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in df.columns if col not in [unique_column, division_column]])}
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

def clear_table(connection_string, table, mode, reporting_year, division_code):
    try:
        # Maak verbinding met de database
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        rows_deleted = 0
        
        if mode == 'truncate':
            # Probeer de tabel leeg te maken met TRUNCATE TABLE
            try:
                cursor.execute(f"DELETE FROM {table} WHERE AdministratieCode = ?", division_code)
                rows_deleted = cursor.rowcount
            except pyodbc.Error as e:
                print(f"DELETE FROM {table} WHERE AdministratieCode = {division_code} failed: {e}")
        elif mode == 'reporting_year':
            # Verwijder rijen waar ReportingYear >= reporting_year en AdministratieCode = division_code
            cursor.execute(f"DELETE FROM {table} WHERE ReportingYear >= ? AND AdministratieCode = ?", reporting_year, division_code)
            rows_deleted = cursor.rowcount
        elif mode == 'none':
            # Doe niets
            print(f"Geen actie ondernomen voor tabel {table}.")
        
        # Commit de transactie
        connection.commit()
        print(f"Actie '{mode}' succesvol uitgevoerd voor tabel {table}.")
        actie = f"Actie '{mode}' succesvol uitgevoerd voor tabel {table}."
    except pyodbc.Error as e:
        print(f"Fout bij het uitvoeren van de actie '{mode}' voor tabel {table}: {e}")
    finally:
        # Sluit de cursor en verbinding
        cursor.close()
        connection.close()
    
    return actie, rows_deleted