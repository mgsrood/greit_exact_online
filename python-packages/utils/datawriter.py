from sqlalchemy import create_engine, event, text
import logging
import struct
import msal
import re

class DataWriterError(RuntimeError):
    pass

class DataWriter():

    def __init__(self, connectie_string, auth_method, tenant_id = None, client_id = None, client_secret = None):
        self.connectie_string = connectie_string
        self.auth_method = auth_method
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    def _get_azure_sql_access_token(self):
        
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=authority,
            client_credential=self.client_secret
        )
        token_response = app.acquire_token_for_client(scopes=["https://database.windows.net//.default"])
        access_token = token_response.get("access_token")
        if not access_token:
            logging.error("Access token ophalen mislukt.")
            raise DataWriterError("Access token ophalen mislukt.")
        
        token_bytes = bytes(access_token, "UTF-8")
        exptoken = b""
        for i in token_bytes:
            exptoken += bytes({i})
            exptoken += bytes(1)
        token_struct = struct.pack("=i", len(exptoken)) + exptoken

        return token_struct

    def _create_engine_with_auth(self):
        
        # Token struct ophalen
        if self.auth_method == "AzureSQLMEI":
            token_struct = self._get_azure_sql_access_token()

        # Engine aanmaken
        if self.auth_method == "AzureSQLMEI":
            if not token_struct:
                raise DataWriterError("Token is vereist voor MEI authenticatie")
            SQL_COPT_SS_ACCESS_TOKEN = 1256
            connect_args = {'attrs_before': {SQL_COPT_SS_ACCESS_TOKEN: token_struct}}
            return create_engine(f"mssql+pyodbc:///?odbc_connect={self.connectie_string}", connect_args=connect_args)
        elif self.auth_method == "AzureSQL":
            return create_engine(f"mssql+pyodbc:///?odbc_connect={self.connectie_string}")
        else:
            logging.error(f"Ongeldige authenticatie methode: {self.auth_method}. Gebruik 'AzureSQL' of 'AzureSQLMEI'")
            raise DataWriterError(f"Ongeldige authenticatie methode: {self.auth_method}. Gebruik 'AzureSQL' of 'AzureSQLMEI'")

    def _write_data(self, dataframe, tabel, pk_kolommen):
        """
        Schrijf een DataFrame naar de database via een MERGE (upsert).
        pk_kolommen: lijst met kolomnamen die de primary key vormen voor matching.
                    Als leeg, worden alle rijen geüpdatet zonder matching.
        Geeft True terug bij succes, False bij fout.
        """
        # Valideer input
        if not isinstance(tabel, str) or not re.match(r'^[a-zA-Z0-9_]+$', tabel):
            logging.error(f"Ongeldige tabelnaam: {tabel}")
            raise DataWriterError("Ongeldige tabelnaam")
            
        if not isinstance(pk_kolommen, list):
            logging.error(f"pk_kolommen moet een lijst zijn: {pk_kolommen}")
            raise DataWriterError("pk_kolommen moet een lijst zijn")
            
        # Event aanmaken
        engine = self._create_engine_with_auth()
        
        try:
            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                if executemany:
                    cursor.fast_executemany = True

            if hasattr(dataframe, "toPandas"):
                df = dataframe.toPandas()
            else:
                df = dataframe

            if df.empty:
                logging.warning("Waarschuwing: DataFrame is leeg")
                return True

            # Valideer pk_kolommen alleen als ze niet leeg zijn
            if pk_kolommen:
                missing_columns = [col for col in pk_kolommen if col not in df.columns]
                if missing_columns:
                    logging.error(f"Ontbrekende primary key kolommen: {missing_columns}")
                    raise DataWriterError(f"Ontbrekende primary key kolommen: {missing_columns}")

            temp_table = f"Staging_{tabel}"
            df.to_sql(temp_table, engine, index=False, if_exists='replace', schema='dbo')

            # Als er geen primary keys zijn, gebruik dan een andere merge strategie
            if not pk_kolommen:
                # Eerst alle bestaande data verwijderen
                delete_sql = f"DELETE FROM dbo.{tabel}"
                
                # Dan alle nieuwe data invoegen
                insert_columns = ", ".join(df.columns)
                insert_values = ", ".join([f"source.{col}" for col in df.columns])
                
                insert_sql = f"""
                INSERT INTO dbo.{tabel} ({insert_columns})
                SELECT {insert_values}
                FROM dbo.{temp_table} AS source;
                """
                
                with engine.begin() as conn:
                    conn.execute(text(delete_sql))
                    conn.execute(text(insert_sql))
                    conn.execute(text(f"DROP TABLE IF EXISTS dbo.{temp_table}"))
            else:
                # Normale merge met primary keys
                join_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_kolommen])

                compare_conditions = " OR ".join(
                    [f"(ISNULL(target.{col}, '') <> ISNULL(source.{col}, ''))" for col in df.columns if col not in pk_kolommen]
                )

                update_set = ", ".join(
                    [f"target.{col} = source.{col}" for col in df.columns if col not in pk_kolommen]
                )

                insert_columns = ", ".join(df.columns)
                insert_values = ", ".join([f"source.{col}" for col in df.columns])

                merge_sql = f"""
                MERGE dbo.{tabel} AS target
                USING dbo.{temp_table} AS source
                ON {join_condition}
                WHEN MATCHED AND ({compare_conditions})
                THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values});
                """

                with engine.begin() as conn:
                    conn.execute(text(merge_sql))
                    conn.execute(text(f"DROP TABLE IF EXISTS dbo.{temp_table}"))

            logging.info("Data succesvol geschreven naar database.")
            return True

        except Exception as e:
            logging.error(f"Fout bij schrijven data naar DB: {e}")
            return False
        
    def apply_data_write(self, dataframe, tabel, pk_kolommen):
        """Wrap functie om schrijven te triggeren met juiste config en auth."""

        logging.info(f"Start schrijven naar tabel: {tabel}")

        success = self._write_data(dataframe, tabel, pk_kolommen)
        if success:
            print(f"Schrijven naar {tabel} succesvol")
        else:
            print(f"Schrijven naar {tabel} mislukt")
        return success