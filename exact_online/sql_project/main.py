import pyodbc
from dotenv import load_dotenv
import os
import time
import requests
from datetime import datetime
from modules.logging import logging
from modules.config import fetch_all_connection_strings, fetch_configurations, fetch_division_codes
from modules.tokens import get_new_tokens, save_refresh_token, save_access_token
from modules.database import connect_to_database
from modules.table_mapping import transform_columns, CrediteurenOpenstaand, DebiteurenOpenstaand
import xml.etree.ElementTree as ET
import pandas as pd

def get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel):
    full_url = f"{url}{endpoint}"
    data = []
    total_rows = 0 

    # Pre-logging actie
    logging_conn = connect_to_database(finn_it_connection_string)
    if logging_conn:
        cursor = logging_conn.cursor()
        logging(cursor, logging_conn, klantnaam, f"Start GET Request: {tabel}")
        logging(cursor, logging_conn, klantnaam, f"Ophalen configuratiegegevens")
        logging_conn.close()

    # Config ophalen
    config_conn = connect_to_database(connection_string)
    if config_conn:
        cursor = config_conn.cursor()
        config_dict = fetch_configurations(cursor)
        if config_dict is None:
            print("Fout bij het ophalen van de configuratiegegevens.")
        else:
            print("Configuratiegegevens succesvol opgehaald.")
        config_conn.close()

    # Variabelen definiëren
    client_secret = config_dict['client_secret']
    client_id = config_dict['client_id']
    access_token = config_dict['access_token']
    refresh_token = config_dict['refresh_token']

    # Post-logging actie
    logging_conn = connect_to_database(finn_it_connection_string)
    if logging_conn:
        cursor = logging_conn.cursor()
        logging(cursor, logging_conn, klantnaam, f"Configuratiegegevens succesvol opgehaald")
        logging_conn.close()

    # Request loop
    while full_url:
        payload = {}
        headers = {
        'Authorization': f'Bearer {access_token}',
        'Cookie': f'.AspNetCore.Antiforgery.cdV5uW_Ejgc=CfDJ8CTJVFXxHxJHqt315CGWnt6RmoANCHzuwWq9U7Hje9I3wCAI4LZuudgNgWB6dYyMvEmg32OtzGlkiXWwnahptcAkcALB6KJT_gEvyE6MVNsWYGaWCvjmIDAtTJaRIoAFFsgnc8-ZLrEq13YMkITaGlg; .AspNetCore.Culture=c%3Dnl%7Cuic%3Dnl; ARRAffinity=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ARRAffinitySameSite=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ASP.NET_SessionId=3esxcu3yqpsi31zurmgciobc; ExactOnlineClient=7LjlWUfX5lQbrehxxcOgynPtG4hwzKvhbdRrllvl9LzDBusePiI7bj3hvRnEiEC15zVDQrffSDkobPKR/bxTiiFmlkNu4odr6q55xN5eyQQd6dQzIORCNib+1rPe5uyf55F2LyZLmpDtrMt1lYIyYU+Y8+fr/PyNF2VDkL5wcpQ=; ExactServer{{2bd296c3-cdda-4a9b-9dec-0aad425078e1}}=Division={division_code}'
        }

        response = requests.request("GET", full_url, headers=headers, data=payload)

        # Controleer of de request succesvol was
        if response.status_code == 200:
            # Parse de XML-response
            root = ET.fromstring(response.content)
            
            # Loop door elk <entry> element en haal de data op
            for entry in root.findall('.//{http://www.w3.org/2005/Atom}entry'):
                item_data = {}
                for prop in entry.findall('.//{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}properties/*'):
                    if prop.text is not None:
                        item_data[prop.tag.replace('{http://schemas.microsoft.com/ado/2007/08/dataservices}', '')] = prop.text.strip()
                    else:
                        item_data[prop.tag.replace('{http://schemas.microsoft.com/ado/2007/08/dataservices}', '')] = None
                data.append(item_data)
                total_rows += 1

            # Kijk of er een volgende pagina is
            next_link = root.find('.//{http://www.w3.org/2005/Atom}link[@rel="next"]')
            full_url = next_link.attrib['href'] if next_link is not None else None

            # Wacht 10 seconden voordat je de volgende request doet
            print(f"Opgehaalde rijen tot nu toe: {total_rows}")
            time.sleep(1)

        else:
            print(f"Fout bij het ophalen van gegevens: {response.status_code} - {response.text}")
            
            # Pre-logging actie
            logging_conn = connect_to_database(finn_it_connection_string)
            if logging_conn:
                cursor = logging_conn.cursor()
                logging(cursor, logging_conn, klantnaam, f"Ophalen nieuwe access- en refresh token")
                logging_conn.close()
            
            oude_refresh_token = refresh_token
            client_id = client_id
            client_secret = client_secret
            new_access_token, new_refresh_token = get_new_tokens(oude_refresh_token, client_id, client_secret, finn_it_connection_string, klantnaam)

            if new_refresh_token:
                save_refresh_token(connection_string, new_refresh_token)
                print("Nieuwe refresh token opgeslagen")

            # Nu kun je de nieuwe access en refresh tokens gebruiken voor je verzoeken
            if new_access_token and new_refresh_token:
                # Voer hier je verzoek uit met de nieuwe access token
                save_access_token(connection_string, new_access_token)
                print("Nieuwe access token opgeslagen")
                access_token = new_access_token

                # Post-logging actie
                logging_conn = connect_to_database(finn_it_connection_string)
                if logging_conn:
                    cursor = logging_conn.cursor()
                    logging(cursor, logging_conn, klantnaam, f"Ophalen nieuwe access- en refresh token gelukt")
                    logging_conn.close()
            else:
                return None

    # Maak een DataFrame van de verzamelde gegevens
    df = pd.DataFrame(data)

    # Logging
    logging_conn = connect_to_database(finn_it_connection_string)
    if logging_conn:
        cursor = logging_conn.cursor()
        logging(cursor, logging_conn, klantnaam, f"GET Request succesvol afgerond: {tabel}")
        logging_conn.close()

    return df

if __name__ == "__main__":

    # Laad de omgevingsvariabelen uit het .env-bestand
    load_dotenv()

    # Verbindingsinstellingen
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    driver = '{ODBC Driver 17 for SQL Server}'

    # Verbindingsstring
    finn_it_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

    # Pre-actie logging
    klantnaam = 'Finn It'
    logging_conn = connect_to_database(finn_it_connection_string)
    if logging_conn:
        cursor = logging_conn.cursor()
        logging(cursor, logging_conn, klantnaam, f"Script gestart")
        logging_conn.close()

    # Pre-actie logging
    logging_conn = connect_to_database(finn_it_connection_string)
    if logging_conn:
        cursor = logging_conn.cursor()
        logging(cursor, logging_conn, klantnaam, f"Ophalen connectiestring gestart")
        logging_conn.close()
    
    # Verbinding maken met database
    database_conn = connect_to_database(finn_it_connection_string)
    if database_conn:
        cursor = database_conn.cursor()
        connection_dict = fetch_all_connection_strings(cursor)
        database_conn.close()
    
    # Post-actie logging
    logging_conn = connect_to_database(finn_it_connection_string)
    if logging_conn:
        cursor = logging_conn.cursor()
        logging(cursor, logging_conn, klantnaam, f"Ophalen connectiestring gelukt")
        logging_conn.close()

    # Klant loop
    for klantnaam, connection_string in connection_dict.items():
        
        # Pre-actie logging
        logging_conn = connect_to_database(finn_it_connection_string)
        if logging_conn:
            cursor = logging_conn.cursor()
            logging(cursor, logging_conn, klantnaam, f"Ophalen configuratie gegevens gestart")
            logging_conn.close()
        
        # Ophalen configuratie gegevens
        config_conn = connect_to_database(connection_string)
        if config_conn:
            cursor = config_conn.cursor()
            config_dict = fetch_configurations(cursor)
            config_conn.close()
        
        # Post- en pre-actie logging
        logging_conn = connect_to_database(finn_it_connection_string)
        if logging_conn:
            cursor = logging_conn.cursor()
            logging(cursor, logging_conn, klantnaam, f"Ophalen configuratie gegevens gelukt")
            logging(cursor, logging_conn, klantnaam, f"Ophalen divisiecodes gestart")
            logging_conn.close()

        # Ophalen divisiecodes
        division_conn = connect_to_database(connection_string)
        if division_conn:
            cursor = division_conn.cursor()
            division_dict = fetch_division_codes(cursor)
            division_conn.close()

        # Post-actie logging
        logging_conn = connect_to_database(finn_it_connection_string)
        if logging_conn:
            cursor = logging_conn.cursor()
            logging(cursor, logging_conn, klantnaam, f"Ophalen divisiecodes gelukt")
            logging_conn.close()

        # Verbinding maken per divisie code
        for division_name, division_code in division_dict.items():
            # Print divisie naam en divisie code
            print(f"Begin GET Requests voor divisie: {division_name} ({division_code})")

            # Pre-actie logging
            logging_conn = connect_to_database(finn_it_connection_string)
            if logging_conn:
                cursor = logging_conn.cursor()
                logging(cursor, logging_conn, klantnaam, f"Start GET Requests voor divisie: {division_name} ({division_code})")
                logging_conn.close()

            url = f"https://start.exactonline.nl/api/v1/{division_code}/"
            endpoints = {
                "CrediteurenOpenstaand": "read/financial/PayablesList?$select=HID,AccountId,Amount,CurrencyCode,Description,DueDate,EntryNumber,InvoiceDate,InvoiceNumber,YourRef",
                "DebiteurenOpenstaand": "read/financial/ReceivablesList?$select=AccountCode,AccountId,AccountName,Amount,CurrencyCode,Description,DueDate,EntryNumber,Id,InvoiceDate,InvoiceNumber,JournalCode,YourRef"
                }

            # Endpoint loop
            for tabel, endpoint in endpoints.items():
                df = get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel)

                # Tabel mapping
                mapping_dict = {
                    "CrediteurenOpenstaand": CrediteurenOpenstaand,
                    "DebiteurenOpenstaand": DebiteurenOpenstaand,
                }

                column_mapping = mapping_dict.get(tabel)
                if column_mapping is None:
                    print(f"Geen kolom mapping gevonden voor tabel: {tabel}")
                    continue

                # Transform the DataFrame
                df_transformed = transform_columns(df, column_mapping, division_code)

                # Show all columns of the DataFrame in the terminal
                pd.set_option("display.max_columns", None)
                print(df_transformed)

            # Post-actie logging
            logging_conn = connect_to_database(finn_it_connection_string)
            if logging_conn:
                cursor = logging_conn.cursor()
                logging(cursor, logging_conn, klantnaam, f"GET Requests succesvol afgerond voor divisie: {division_name}")
                logging_conn.close()
    
    # Post-actie logging
    logging_conn = connect_to_database(finn_it_connection_string)
    if logging_conn:
        cursor = logging_conn.cursor()
        logging(cursor, logging_conn, klantnaam, f"Script succesvol afgerond")
        logging_conn.close()