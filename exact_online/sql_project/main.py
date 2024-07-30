import pyodbc
from dotenv import load_dotenv
import os
import time
import requests
from datetime import datetime
from modules.logging import logging
from modules.config import fetch_all_connection_strings, fetch_configurations, fetch_division_codes
from modules.tokens import get_new_tokens, save_refresh_token, save_access_token
import xml.etree.ElementTree as ET
import pandas as pd

def get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam):
    full_url = f"{url}{endpoint}"
    data = []

    # Retrieve config
    try:
        conn = pyodbc.connect(connection_string)
        print("Verbonden met de database!")
    except Exception as e:
        print(f"Fout bij verbinding: {e}")

    # Cursor maken
    cursor = conn.cursor()

    config_dict = fetch_configurations(cursor)

    conn.close()

    client_secret = config_dict['client_secret']
    client_id = config_dict['client_id']
    access_token = config_dict['access_token']
    refresh_token = config_dict['refresh_token']
    
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

            # Kijk of er een volgende pagina is
            next_link = root.find('.//{http://www.w3.org/2005/Atom}link[@rel="next"]')
            full_url = next_link.attrib['href'] if next_link is not None else None

            # Wacht 10 seconden voordat je de volgende request doet
            print("Volgende batch opvragen in 10 seconden...")
            time.sleep(10)

        else:
            print(f"Fout bij het ophalen van gegevens: {response.status_code} - {response.text}")
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
            else:
                return None

    # Maak een DataFrame van de verzamelde gegevens
    df = pd.DataFrame(data)

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

    # Verbinding maken
    try:
        conn = pyodbc.connect(finn_it_connection_string)
        print("Verbonden met de database!")
    except Exception as e:
        print(f"Fout bij verbinding: {e}")

    # Cursor maken
    cursor = conn.cursor()

    # Logging
    klantnaam = 'Finn It'
    actie = 'Ophalen connectiestrings'
    logging(cursor, conn, klantnaam, actie)

    # Je kunt nu een cursor maken en queries uitvoeren
    connection_dict = fetch_all_connection_strings(cursor)

    # Verbinding sluiten
    conn.close()

    for klantnaam, connection_string in connection_dict.items():
        try:
            conn = pyodbc.connect(connection_string)
            print("Verbonden met de database!")
        except Exception as e:
            print(f"Fout bij verbinding: {e}")

        # Cursor maken
        cursor = conn.cursor()

        config_dict = fetch_configurations(cursor)
        
        conn.close()

        # Verbinding maken
        try:
            conn = pyodbc.connect(finn_it_connection_string)
            print("Verbonden met de database!")
        except Exception as e:
            print(f"Fout bij verbinding: {e}")

        # Cursor maken
        cursor = conn.cursor()

        # Logging
        actie = 'Verbinding maken met config tabel'
        logging(cursor, conn, klantnaam, actie)

        conn.close()

        try:
            conn = pyodbc.connect(connection_string)
            print("Verbonden met de database!")
        except Exception as e:
            print(f"Fout bij verbinding: {e}")

        # Cursor maken
        cursor = conn.cursor()

        division_codes = fetch_division_codes(cursor)

        conn.close()

        # Verbinding maken
        try:
            conn = pyodbc.connect(finn_it_connection_string)
            print("Verbonden met de database!")
        except Exception as e:
            print(f"Fout bij verbinding: {e}")

        # Cursor maken
        cursor = conn.cursor()

        # Logging
        actie = 'Verbinding maken met divisions tabel'
        logging(cursor, conn, klantnaam, actie)

        conn.close()

        division_code = division_codes['HAIL Europe B.V.']

        try:
            conn = pyodbc.connect(connection_string)
            print("Verbonden met de database!")
        except Exception as e:
            print(f"Fout bij verbinding: {e}")

        # Cursor maken
        cursor = conn.cursor()

        url = f"https://start.exactonline.nl/api/v1/{division_code}/"
        endpoint = 'read/financial/ReceivablesList?$select=AccountCode,AccountId,AccountName,Amount,CurrencyCode,Description,DueDate,EntryNumber,Id,InvoiceDate,InvoiceNumber,JournalCode,YourRef'

        df = get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam)

        print(df)
        
        conn.close()

'''        # Verbinding maken per divisie code
        for division_name, division_code in division_codes.items():
            try:
                conn = pyodbc.connect(connection_string)
                print("Verbonden met de database!")
            except Exception as e:
                print(f"Fout bij verbinding: {e}")

            # Cursor maken
            cursor = conn.cursor()

            url = f"https://start.exactonline.nl/api/v1/{division_code}/"
            endpoint = 'read/financial/ReceivablesList?$select=AccountCode,AccountId,AccountName,Amount,CurrencyCode,Description,DueDate,EntryNumber,Id,InvoiceDate,InvoiceNumber,JournalCode,YourRef'

            df = get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam)

            conn.close()

            print(df)'''