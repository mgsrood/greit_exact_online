import requests
import xml.etree.ElementTree as ET
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from google.cloud import bigquery
import pandas_gbq as pd_gbq
from datetime import datetime
from modules.config import retrieve_connection, retrieve_config, retrieve_division_code
from modules.tokens import get_new_tokens, save_refresh_token, save_access_token
import time

# Get Request Function
def get_request(division_code, url, endpoint):
    full_url = f"{url}{endpoint}"
    data = []

    # Retrieve config
    client_id, client_secret, access_token, refresh_token = retrieve_config(client, customer_config_table_id, connection, log_table)

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
            print("Volgende batch opvragen")
            time.sleep(1)

        else:
            print(f"Fout bij het ophalen van gegevens: {response.status_code} - {response.text}")
            oude_refresh_token = refresh_token
            client_id = client_id
            client_secret = client_secret
            new_access_token, new_refresh_token = get_new_tokens(oude_refresh_token, client_id, client_secret)

            if new_refresh_token:
                save_refresh_token(customer_config_table_id, new_refresh_token, client_id, client)
                print("Nieuwe refresh token opgeslagen")

            # Nu kun je de nieuwe access en refresh tokens gebruiken voor je verzoeken
            if new_access_token and new_refresh_token:
                # Voer hier je verzoek uit met de nieuwe access token
                save_access_token(customer_config_table_id, new_access_token, client_id, client)
                print("Nieuwe access token opgeslagen")
                access_token = new_access_token
            else:
                return None

    # Maak een DataFrame van de verzamelde gegevens
    df = pd.DataFrame(data)
    
    # Add row to the log table
    if not df.empty:
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_query = f"""
        INSERT INTO {log_table} (Actie, Timestamp)
        VALUES ('GET request {connection}', '{current_datetime}')
        """
        # Voer de query uit
        log_query_job = client.query(log_query)
        log_query_job.result()  # Wacht op voltooiing

        print("GET request log succesvol toegevoegd.")

    return df

if __name__ == "__main__":
    
    # Laad de omgevingsvariabelen uit het .env-bestand
    load_dotenv()

    # Get the BigQuery keys
    gc_keys = os.getenv("GREIT_GOOGLE_CREDENTIALS")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

    # BigQuery information
    project_id = os.getenv("PROJECT_ID")
    dataset_id = os.getenv("DATASET_ID")
    table_id = os.getenv("TABLE_ID")
    full_table_id = f'{project_id}.{dataset_id}.{table_id}'
    log_table = os.getenv("LOG_TABLE")

    # Initialiseer BigQuery connectie
    client = bigquery.Client(location='EU')
        
    # Retrieve connection
    customer_config_table_id, connection = retrieve_connection(client, full_table_id, log_table)

    # Retrieve division code
    division_code = retrieve_division_code(client, connection, log_table)

    # URL en endpoints
    url = f"https://start.exactonline.nl/api/v1/{division_code}/"
    endpoints = {
        'GrootboekMutaties': 'bulk/Financial/TransactionLines?$filter=FinancialYear eq 2016&$select=ID,Account,AmountDC,AmountVATFC,Currency,Date,Description,Division,EntryNumber,FinancialPeriod,FinancialYear,GLAccount,InvoiceNumber,OrderNumber,PaymentReference,VATPercentage',
    }

    for table, endpoint in endpoints.items():
        df = get_request(division_code, url, endpoint)
        if df is not None:
            # Full table id
            full_table_id = f'greit-administration.klant_1_dataset.{table}'

            # Show all columns of DataFrame in terminal
            pd.set_option('display.max_columns', None)

            # Print DataFrame
            print(df)