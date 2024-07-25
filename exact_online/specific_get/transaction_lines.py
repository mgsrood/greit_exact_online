import requests
import xml.etree.ElementTree as ET
import pandas as pd
import os
from dotenv import set_key, load_dotenv, find_dotenv

# Laad de omgevingsvariabelen uit het .env-bestand
load_dotenv()

def load_access_token():
    # Retrieve the access token from the environment
    return os.getenv("ACCESS_TOKEN")

def load_refresh_token():
    # Retrieve the refresh token from the environment
    return os.getenv("REFRESH_TOKEN")

def get_new_tokens(refresh_token):
    # Endpoint voor het verkrijgen van een nieuwe access token
    token_url = "https://start.exactonline.nl/api/oauth2/token"
    
    # Parameters voor het vernieuwen van het token
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": os.getenv("CLIENT_ID"),
        "client_secret": os.getenv("CLIENT_SECRET")
    }
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"  # Vereiste header voor OAuth 2.0
    }

    # Request maken om een nieuwe access token te verkrijgen
    response = requests.post(token_url, data=payload, headers=headers)
    
    # Controleren of het verzoek succesvol was
    if response.status_code == 200:
        # Het JSON-antwoord parsen om de nieuwe access token en refresh token te krijgen
        tokens = response.json()
        new_access_token = tokens["access_token"]
        new_refresh_token = tokens.get("refresh_token", refresh_token)  # Nieuwe refresh token of behoud de oude
        return new_access_token, new_refresh_token
    else:
        # Als het verzoek niet succesvol was, print de foutmelding
        print("Fout bij het vernieuwen van het toegangstoken:", response.text)
        return None, None

def save_refresh_token(refresh_token):
    # Save the new refresh token
    dotenv_path = find_dotenv()
    set_key(dotenv_path, "REFRESH_TOKEN", refresh_token)

def save_access_token(access_token):
    # Save the new refresh token
    dotenv_path = find_dotenv()
    set_key(dotenv_path, "ACCESS_TOKEN", access_token)

def get_request(division_code, url, endpoint):
    access_token = load_access_token()
    full_url = f"{url}{endpoint}"
    data = []

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

        else:
            print(f"Fout bij het ophalen van gegevens: {response.status_code} - {response.text}")
            oude_refresh_token = load_refresh_token()
            new_access_token, new_refresh_token = get_new_tokens(oude_refresh_token)

            if new_refresh_token:
                save_refresh_token(new_refresh_token)
                print("Nieuwe refresh token opgeslagen")

            # Nu kun je de nieuwe access en refresh tokens gebruiken voor je verzoeken
            if new_access_token and new_refresh_token:
                # Voer hier je verzoek uit met de nieuwe access token
                save_access_token(new_access_token)
                print("Nieuwe access token opgeslagen")
                access_token = new_access_token
            else:
                return None
    
    # Maak een DataFrame van de verzamelde gegevens
    df = pd.DataFrame(data)
    return df

def export_to_csv(df, file_path):
    try:
        df.to_csv(file_path, index=False)
        print(f"Data successfully exported to {file_path}")
    except Exception as e:
        print(f"Error exporting to CSV: {e}")

if __name__ == "__main__":
    
    # Laad de divisie code
    division_code = os.getenv("DIVISION_CODE")
    
    # URL en Endpoint
    url = f"https://start.exactonline.nl/api/v1/{division_code}"
    endpoint = "/bulk/Financial/TransactionLines?$select=ID,Account,AmountDC,AmountVATFC,Currency,Date,Description,Division,EntryNumber,FinancialPeriod,FinancialYear,GLAccount,InvoiceNumber,OrderNumber,PaymentReference,VATPercentage"
    
    df = get_request(division_code, url, endpoint)
    if df is not None:
        print(df)
        export_to_csv(df, "transaction_lines.csv")


