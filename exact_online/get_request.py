import requests
import xml.etree.ElementTree as ET
import pandas as pd
import os
from dotenv import set_key, load_dotenv, find_dotenv

# Load environment variables from .env file
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

url = "https://start.exactonline.nl/api/v1/3356148/payroll/Employees?$select=FullName,Nationality,BusinessEmail"
access_token = load_access_token()

payload = {}
headers = {
'Authorization': f'Bearer {access_token}',
'Cookie': '.AspNetCore.Antiforgery.cdV5uW_Ejgc=CfDJ8CTJVFXxHxJHqt315CGWnt6RmoANCHzuwWq9U7Hje9I3wCAI4LZuudgNgWB6dYyMvEmg32OtzGlkiXWwnahptcAkcALB6KJT_gEvyE6MVNsWYGaWCvjmIDAtTJaRIoAFFsgnc8-ZLrEq13YMkITaGlg; .AspNetCore.Culture=c%3Dnl%7Cuic%3Dnl; ARRAffinity=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ARRAffinitySameSite=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ASP.NET_SessionId=3esxcu3yqpsi31zurmgciobc; ExactOnlineClient=7LjlWUfX5lQbrehxxcOgynPtG4hwzKvhbdRrllvl9LzDBusePiI7bj3hvRnEiEC15zVDQrffSDkobPKR/bxTiiFmlkNu4odr6q55xN5eyQQd6dQzIORCNib+1rPe5uyf55F2LyZLmpDtrMt1lYIyYU+Y8+fr/PyNF2VDkL5wcpQ=; ExactServer{2bd296c3-cdda-4a9b-9dec-0aad425078e1}=Division=3356148'
}

response = requests.request("GET", url, headers=headers, data=payload)

# Controleer of de request succesvol was
if response.status_code == 200:
    # Parse de XML-response
    root = ET.fromstring(response.content)
    data = []

    # Loop door elk <entry> element en haal de naam en nationaliteit op
    for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
        properties = entry.find('{http://www.w3.org/2005/Atom}content').find('{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}properties')
        name = properties.find('{http://schemas.microsoft.com/ado/2007/08/dataservices}FullName').text
        nationality = properties.find('{http://schemas.microsoft.com/ado/2007/08/dataservices}Nationality').text.strip()
        data.append({"Name": name, "Nationality": nationality})

    # Maak een DataFrame van de verzamelde gegevens
    df = pd.DataFrame(data)
    print(df)

else: 
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
        
        payload = {}
        headers = {
        'Authorization': f'Bearer {access_token}',
        'Cookie': '.AspNetCore.Antiforgery.cdV5uW_Ejgc=CfDJ8CTJVFXxHxJHqt315CGWnt6RmoANCHzuwWq9U7Hje9I3wCAI4LZuudgNgWB6dYyMvEmg32OtzGlkiXWwnahptcAkcALB6KJT_gEvyE6MVNsWYGaWCvjmIDAtTJaRIoAFFsgnc8-ZLrEq13YMkITaGlg; .AspNetCore.Culture=c%3Dnl%7Cuic%3Dnl; ARRAffinity=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ARRAffinitySameSite=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ASP.NET_SessionId=3esxcu3yqpsi31zurmgciobc; ExactOnlineClient=7LjlWUfX5lQbrehxxcOgynPtG4hwzKvhbdRrllvl9LzDBusePiI7bj3hvRnEiEC15zVDQrffSDkobPKR/bxTiiFmlkNu4odr6q55xN5eyQQd6dQzIORCNib+1rPe5uyf55F2LyZLmpDtrMt1lYIyYU+Y8+fr/PyNF2VDkL5wcpQ=; ExactServer{2bd296c3-cdda-4a9b-9dec-0aad425078e1}=Division=3356148'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        # Controleer of de request succesvol was
        if response.status_code == 200:
            # Parse de XML-response
            root = ET.fromstring(response.content)
            data = []

            # Loop door elk <entry> element en haal de naam en nationaliteit op
            for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
                properties = entry.find('{http://www.w3.org/2005/Atom}content').find('{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}properties')
                name = properties.find('{http://schemas.microsoft.com/ado/2007/08/dataservices}FullName').text
                nationality = properties.find('{http://schemas.microsoft.com/ado/2007/08/dataservices}Nationality').text.strip()
                data.append({"Name": name, "Nationality": nationality})

            # Maak een DataFrame van de verzamelde gegevens
            df = pd.DataFrame(data)
            print(df)