from modules.logging import logging
from modules.database import connect_to_database
from modules.config import fetch_configurations
from modules.tokens import get_new_tokens, save_refresh_token, save_access_token
import time
import requests
import re
import pandas as pd
import xml.etree.ElementTree as ET

def clean_xml_string(xml_string):
    """Remove invalid XML characters from the XML string."""
    # Remove invalid XML characters (e.g., control characters like `&#x1F;`)
    return re.sub(r'&#x[0-9A-Fa-f]+;', '', xml_string)

def get_request(division_code, url, endpoint, connection_string, finn_it_connection_string, klantnaam, tabel, script_id, script):
    full_url = f"{url}{endpoint}"
    data = []
    total_rows = 0 

    # Start logging
    print(f"Start GET Request voor tabel: {tabel} | {division_code} ({division_code})")
    logging(finn_it_connection_string, klantnaam, f"Start GET Request", script_id, script, division_code, tabel)
    logging(finn_it_connection_string, klantnaam, f"Ophalen configuratiegegevens", script_id, script, division_code, tabel)

    # Ophalen configuratie gegevens
    config_conn = connect_to_database(connection_string)

    if config_conn:
        cursor = config_conn.cursor()
        config_dict = None
        for attempt in range(3):
            try:
                config_dict = fetch_configurations(cursor)
                if config_dict:
                    break
            except Exception as e:
                time.sleep(5)

        if config_dict is None:
            # Fout logging
            print("Fout bij het ophalen van de configuratiegegevens.")
            logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het ophalen van de configuratiegegevens", script_id, script, division_code, tabel)
            return None
        else:
            # Succes logging
            logging(finn_it_connection_string, klantnaam, f"Configuratiegegevens succesvol opgehaald", script_id, script, division_code, tabel)
    else:
        # Foutmelding logging
        print(f"Fout bij het connecten met de database: {tabel} | {division_code} ({division_code}).")
        logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Fout bij het connecten met de database", script_id, script, division_code, tabel)
        return None

    # Variabelen definiëren
    client_secret = config_dict['client_secret']
    client_id = config_dict['client_id']
    access_token = config_dict['access_token']
    refresh_token = config_dict['refresh_token']
    
    # Request loop
    while full_url:
        payload = {}
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Cookie': f'.AspNetCore.Antiforgery.cdV5uW_Ejgc=CfDJ8CTJVFXxHxJHqt315CGWnt6RmoANCHzuwWq9U7Hje9I3wCAI4LZuudgNgWB6dYyMvEmg32OtzGlkiXWwnahptcAkcALB6KJT_gEvyE6MVNsWYGaWCvjmIDAtTJaRIoAFFsgnc8-ZLrEq13YMkITaGlg; .AspNetCore.Culture=c%3Dnl%7Cuic%3Dnl; ARRAffinity=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ARRAffinitySameSite=f6d71432aed62b959190addf74474ec1e99db2259fa81bdfbcc8a98eae7af7bb; ASP.NET_SessionId=3esxcu3yqpsi31zurmgciobc; ExactOnlineClient=7LjlWUfX5lQbrehxxcOgynPtG4hwzKvhbdRrllvl9LzDBusePiI7bj3hvRnEiEC15zVDQrffSDkobPKR/bxTiiFmlkNu4odr6q55xN5eyQQd6dQzIORCNib+1rPe5uyf55F2LyZLmpDtrMt1lYIyYU+Y8+fr/PyNF2VDkL5wcpQ=; ExactServer{{2bd296c3-cdda-4a9b-9dec-0aad425078e1}}=Division={division_code}'
        }

        # Retry loop voor GET request
        for attempt in range(3):
            response = requests.request("GET", full_url, headers=headers, data=payload)

            # Controleer of de request succesvol was
            if response.status_code == 200:
                # Reinig de XML response van ongewenste tekens
                cleaned_response = clean_xml_string(response.text)

                # Parse de XML-response
                root = ET.fromstring(cleaned_response)

                # Namespace instellen
                namespace = {'ns': 'http://schemas.microsoft.com/ado/2007/08/dataservices'}

                # Check voor ArtikelenExtraVelden
                if tabel == 'ArtikelenExtraVelden':
                    print("Start ArtikelenExtraVelden")
                    # Loop door elk <element> en haal de data op
                    for entry in root.findall('.//ns:element', namespace):
                        item_data = {
                            'ItemID': entry.find('ns:ItemID', namespace).text.strip() if entry.find('ns:ItemID', namespace) is not None else None,
                            'Modified': entry.find('ns:Modified', namespace).text.strip() if entry.find('ns:Modified', namespace) is not None else None,
                            'Number': entry.find('ns:Number', namespace).text.strip() if entry.find('ns:Number', namespace) is not None else None,
                            'Description': entry.find('ns:Description', namespace).text.strip() if entry.find('ns:Description', namespace) is not None else None,
                            'Value': entry.find('ns:Value', namespace).text.strip() if entry.find('ns:Value', namespace) is not None else None
                        }
                        data.append(item_data)
                        total_rows += 1                

                    # Kijk of er een volgende pagina is
                    next_link = root.find('.//{http://www.w3.org/2005/Atom}link[@rel="next"]')
                    full_url = next_link.attrib['href'] if next_link is not None else None

                    # Wacht 1 seconde voordat je de volgende request doet
                    print(f"Totaal aantal opgehaalde rijen: {total_rows}")
                    time.sleep(1)

                else:
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

                    # Wacht 1 seconde voordat je de volgende request doet
                    print(f"Opgehaalde rijen tot nu toe: {total_rows}")
                    time.sleep(1)

                break  # Stop de retry-lus bij een succesvolle response

            elif response.status_code == 503:
                print(f"Server is tijdelijk onbeschikbaar, poging {attempt + 1} van 3. Wacht 5 minuten...")
                time.sleep(300)  # Wacht 5 seconden voordat je opnieuw probeert

            elif response.status_code == 401:
                print(f"Fout bij het ophalen van gegevens: {response.status_code} - {response.text}")
                # Logging
                logging(finn_it_connection_string, klantnaam, f"Ophalen nieuwe access- en refresh token", script_id, script, division_code, tabel)
                
                # Ophalen nieuwe tokens
                oude_refresh_token = refresh_token
                client_id = client_id
                client_secret = client_secret
                new_access_token, new_refresh_token = get_new_tokens(oude_refresh_token, client_id, client_secret, finn_it_connection_string, klantnaam)

                if new_refresh_token:
                    save_refresh_token(connection_string, new_refresh_token)
                    refresh_token = new_refresh_token

                    # Succes logging
                    logging(finn_it_connection_string, klantnaam, f"Nieuwe refresh token successvol opgehaald en opgeslagen", script_id, script, division_code, tabel)

                    # Opslaan nieuwe access token
                    if new_access_token:
                        # Voer hier je verzoek uit met de nieuwe access token
                        save_access_token(connection_string, new_access_token)
                        access_token = new_access_token

                        # Succes logging
                        print(f"Nieuwe access token successvol opgehaald en opgeslagen")
                        logging(finn_it_connection_string, klantnaam, f"Nieuwe access token successvol opgehaald en opgeslagen", script_id, script, division_code, tabel)

                        break
                
                    # Foutmelding logging
                    logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Nieuwe access token niet kunnen ophalen", script_id, script, division_code, tabel)
                    return None
            
            else:
                print(f"Error {response.status_code}: {response.text}")
                logging(finn_it_connection_string, klantnaam, f"FOUTMELDING | Exact Online API foutmelding | Status code: {response.status_code}", script_id, script, division_code, tabel)
                return None

    # Maak een DataFrame van de verzamelde gegevens
    if data:
        df = pd.DataFrame(data)

        # Succes logging
        logging(finn_it_connection_string, klantnaam, f"Data succesvol opgehaald", script_id, script, division_code, tabel)

        return df
    
    else:
        df = pd.DataFrame()

        return df

