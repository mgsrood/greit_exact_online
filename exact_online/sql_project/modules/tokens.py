import requests
import pyodbc
from datetime import datetime
from modules.logging import logging

def get_new_tokens(refresh_token, client_id, client_secret, finn_it_connection_string, klantnaam):
    # Endpoint voor het verkrijgen van een nieuwe access token
    token_url = "https://start.exactonline.nl/api/oauth2/token"
    
    # Parameters voor het vernieuwen van het token
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret
    }
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"  # Vereiste header voor OAuth 2.0
    }

    # Request maken om een nieuwe access token te verkrijgen
    response = requests.post(token_url, data=payload, headers=headers)
    
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

    # Controleren of het verzoek succesvol was
    if response.status_code == 200:
        # Het JSON-antwoord parsen om de nieuwe access token en refresh token te krijgen
        tokens = response.json()
        new_access_token = tokens["access_token"]
        print(new_access_token)
        new_refresh_token = tokens.get("refresh_token", refresh_token)  # Nieuwe refresh token of behoud de oude
        print(new_refresh_token)
        return new_access_token, new_refresh_token
    else:
        # Als het verzoek niet succesvol was, print de foutmelding
        print("Fout bij het vernieuwen van het toegangstoken:", response.text)
        return None, None


def save_refresh_token(connection_string, new_refresh_token):
    # Save the new refresh token
    try:
        conn = pyodbc.connect(connection_string)
        print("Verbonden met de database!")
    except Exception as e:
        print(f"Fout bij verbinding: {e}")

    # Cursor maken
    cursor = conn.cursor()

    # Query en uitvoering
    query = 'UPDATE Config SET Waarde = ? WHERE Config = ?'
    config = 'refresh_token'
    try:
        cursor.execute(query, (new_refresh_token, config))
        conn.commit()  # Maak de wijziging permanent
        print("Refresh token succesvol bijgewerkt.")
    except Exception as e:
        print(f"Fout bij uitvoeren van de query: {e}")
        conn.rollback()  # Rollback in geval van een fout
    finally:
        conn.close() 

def save_access_token(connection_string, new_access_token):
    # Save the new access token
    try:
        conn = pyodbc.connect(connection_string)
        print("Verbonden met de database!")
    except Exception as e:
        print(f"Fout bij verbinding: {e}")

    # Cursor maken
    cursor = conn.cursor()

    # Query en uitvoering
    query = 'UPDATE Config SET Waarde = ? WHERE Config = ?'
    config = 'access_token'
    try:
        cursor.execute(query, (new_access_token, config))
        conn.commit()  # Maak de wijziging permanent
        print("Refresh token succesvol bijgewerkt.")
    except Exception as e:
        print(f"Fout bij uitvoeren van de query: {e}")
        conn.rollback()  # Rollback in geval van een fout
    finally:
        conn.close() 