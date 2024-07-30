import requests

def get_new_tokens(refresh_token, client_id, client_secret):
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

def save_refresh_token(customer_config_table_id, new_refresh_token, client_id, client):
    # Save the new refresh token
    update_query_1 = f"""
    UPDATE {customer_config_table_id}
    SET refresh_token = '{new_refresh_token}'
    WHERE client_id = '{client_id}';
    """

    try:
        # Voer de query uit
        query_job = client.query(update_query_1)
        query_job.result()  # Wacht op voltooiing
        print(f"Refresh token voor client_id {client_id} succesvol bijgewerkt.")
    except Exception as e:
        print(f"Fout bij het uitvoeren van de query: {str(e)}")

def save_access_token(customer_config_table_id, new_access_token, client_id, client):
    # Save the new access token
    update_query_2 = f"""
    UPDATE {customer_config_table_id}
    SET access_token = '{new_access_token}'
    WHERE client_id = '{client_id}';
    """

    try:
        # Voer de query uit
        query_job = client.query(update_query_2)
        query_job.result()  # Wacht op voltooiing
        print(f"Access token voor client_id {client_id} succesvol bijgewerkt.")
    except Exception as e:
        print(f"Fout bij het uitvoeren van de query: {str(e)}")