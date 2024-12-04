import requests
from urllib.parse import unquote
from dotenv import set_key, load_dotenv, find_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Exact Online OAuth endpoints
token_url = 'https://start.exactonline.nl/api/oauth2/token'

# Client credentials (replace with your own)
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
redirect_uri = os.getenv('REDIRECT_URI')
try:
    # Manually retrieve authorization code from user input
    authorization_code = 'stampNL001.V2LA%21IAAAAIMj3Mt2MoYfvWkoAC7cNiXX8gKXLeJxjRVSHc5MCLrfwQEAAAF5m_yra6V2qMO5j2onlKi2-S4N4Mg1Q8_PrjNPlymECgtti-3W5e5h-m6NKDDYTXkQIlK719UOQONK1cg9Q8NojkEUM74qWe8Ba7Hk7lXHslm4NavByyRibGbCinTYFxfqpixkfbeMVXBKoFQjcwdqiMrqS0CP_Gcaid3MXFbR6nXbcTbhui6EJ-C_GgrzYILEaqgtV_aew3UONnvpznuDRQD8G7UgtBuYA_mnIF5u01oETsiysoME9-9TuX4L6WBdIJR6H7dPfgXI751TOCQSu5cuYUyRxctsr3vusP-Nm2uzbiyDnQnE-OTQ6bQaptOOdEHX_0PnkkV3CN77r0KubawjaqUokdBAhUmWcpg5qW-evTBRiuj57kBMLeZ-8Q97o1_-0K-vT6utBsFNN1-vkk1Pn3ZR9ObQ8__cHg-gTNWwrZjtsuoyMuLZLpkfVwwViIdDnra3lGVo4yZSUGOrOo8-nQjE5hH1n53PA4YXcwY2cLorWXFlefS_qoLhgKZ3xf4rlQW4ctuwDH5xhA6SkRvpGmd0M1Z93xImragYBxhBl5WWE77tGEQJ8Pz5y2WcaQrZm125vzAveeCv80m6'
    
    authorization_code = unquote(authorization_code)

    # Exchange authorization code for access token
    token_payload = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'authorization_code',
        'code': authorization_code,
        'redirect_uri': redirect_uri
    }

    # Requesting the token
    token_response = requests.post(token_url, data=token_payload)

    # Check if request was successful (status code 200)
    if token_response.status_code == 200:
        # Extract access token from response
        token_data = token_response.json()

        access_token = token_data['access_token']
        print(f"\nAccess Token: {access_token}")

        # Save access token to file
        dotenv_path = find_dotenv()
        set_key(dotenv_path, "ACCESS_TOKEN", access_token)
        print("Saved new access token to .env file")
        
        # Save refresh token to file
        refresh_token = token_data.get('refresh_token')
        print(f"\nRefresh Token: {refresh_token}")

        dotenv_path = find_dotenv()
        set_key(dotenv_path, "REFRESH_TOKEN", refresh_token)
        print("Saved new refresh token to .env file")

    else:
        print(f"\nError: {token_response.status_code} - {token_response.text}")

except requests.exceptions.RequestException as e:
    print(f"\nRequest failed: {e}")
