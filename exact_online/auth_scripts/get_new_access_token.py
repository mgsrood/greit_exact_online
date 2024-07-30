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
    authorization_code = 'stampNL001.7Aao%21IAAAAEx89qGON_MEpziMAyb0iUwNBuMymOeSGY1ttqSQ7zsNwQEAAAGlbVxKDPcBwdl3qGSxtp7ypiA_aohOwyjH3QdW3zysakbi3f0PwA07HvG6qkvtWg0Kj9vmmXfBNviP4uBfbqvhLKnwhsDHPepxUZY6lU3yDZPeUbWwdel02iRomQD4_9yW3h3YeMrUB8DV3LmpMT9xEE4Gcc5sTEimpuJn5BCDHgIKOfDZuaVftBYzt2jcboR0eE03d7e7PZA_cphr3SBuKTmNmep2E5PbryauS1LoK3wWMXn-Vr2Tpgl3b6kKGtFs-ORhaXaU1H5mQOg8oxBmLkzhA-qhbjxDrTU0pb0NDAf_rFVGSEpOhBmeUsrk7n2mlkr_090MtBbLSsnWES-fi2O-SX1CQ0sLyRjRGvm5Q-XQBJhnMbBs528Z2Ots08J_mQG7hmDl-AhWWZGEQbpEj7AATnQd4wp4I05jzZiaawC6o2H7cKN7eO14zSsF_lro8nVTxPP6aYVM161AvzdqbnHCw7zo2rT23oDFUEaJIvXiSenNV7VCozk3ZTkW9mBFJ239aiETru6oOViDYP8h_ZOzx6_XTjVuPNE-M7Gvqi3nXza_0m5XuXJRPArU2GAN-bp0si3e1wuT9P0BRpW7'
    
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
