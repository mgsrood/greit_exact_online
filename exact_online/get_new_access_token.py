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
    authorization_code = 'stampNL001.TffI%21IAAAAFwcy7YQEfRZTwqJObM9YJGfnmv0jDg03_eSx4jVJTrawQEAAAFUUjScNkb8NptLxU7vUYxN7ObiLheqce4bimvEALlaV8iq-v-l2Or86718vzae644m-RyjoE1hm58qyI8K-Skb-gWc-kMf5EOZBGScNR8SqahOX720VxRysLMqK5bm5Np297kNn2UoHeB9B4m4HP5Zu0Z50kfmeajdrk14EVrjobcJ6gPZiqDSmmua8r2nbrEGASU8X3xH3IME1X3X53rD1iRMG75q8O3N5fFESE4dcX-tsFt-fCJ76PE697tx1i2v_Ik7hv3V-C8Y2s3nw-OlUVneV76_c76X2kxTMkz7JRcXvpzNyBtkKwfIQ5G3zPG1novGHJkd5AN6C_CGPwJxf6A9Cow1GMPdrH8nly86NOmGWi9tkGZd9V_Q54Isz7sVCJw_AE9MF-FyQbzVlIGpEEgKbkq4FgbUtd11DOT0va9_UQdHZjqm8ApyVbCwiyUcd6tehG33AatYHmpfcNrH8d6WXxMwoiL1DW02tvFp1_zglQELXOfBw3wk3c58kXdnxJtUJi4rHJmPZWXBjaUtwuJj21i51I_j6YARVA_sjAASH1Wzo1u_xrmQa_9mZddNFxSbJwVrN2Ud_gILXXCr'
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
        print("Save new access token to .env file")
        
        # Save refresh token to file
        refresh_token = token_data.get('refresh_token')
        print(f"\nRefresh Token: {access_token}")

        dotenv_path = find_dotenv()
        set_key(dotenv_path, "REFRESH_TOKEN", refresh_token)
        print("Save new refresh token to .env file")

    else:
        print(f"\nError: {token_response.status_code} - {token_response.text}")

except requests.exceptions.RequestException as e:
    print(f"\nRequest failed: {e}")
