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
    authorization_code = 'stampNL001.7Aao%21IAAAAHENOm4FTlO5rYwtTQLqzUcJ18I9NxYZw_WywwnHFBuvwQEAAAETKMv3GeYOGewnR5CNFSOtyGDpmK32KUyv3RH-6rEWvNs7InfqKSC2BPzUZW5PLXcFFT8843z7N0WoCGOoVCdjPJxtDli7D32dRp1NTSZefSTU6WN9zs8IHPItI2r5LIVxeEyd-rPZEbT0rugWkOnPOczeQyyPXjuRrOux5wS6W6iSi15VOQlrCdtwTstd6DtlFcoBR2foFs69S0-ITe43I5u0bHwwP6g2A3hiIGcVvC8LZePcfyikpk0ceL95VQvKSEbqln_40Yv1BvXifDkd0KnX0PWqqw83aWbtklIzvt3oqepXDoh5Q41hbsV5ImYvbj6DRGMh7sgFmN-wrQC1MM_MifeqTf370H0Z0PqeyNkhsa01gPtoGKz28Zo4jFtTcM5YBKD5iikagz1K4pM05CHeHzen8ot_n3SWb_ePBZv3uz1XszrXK1ayRhO-4cEYoxwJPefE9hqG_3eB93P6rlYTMe91e5u9XTs1WIq4GvGXhWbh4-GToul7ZqW-RJqtkvgnPKFBamB0wNd3OcE7hrEVerKXGckp9JwGdigWgPhIrjIoH1PMIg8IDn7yJGcxMj0FZy5CpkkJhlKRB70Q'
    
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
        print(f"\nRefresh Token: {access_token}")

        dotenv_path = find_dotenv()
        set_key(dotenv_path, "REFRESH_TOKEN", refresh_token)
        print("Saved new refresh token to .env file")

    else:
        print(f"\nError: {token_response.status_code} - {token_response.text}")

except requests.exceptions.RequestException as e:
    print(f"\nRequest failed: {e}")
