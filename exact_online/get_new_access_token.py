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
    authorization_code = 'stampNL001.TffI%21IAAAAPWEgCNH0tIRsDGRtGzA_QxWZqprK8p9m67JAQ32zV01wQEAAAF4nJdz24FPlpIWwBSjrKCr1Y4LsEZZTB5EpvsSQcNDfMVH4U78U7wGjR9e6YG6jZIdrJJcOoatHE4achF-jWirYduX_snayfHxV7wRJcc5DJfEiExznE8jTzkZO41RVhcd0mYh8qbx-HJE6n7UNthmyax8TzKhmFNet_0PiRW7HaDHU0f5H_p83ynPotrLS3t8RP-fpVNX2hbt3itkOMBXgFpol3TUWoIVyKsVcXLuO0_zKRAFEom3sF9GRbx0eEJmyEvEru0eAl5oEzwOj5meVMQkeVbWtXwfip6i52xzW9vFhbIxgPMlj_0k_aJWEHw5U_cQdYP1NTnBdbffAUu-fTc_WAIWeKlPNcmK0Vcq8bcyNaLzaQgH8N-dFDymFvEoFLEFzlK3jEcK2LYVC1qe16I9Yk3oQErtrl4RrC26TJrqrRR_zEdrJZjQJN8frXu6DPqFiuz5-tX1vM-aZNY_zXRX8wah8L9OWJMR4Rn0H52I1-phGNPQKbu6Ab_-4PqerkPYZDGkxEVWtASxRZ04XD4fnAemFaVKnovkfteiWcwVWw1Fp2i4-mEiVavHgkSjv06uOFRAETKCYWFcpAt3'
    
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
        
        # Save refresh token to file
        refresh_token = token_data.get('refresh_token')
        print(f"\nRefresh Token: {access_token}")

        dotenv_path = find_dotenv()
        set_key(dotenv_path, "REFRESH_TOKEN", refresh_token)

    else:
        print(f"\nError: {token_response.status_code} - {token_response.text}")

except requests.exceptions.RequestException as e:
    print(f"\nRequest failed: {e}")
