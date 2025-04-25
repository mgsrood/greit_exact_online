import requests
from urllib.parse import unquote
from dotenv import set_key, load_dotenv, find_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Exact Online OAuth endpoints
token_url = 'https://start.exactonline.nl/api/oauth2/token'

# Client credentials (replace with your own)
client_id = "4c826c24-1563-4154-9404-31cf0e622f0a"
client_secret = "vBTk6SJh6g45"
redirect_uri = "https://finnit.nl/"
try:
    # Manually retrieve authorization code from user input
    authorization_code = 'stampNL001.PIAy%21IAAAAMzxRHEfvUjf1YdcEo49uDxsyh5O0u5bT0tckEG93WPz4QEAAAHH_HySixlCb8e23hh7lsVqgVL4zYZYGlagGJShm_cfg54jQr0szllxd924nqZKr3Qwy-ix53XBh0VxVuu1wU4_Dx5aeKeh4yXNWin6WWdznezGkphw4uj40jfF6IAMKApP2ny1WN9_TXynem9GlubmVujUS8VXWH5kdfarHFNm8gPrreFi4cZ42-jcidDmBlZnP6iQyI8NDDW3vCNOoS2UmMO6TnkVcm9Q22INaNgjpprsvhINRvaBjK8AC15qWNDTMY404_I9tpLcfeJhRD0GEONOWzqaOK5AX4M-1sjzLK4JDC1mnF5IsEWxqCdBrDwmILbtuiD1J4zFRs36ffPe7qext5QRhcsqoek3y9N9GZ_FTBQLQuwLjNoPxprrTL6riI5ONXsizgb8KTtt6PPduqnXTRkYTWR0Md8orv4XMyLoHle-vHMa7Hy09TrYCiVw6T9RAbppijEGzRHNIKY-FM_XZdWrIyROE1mUcG_ylyecVvYnuh3TUIxz1ech2VQa6URd1WjtwkIpz792Bm0fjcmtkIrv5ulHhKVXJRS-SbWe0LkYNQPCSQLcm1C5t1jz_uyPVZL_4MqoGrR2kvSV9kOZwGc7HB0Qg1Uc33CmBdDFOujZtkSXUALrDBdudss'
    
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
