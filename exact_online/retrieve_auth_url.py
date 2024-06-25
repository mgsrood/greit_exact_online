import requests
from urllib.parse import urlencode
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Exact Online OAuth endpoints
authorize_url = 'https://start.exactonline.nl/api/oauth2/auth'

# Client credentials (replace with your own)
client_id = os.getenv('CLIENT_ID')
redirect_uri = os.getenv('REDIRECT_URI') 

# Parameters for authorization request
params = {
    'client_id': client_id,
    'redirect_uri': redirect_uri,
    'response_type': 'code',
    'force_login': '1'  # Optional: Forces user login
}

# Step 1: Construct and print the authorization URL
auth_url = f"{authorize_url}?{urlencode(params)}"
print(f"Visit this URL and authorize access:\n{auth_url}")
