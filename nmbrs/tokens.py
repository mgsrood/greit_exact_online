from urllib.parse import urlencode
import base64
import requests
import os

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Get the values from the environment variables
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
redirect_uri = os.getenv("REDIRECT_URI")
code = os.getenv("CODE")

# URL-encode the client_id and client_secret and base64 encode them for Basic Auth
basic_auth = f"{client_id}:{client_secret}"
encoded_auth = base64.b64encode(basic_auth.encode()).decode()
basicAuthentication = encoded_auth


# Prepare the URL and headers for the POST request
url = "https://identityservice.nmbrs.com/connect/token"

headers = {
    "Authorization": f"Basic {basicAuthentication}",
    "Content-Type": "application/x-www-form-urlencoded"
}

# Prepare the body of the POST request
data = {
    "grant_type": "authorization_code",
    "code": code,
    "redirect_uri": redirect_uri
}

# Make the POST request to exchange the authorization code for an access token
response = requests.post(url, headers=headers, data=data)

# Print the response (access token or error message)
if response.status_code == 200:
    print(response.json())  # Print the response JSON if the status code is 200
else:
    print(f"Error: {response.status_code}")
    print(response.text)