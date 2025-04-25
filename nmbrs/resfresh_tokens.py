import requests
import os
from dotenv import load_dotenv

load_dotenv()

url = "https://identityservice.nmbrs.com/connect/token"

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
redirect_uri = os.getenv("REDIRECT_URI")
refresh_token = os.getenv("REFRESH_TOKEN")

payload = {
  'grant_type=': 'refresh_token',
  'redirect_uri': redirect_uri,
  'client_id': client_id,
  'client_secret': client_secret,
  'refresh_token': refresh_token
}

headers = {
  'Content-Type': 'application/x-www-form-urlencoded',
  'Cookie': 'ApplicationGatewayAffinity=93217eb3c3dfd0e80eec38bcab06468b; ApplicationGatewayAffinityCORS=93217eb3c3dfd0e80eec38bcab06468b'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
