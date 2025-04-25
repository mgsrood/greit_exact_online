import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

access_token = os.getenv("ACCESS_TOKEN")
refresh_token = os.getenv("REFRESH_TOKEN")
subscription_key = os.getenv("SUBSCRIPTION_KEY")
company_id = "49e1a5cf-54fa-47fd-9510-a6d0c2cc290e"

url = f"https://api.nmbrsapp.com/api/companies/{company_id}/employees"

headers = {
    "X-Subscription-Key": f"{subscription_key}",
    "Accept": "application/json",
    "Authorization": f"Bearer {access_token}"
}

response = requests.get(url, headers=headers)

print(json.dumps(response.json(), indent=4))
        
        