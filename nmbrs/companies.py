import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

access_token = os.getenv("ACCESS_TOKEN")
refresh_token = os.getenv("REFRESH_TOKEN")
subscription_key = os.getenv("SUBSCRIPTION_KEY")
debtor_id = "508987b8-a798-4935-93a3-b795c48d4944"

url = f"https://api.nmbrsapp.com/api/debtors/{debtor_id}/companies"

headers = {
    "X-Subscription-Key": f"{subscription_key}",
    "Accept": "application/json",
    "Authorization": f"Bearer {access_token}"
}

response = requests.get(url, headers=headers)

print(json.dumps(response.json(), indent=4))
        
        