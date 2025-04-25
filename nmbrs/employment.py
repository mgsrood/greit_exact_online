import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

access_token = os.getenv("ACCESS_TOKEN")
refresh_token = os.getenv("REFRESH_TOKEN")
subscription_key = os.getenv("SUBSCRIPTION_KEY")
employee_id = "f2e32e4f-d4b5-4940-b7bd-135614f546c2"
url = f"https://api.nmbrsapp.com/api/employees/{employee_id}/variablehours"

headers = {
    "X-Subscription-Key": f"{subscription_key}",
    "Accept": "application/json",
    "Authorization": f"Bearer {access_token}"
}

response = requests.get(url, headers=headers)

print(json.dumps(response.json(), indent=4))
        
        