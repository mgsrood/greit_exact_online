import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

access_token = os.getenv("ACCESS_TOKEN")
refresh_token = os.getenv("REFRESH_TOKEN")
subscription_key = os.getenv("SUBSCRIPTION_KEY")
employee_id = "4fc1c10d-e611-42a4-963d-2ec5d40451a2"

url = f"https://api.nmbrsapp.com/api/employees/{employee_id}/employments"

headers = {
    "X-Subscription-Key": f"{subscription_key}",
    "Accept": "application/json",
    "Authorization": f"Bearer {access_token}"
}

response = requests.get(url, headers=headers)

print(json.dumps(response.json(), indent=4))
        
        