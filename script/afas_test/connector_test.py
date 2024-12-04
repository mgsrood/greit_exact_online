
from modules.env_tool import env_check
import requests
import json
import os

def afas_main():

    env_check()

    # Define the environment variables
    api_string = "https://93667.restaccept.afas.online/ProfitRestServices/"
    token = os.getenv("TOKEN")
    
    connectors = ["Finnit_GrootboekRubriek"]
    
    for connector in connectors:

        # Set up headers for authentication
        headers = {
            "Authorization": f"{token}",
            "Content-Type": "application/json"
        }
    
        # Define other parameters
        url = f"{api_string}metainfo/get/{connector}"

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            print(json.dumps(data, indent=4))

if __name__ == "__main__":
    afas_main()