
from afas_modules.get_request import get_request
from modules.env_tool import env_check
import pandas as pd
import os

def afas_main():

    env_check()

    # Define the environment variables
    api_string = os.getenv("API_STRING")
    token = os.getenv("TOKEN")
    endpoint = os.getenv("CONNECTOR")

    print(api_string)
    print(token)
    print(endpoint)


    # Start GET request
    try:
        df = get_request(api_string, token, endpoint)
    except Exception as e:
        print(f"An error occurred: {e}")

    pd.set_option('display.max_columns', None)
    print(df.head())

if __name__ == "__main__":
    afas_main()