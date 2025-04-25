from dotenv import load_dotenv
import os

load_dotenv()

client_id = os.getenv("CLIENT_ID")
redirect_uri = os.getenv("REDIRECT_URI")
state = os.getenv("STATE")
scopes = os.getenv("SCOPES")

print(f"https://identityservice.nmbrs.com/connect/authorize?client_id={client_id}&state={state}&scope={scopes}&response_type=code&redirect_uri={redirect_uri}")