from zeep import Client
from zeep.wsse.username import UsernameToken
from zeep.exceptions import Fault

# WSDL URL
wsdl_url = 'https://api.nmbrs.nl/soap/v3/CompanyService.asmx?WSDL'

domain = "beuklonen"  
username = "kevin@finnit.nl"
token = "f8e3f91ec2e34a85ba5be8389713cfc6"

# Maak de SOAP client aan
client = Client(wsdl_url, wsse=UsernameToken(username, token))

# Definieer de actie die je wilt uitvoeren (bijvoorbeeld 'GetCompanyInfo')
response = client.service.List_GetAll()

# Print de response
print(response)

def get_all_companies(domain, username, token, sandbox=True):
    # Kies de juiste URL (sandbox of productie)
    base_url = "https://api-sandbox.nmbrs.nl" if sandbox else "https://api.nmbrs.nl"
    url = f"{base_url}/soap/v3/CompanyService.asmx?wsdl"

    # Maak een client aan met de WSDL URL
    client = Client(url)

    # De juiste header instellen voor authenticatie
    header = {
        'Username': username,
        'Token': token,
        'Domain': domain
    }

    # Voeg de header toe aan de client
    client.set_ns_prefix('ns', 'https://api.nmbrs.nl/soap/v3/CompanyService')
    client.transport.operation_timeout = 10  # Time-out indien gewenst

    try:
        # Roep de SOAP-methode aan
        response = client.service.List_GetAll(_soapheaders={'AuthHeaderWithDomain': header})
        print("Response:")
        print(response)

    except Fault as e:
        print(f"SOAP Fault: {e}")
        
get_all_companies(domain, username, token, sandbox=False)