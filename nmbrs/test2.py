import requests
import xml.dom.minidom
def get_all_debtors(domain, username, token, sandbox=True):
    # Kies de juiste URL (sandbox of productie)
    base_url = "https://api-sandbox.nmbrs.nl" if sandbox else "https://api.nmbrs.nl"
    url = f"{base_url}/soap/v3/DebtorService.asmx"
    
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "https://api.nmbrs.nl/soap/v3/DebtorService/List_GetAll"
    }

    body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                   xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Header>
        <AuthHeaderWithDomain xmlns="https://api.nmbrs.nl/soap/v3/DebtorService">
          <Username>{username}</Username>
          <Token>{token}</Token>
          <Domain>{domain}</Domain>
        </AuthHeaderWithDomain>
      </soap:Header>
      <soap:Body>
        <List_GetAll xmlns="https://api.nmbrs.nl/soap/v3/DebtorService" />
      </soap:Body>
    </soap:Envelope>
    """

    response = requests.post(url, data=body, headers=headers)

    if response.status_code == 200:
        print("Response:")
        print(response.text)
    else:
        print(f"Error {response.status_code}: {response.text}")

def get_all_companies(domain, username, token, sandbox=True):
    # Kies de juiste URL (sandbox of productie)
    base_url = "https://api-sandbox.nmbrs.nl" if sandbox else "https://api.nmbrs.nl"
    url = f"{base_url}/soap/v3/CompanyService.asmx"

    headers = {
        "Content-Type": "text/xml; charset=utf-8",
        "SOAPAction": "https://api.nmbrs.nl/soap/v3/CompanyService/List_GetAll"
    }

    body = f"""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                   xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
      <soap:Header>
        <AuthHeaderWithDomain xmlns="https://api.nmbrs.nl/soap/v3/CompanyService">
          <Username>{username}</Username>
          <Token>{token}</Token>
          <Domain>{domain}</Domain>
        </AuthHeaderWithDomain>
      </soap:Header>
      <soap:Body>
        <List_GetAll xmlns="https://api.nmbrs.nl/soap/v3/CompanyService" />
      </soap:Body>
    </soap:Envelope>
    """

    response = requests.post(url, data=body, headers=headers)

    if response.status_code == 200:
        print("Response:")
        pretty_print_xml(response.text)
    else:
        print(f"Error {response.status_code}: {response.text}")

def pretty_print_xml(xml_string):
    dom = xml.dom.minidom.parseString(xml_string)
    pretty_xml_as_string = "\n".join([line for line in dom.toprettyxml().split('\n') if line.strip()])
    print(pretty_xml_as_string)

# Vul hieronder je gegevens in
domain = "beuklonen"         # Bijvoorbeeld: greit
username = "kevin@finnit.nl"
token = "f8e3f91ec2e34a85ba5be8389713cfc6"

# Roep de functie aan
get_all_debtors(domain, username, token, sandbox=False)
get_all_companies(domain, username, token, sandbox=False)
