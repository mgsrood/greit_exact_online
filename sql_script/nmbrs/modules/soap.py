import xml.dom.minidom
import pandas as pd
import requests
import logging

class SoapManager:
    def __init__(self, config_manager, domain, username, token):
        self.config = config_manager
        self.logger = config_manager.logger
        self.script_name = config_manager.script_name
        self.domain = domain
        self.username = username
        self.token = token

    def soap_requests(self):
        """Retourneert de NMBRS SOAP requests met alle benodigde parameters."""
        return {
            "Debiteuren": {
                "endpoint": "DebtorService",
                "method": "DebtorService/List_GetAll",
                "base_url": "https://api.nmbrs.nl",
                "sandbox": False,
                "soap_action": "https://api.nmbrs.nl/soap/v3/DebtorService/List_GetAll",
                "service_path": "DebtorService.asmx",
                "body_action": "List_GetAll"
            },
            "Bedrijven": {
                "endpoint": "CompanyService",
                "method": "CompanyService/List_GetAll",
                "base_url": "https://api.nmbrs.nl",
                "sandbox": False,
                "soap_action": "https://api.nmbrs.nl/soap/v3/CompanyService/List_GetAll",
                "service_path": "CompanyService.asmx",
                "body_action": "List_GetAll"
            }
        }
    
    def get_soap_parameters(self, service_name):
        """Retourneert de benodigde parameters voor een specifieke SOAP-service."""
        requests = self.soap_requests()
        if service_name not in requests:
            logging.error(f"Service '{service_name}' niet gevonden.")
            return None

        service = requests[service_name]
        
        return {
            "domain": self.domain,
            "username": self.username,
            "token": self.token,
            "endpoint": service['endpoint'],
            "soap_action": service['soap_action'],
            "service_path": service['service_path'],
            "body_action": service['body_action'],
            "base_url": service['base_url'],
            "sandbox": service['sandbox']
        }
    
    def soap_request(self, service_name, domain, username, token, soap_action, endpoint, service_path, body_action, base_url=None, sandbox=False):
        """
        Haalt de gegevens op uit de NMBRS API met de SOAP request.
        
        Parameters:
            - domain (str): Het domein waarvoor de gegevens opgevraagd worden.
            - username (str): De gebruikersnaam voor authenticatie.
            - token (str): Het token voor authenticatie.
            - soap_action (str): De SOAPAction header voor de request.
            - service_path (str): Het pad naar de service.
            - body_action (str): De actie in de body van de SOAP-request.
            - base_url (str): De basis-URL van de API (indien leeg, wordt de sandbox/ productie-URL gebruikt).
            - sandbox (bool): Of het sandbox- of productie-omgeving betreft (standaard False voor sandbox).
            
        Returns:
            - Response van de API-call.
        """
        # Kies de juiste basis-URL, of gebruik de meegegeven base_url
        if not base_url:
            base_url = "https://api-sandbox.nmbrs.nl" if sandbox else "https://api.nmbrs.nl"
        
        # Default SOAPAction als deze niet is meegegeven
        if not soap_action:
            soap_action = f"{base_url}/soap/v3/{service_path}/{body_action}"

        # Maak de URL voor de SOAP request
        url = f"{base_url}/soap/v3/{service_path}"
        print(url)
        # Maak de headers voor de SOAP request
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": soap_action
        }
        print(headers)
        # Maak de body voor de SOAP request
        body = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                       xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                       xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
          <soap:Header>
            <AuthHeaderWithDomain xmlns="{base_url}/soap/v3/{endpoint}">
              <Username>{username}</Username>
              <Token>{token}</Token>
              <Domain>{domain}</Domain>
            </AuthHeaderWithDomain>
          </soap:Header>
          <soap:Body>
            <{body_action} xmlns="{base_url}/soap/v3/{endpoint}" />
          </soap:Body>
        </soap:Envelope>
        """
        print(body)
        # Verstuur de request naar de API
        logging.info(f"SOAP request versturen voor {service_name}")
        response = requests.post(url, data=body, headers=headers)

        # Verwerk de response
        if response.status_code == 200:
            dom = xml.dom.minidom.parseString(response.text)
            pretty_xml_as_string = "\n".join([line for line in dom.toprettyxml().split('\n') if line.strip()])
            return pretty_xml_as_string
        else:
            logging.error(f"Fout bij het versturen van de SOAP request voor {service_name}: {response.status_code}: {response.text}")
            return None
    
    def execute_soap_request(self, service_name):
        """Voert de SOAP request uit voor de opgegeven service."""
        service_params = self.get_soap_parameters(service_name)
        if service_params:
            return self.soap_request(
                service_name,
                service_params["domain"],
                service_params["username"],
                service_params["token"],
                service_params["soap_action"],
                service_params["endpoint"],
                service_params["service_path"],
                service_params["body_action"],
                service_params["base_url"],
                service_params["sandbox"]
            )