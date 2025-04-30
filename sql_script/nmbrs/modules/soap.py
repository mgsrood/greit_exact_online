import xml.etree.ElementTree as ET
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
            "Bedrijven": {
                "endpoint": "CompanyService",
                "method": "CompanyService/List_GetAll",
                "base_url": "https://api.nmbrs.nl",
                "sandbox": False,
                "soap_action": "https://api.nmbrs.nl/soap/v3/CompanyService/List_GetAll",
                "service_path": "CompanyService.asmx",
                "body_action": "List_GetAll",
                "element_name": "Company",
                "namespace": "https://api.nmbrs.nl/soap/v3/CompanyService",
                "id_necessary": False,
                "period_necessary": True,
                "year_necessary": True,
                "werknemer_id_necessary": False
            },
            "Werknemers": {
                "endpoint": "EmployeeService",
                "method": "EmployeeService/Function_GetAll_AllEmployeesByCompany_V2",
                "base_url": "https://api.nmbrs.nl",
                "sandbox": False,
                "soap_action": "https://api.nmbrs.nl/soap/v3/EmployeeService/Function_GetAll_AllEmployeesByCompany_V2",
                "service_path": "EmployeeService.asmx",
                "body_action": "Function_GetAll_AllEmployeesByCompany_V2",
                "element_name": "EmployeeFunctionItem_V2",
                "namespace": "https://api.nmbrs.nl/soap/v3/EmployeeService",
                "id_necessary": True,  
                "period_necessary": False,  
                "year_necessary": False,
                "werknemer_id_necessary": False
            },
            "Uurcodes": {
                "endpoint": "CompanyService",
                "method": "CompanyService/HourModel2_GetHourCodes",
                "base_url": "https://api.nmbrs.nl",
                "sandbox": False,
                "soap_action": "https://api.nmbrs.nl/soap/v3/CompanyService/HourModel2_GetHourCodes",
                "service_path": "CompanyService.asmx",
                "body_action": "HourModel2_GetHourCodes",
                "element_name": "HourCode",
                "namespace": "https://api.nmbrs.nl/soap/v3/CompanyService",
                "id_necessary": True,
                "period_necessary": False,
                "year_necessary": False,
                "werknemer_id_necessary": False
            },
            "Uurcodes_2": {
                "endpoint": "CompanyService",
                "method": "CompanyService/HourModel_GetHourCodes",
                "base_url": "https://api.nmbrs.nl",
                "sandbox": False,
                "soap_action": "https://api.nmbrs.nl/soap/v3/CompanyService/HourModel_GetHourCodes",
                "service_path": "CompanyService.asmx",
                "body_action": "HourModel_GetHourCodes",
                "element_name": "HourCode",
                "namespace": "https://api.nmbrs.nl/soap/v3/CompanyService",
                "id_necessary": True,
                "period_necessary": False,
                "year_necessary": False,
                "werknemer_id_necessary": False
            },
            "Uren_Vast": {
                "endpoint": "EmployeeService",
                "method": "EmployeeService/WageComponentFixed_Get",
                "base_url": "https://api.nmbrs.nl",
                "sandbox": False,
                "soap_action": "https://api.nmbrs.nl/soap/v3/EmployeeService/WageComponentFixed_Get",
                "service_path": "EmployeeService.asmx",
                "body_action": "WageComponentFixed_Get",
                "element_name": "WageComponent",
                "namespace": "https://api.nmbrs.nl/soap/v3/EmployeeService",
                "id_necessary": False,
                "period_necessary": True,
                "year_necessary": True,
                "werknemer_id_necessary": True
            },
            "Uren_Variabel": {
                "endpoint": "EmployeeService",
                "method": "EmployeeService/WageComponentVar_Get",
                "base_url": "https://api.nmbrs.nl",
                "sandbox": False,
                "soap_action": "https://api.nmbrs.nl/soap/v3/EmployeeService/WageComponentVar_Get",
                "service_path": "EmployeeService.asmx",
                "body_action": "WageComponentVar_Get",
                "element_name": "WageComponent",
                "namespace": "https://api.nmbrs.nl/soap/v3/EmployeeService",
                "id_necessary": False,
                "period_necessary": True,
                "year_necessary": True,
                "werknemer_id_necessary": True
            },
            "Uren_Schemas": {
                "endpoint": "EmployeeService",
                "method": "EmployeeService/Schedule_Get",
                "base_url": "https://api.nmbrs.nl",
                "sandbox": False,
                "soap_action": "https://api.nmbrs.nl/soap/v3/EmployeeService/Schedule_Get",
                "service_path": "EmployeeService.asmx",
                "body_action": "Schedule_Get",
                "element_name": "Schedule_GetResult",
                "namespace": "https://api.nmbrs.nl/soap/v3/EmployeeService",
                "id_necessary": False,
                "period_necessary": True,
                "year_necessary": True,
                "werknemer_id_necessary": True
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
            "sandbox": service['sandbox'],
            "element_name": service['element_name'],
            "namespace": service['namespace'],
            "id_necessary": service['id_necessary'],
            "period_necessary": service['period_necessary'],
            "year_necessary": service['year_necessary'],
            "werknemer_id_necessary": service['werknemer_id_necessary']
        }
    
    def soap_request(self, service_name, domain, username, token, soap_action, endpoint, service_path, body_action, element_name, namespace, id_necessary=False, company_id=None, period_necessary=False, period=None, year_necessary=False, year=None, werknemer_id_necessary=False, werknemer_id=None, base_url=None, sandbox=False):
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

        # Bepaal de juiste namespace prefix op basis van de endpoint
        endpoint_config = {
            "CompanyService": {
                "prefix": "com",
                "id_parameter": "CompanyId"
            },
            "EmployeeService": {
                "prefix": "emp",
                "id_parameter": "CompanyID"
            },
            "DebtorService": {
                "prefix": "deb",
                "id_parameter": None
            }
        }
        
        config = endpoint_config.get(endpoint, {"prefix": "com", "id_parameter": "CompanyId"})
        namespace_prefix = config["prefix"]
        id_parameter = config["id_parameter"]

        # Maak de URL voor de SOAP request
        url = f"{base_url}/soap/v3/{service_path}"

        # Maak de headers voor de SOAP request
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": soap_action
        }

        # Bouw de body voor de SOAP request
        body = f"""<?xml version="1.0" encoding="utf-8"?>
        <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                        xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                        xmlns:soap12="http://www.w3.org/2003/05/soap-envelope"
                        xmlns:{namespace_prefix}="{base_url}/soap/v3/{endpoint}">
        <soap12:Header>
            <{namespace_prefix}:AuthHeaderWithDomain>
            <{namespace_prefix}:Username>{username}</{namespace_prefix}:Username>
            <{namespace_prefix}:Token>{token}</{namespace_prefix}:Token>
            <{namespace_prefix}:Domain>{domain}</{namespace_prefix}:Domain>
            </{namespace_prefix}:AuthHeaderWithDomain>
        </soap12:Header>
        <soap12:Body>
            <{namespace_prefix}:{body_action}>
        """
        
        # Voeg de CompanyID toe als deze nodig is
        if id_necessary:
            if company_id is None:
                logging.error(f"CompanyID is vereist voor service {service_name} maar is niet opgegeven")
                return None
            if id_parameter is None:
                logging.error(f"ID parameter is niet gedefinieerd voor service {service_name}")
                return None
            body += f"<{namespace_prefix}:{id_parameter}>{company_id}</{namespace_prefix}:{id_parameter}>"
        
        # Voeg de WerknemerID toe als deze nodig is
        if werknemer_id_necessary:
            if werknemer_id is None:
                logging.error(f"WerknemerID is vereist voor service {service_name} maar is niet opgegeven")
                return None
            body += f"<{namespace_prefix}:EmployeeId>{werknemer_id}</{namespace_prefix}:EmployeeId>"
        
        # Voeg de Period toe als deze nodig is
        if period_necessary:
            body += f"<{namespace_prefix}:Period>{period}</{namespace_prefix}:Period>"
        
        # Voeg de Year toe als deze nodig is
        if year_necessary:
            body += f"<{namespace_prefix}:Year>{year}</{namespace_prefix}:Year>"
        
        # Sluit de body en envelop
        body += f"""
            </{namespace_prefix}:{body_action}>
        </soap12:Body>
        </soap12:Envelope>
        """

        # Verstuur de request naar de API
        logging.info(f"SOAP request versturen voor {service_name}")
        response = requests.post(url, data=body, headers=headers)

        # Verwerk de response
        if response.status_code == 200:
            # Print XML response
            dom = xml.dom.minidom.parseString(response.text)
            pretty_xml_as_string = "\n".join([line for line in dom.toprettyxml().split('\n') if line.strip()])
            """print(f"Pretty XML: {pretty_xml_as_string}")"""
            
            df = self.xml_to_dataframe(response.text, element_name, namespace)
            return df
        else:
            logging.error(f"Fout bij het versturen van de SOAP request voor {service_name}: {response.status_code}: {response.text}")
            return None
    
    def execute_soap_request(self, service_name, company_id=None, period=None, year=None, werknemer_id=None):
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
                service_params["element_name"],
                service_params["namespace"],
                service_params["id_necessary"],
                company_id,
                service_params["period_necessary"],
                period,
                service_params["year_necessary"],
                year,
                service_params["werknemer_id_necessary"],
                werknemer_id,
                service_params["base_url"],
                service_params["sandbox"],
            )

    def xml_to_dataframe(self, xml_string, element_name, namespace):
        """
        Zet een SOAP XML response om in een pandas DataFrame.
        
        Parameters:
            - xml_string (str): De volledige XML-string van de SOAP-response.
            - element_name (str): Het element waarvan je de gegevens wilt extraheren (bijv. 'EmployeeFunctionItem_V2').
            - namespace (str): De namespace die moet worden gebruikt bij het zoeken naar de elementen.
        
        Returns:
            - pandas DataFrame: Een DataFrame met de geëxtraheerde gegevens.
        """
        try:
            # Parse de XML string
            root = ET.fromstring(xml_string)
            
            # Verkrijg de namespace van de XML
            namespaces = {'ns': namespace}  # Pas deze aan afhankelijk van de service
            
            # Zoek het hoofdelement waar we de gegevens willen extraheren
            items = root.findall(f".//ns:{element_name}", namespaces)
            
            # Controleer of er items zijn om te verwerken
            if not items:
                logging.info(f"Geen items gevonden voor element '{element_name}' met namespace '{namespace}'")
                return pd.DataFrame()  # Return een lege DataFrame
            
            # Lijst om de gegevens in op te slaan
            data = []
            
            # Voor elk item (bijvoorbeeld EmployeeFunctionItem_V2), haal de relevante gegevens op
            for item in items:
                entry = {}
                for child in item:
                    tag_name = child.tag.split('}')[1] if '}' in child.tag else child.tag
                    entry[tag_name] = child.text
                
                # Controleer of er geneste EmployeeFunctions zijn en verwerk deze
                employee_functions = item.findall(".//ns:EmployeeFunctions/ns:EmployeeFunction", namespaces)
                if employee_functions:
                    functions_data = []
                    for emp_func in employee_functions:
                        # Controleer of de EmployeeFunction leeg is (met xsi:nil="true")
                        function_data = {}
                        for func_child in emp_func:
                            # Verwerk de gegevens, maar sla lege elementen over
                            tag_name = func_child.tag.split('}')[1] if '}' in func_child.tag else func_child.tag
                            function_data[tag_name] = func_child.text if func_child.text else None
                        functions_data.append(function_data)
                    entry["EmployeeFunctions"] = functions_data
                
                # Voeg de entry toe aan de lijst van data
                data.append(entry)
            
            # Zet de gegevens om in een pandas DataFrame
            df = pd.DataFrame(data)
            
            # Ontvouwt geneste kolommen
            for column in df.columns:
                if isinstance(df[column].iloc[0], list) and isinstance(df[column].iloc[0][0], dict):
                    df = df.explode(column, ignore_index=True)
                    df = df.join(pd.json_normalize(df[column]).add_prefix(f'{column}_'))
                    df = df.drop(columns=[column])
            
            return df
            
        except ET.ParseError as e:
            logging.error(f"Fout bij het parsen van XML: {e}")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Onverwachte fout bij het converteren van XML naar DataFrame: {e}")
            return pd.DataFrame()