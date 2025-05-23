import xml.etree.ElementTree as ET
from jinja2 import Template
import xml.dom.minidom
import pandas as pd
import requests
import logging
import time

class SoapManager:
    
    def __init__(self, domain, username, token):
        self.domain = domain
        self.username = username
        self.token = token
        self.base_url = "https://api.nmbrs.nl"
        self.namespace = "https://api.nmbrs.nl/soap/v3/ReportService"
        self.download_namespace = "https://api.nmbrs.nl/soap/v3/Reports"
        self.download_service_path = "Reports.svc"
        self.service_path = "ReportService.asmx"
        self.body_action = "Reports_Accountant_Company_EmployeeWageComponentsPerRunPeriod_Background"
        self.soap_action = f"{self.namespace}/{self.body_action}"
    
    SOAP_TEMPLATE = Template("""<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:rep="{{ namespace }}">
        <soap:Header>
            <rep:AuthHeaderWithDomain>
                <rep:Username>{{ username }}</rep:Username>
                <rep:Token>{{ token }}</rep:Token>
                <rep:Domain>{{ domain }}</rep:Domain>
            </rep:AuthHeaderWithDomain>
        </soap:Header>
        <soap:Body>
            <rep:{{ body_action }}>
                <rep:companyId>{{ company_id }}</rep:companyId>
                <rep:year>{{ year }}</rep:year>
            </rep:{{ body_action }}>
        </soap:Body>
    </soap:Envelope>
    """)
    
    def build_request_body(self, company_id, year):
        return self.SOAP_TEMPLATE.render(
            username=self.username,
            token=self.token,
            domain=self.domain,
            company_id=company_id,
            year=year,
            namespace=self.namespace,
            body_action=self.body_action
        )

    def report_request(self, company_id, year):
        url = f"{self.base_url}/soap/v3/{self.service_path}"
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": self.soap_action
        }

        body = self.build_request_body(company_id, year)

        logging.info(f"Verstuur SOAP request naar {url}")
        response = requests.post(url, data=body.encode("utf-8"), headers=headers)

        if response.status_code == 200:
            root = ET.fromstring(response.text)
            ns = {'rep': self.namespace}

            guid_element = root.find(".//rep:Reports_Accountant_Company_EmployeeWageComponentsPerRunPeriod_BackgroundResult", ns)
            if guid_element is not None and guid_element.text:
                return guid_element.text.strip()
            else:
                logging.error("Geen GUID gevonden in de response")
                return None
        else:
            logging.error(f"Fout bij SOAP request: {response.status_code}\n{response.text}")
            return None

    def poll_report_status(self, task_id):
        """Controleert de status van een background rapportverzoek en geeft resultaat terug als 'Success'."""
        soap_action = f"{self.namespace}/Reports_BackgroundTask_Result"
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": soap_action
        }

        body_template = Template("""<?xml version="1.0" encoding="utf-8"?>
        <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/" xmlns:rep="{{ namespace }}">
        <soap:Header>
            <rep:AuthHeaderWithDomain>
            <rep:Username>{{ username }}</rep:Username>
            <rep:Token>{{ token }}</rep:Token>
            <rep:Domain>{{ domain }}</rep:Domain>
            </rep:AuthHeaderWithDomain>
        </soap:Header>
        <soap:Body>
            <rep:Reports_BackgroundTask_Result>
            <rep:TaskId>{{ task_id }}</rep:TaskId>
            </rep:Reports_BackgroundTask_Result>
        </soap:Body>
        </soap:Envelope>
        """)

        body = body_template.render(
            username=self.username,
            token=self.token,
            domain=self.domain,
            task_id=task_id,
            namespace=self.namespace
        )

        url = f"{self.base_url}/soap/v3/{self.service_path}"
        logging.info(f"Verstuur status-polling request naar {url}")

        response = requests.post(url, data=body.encode("utf-8"), headers=headers)

        if response.status_code == 200:
            root = ET.fromstring(response.text)
            ns = {'rep': self.namespace}

            status_element = root.find(".//rep:Status", ns)
            content_element = root.find(".//rep:Content", ns)

            if status_element is not None:
                status = status_element.text.strip()
                logging.info(f"Status voor task {task_id}: {status}")

                if status == "Success":
                    if content_element is not None and content_element.text:
                        return {
                            "status": "Success",
                            "content": content_element.text.strip()
                        }
                    else:
                        logging.info("Status is 'Success' maar er is geen content.")
                        return {"status": "Success", "content": None}
                return {"status": status, "content": None}
            else:
                logging.error("Kon status niet vinden in de response.")
                return None
        else:
            logging.error(f"Fout bij polling request: {response.status_code}\n{response.text}")
            return None
        
    @staticmethod
    def is_hex(s):
        try:
            int(s[:8], 16)
            return True
        except ValueError:
            return False

    def parse_company_employee_wage_component_report(self, xml_string):
        """
        Zet een NMBRS 'ReportCompanyEmployeeWageComponent' XML-string om in een pandas DataFrame.

        Args:
            xml_string (str): De XML-string van het rapport.

        Returns:
            pd.DataFrame: Een DataFrame met de geëxtraheerde gegevens.
        """
        try:
            # Verwijder de BOM (Byte Order Mark) als die aanwezig is, bv. bij utf-16
            if xml_string.startswith('\\ufeff'):
                xml_string = xml_string[1:]

            root = ET.fromstring(xml_string)
            
            data = []

            # Algemene bedrijfsinfo
            company_id = root.findtext('CompanyID')
            company_name = root.findtext('CompanyName')
            company_number = root.findtext('CompanyNumber')
            period_type = root.findtext('PeriodType')

            employees_list_node = root.find('EmployeesList')
            if employees_list_node is None:
                logging.info("Geen 'EmployeesList' gevonden in de XML-rapportage.")
                return pd.DataFrame()

            for employee_node in employees_list_node.findall('Employees'):
                employee_id = employee_node.findtext('EmployeeID')
                employee_name = employee_node.findtext('EmployeeName')
                employee_number = employee_node.findtext('EmployeeNumber')

                wage_components_list_node = employee_node.find('WageComponentsList')
                if wage_components_list_node is None:
                    logging.debug(f"Werknemer {employee_name} ({employee_id}) heeft geen 'WageComponentsList'.")
                    continue

                for wage_component_node in wage_components_list_node.findall('WageComponents'):
                    component_guid = wage_component_node.findtext('ComponentGuid')
                    component_number = wage_component_node.findtext('ComponentNumber')
                    component_name = wage_component_node.findtext('ComponentName')
                    cumulative_value_component = wage_component_node.findtext('CumulativeValue')

                    values_node = wage_component_node.find('Values')
                    if values_node is None:
                        # Als er geen 'Values' zijn, maar wel een 'WageComponent', kunnen we besluiten
                        # om een record te maken met None voor de Value-specifieke velden,
                        # of deze wage component overslaan voor de "platte" tabel.
                        # Hier kiezen we ervoor om alleen records te maken als er <Value> elementen zijn.
                        logging.debug(f"WageComponent {component_name} ({component_number}) voor werknemer {employee_name} ({employee_id}) "
                                      f"heeft geen 'Values' node. Cumulatieve waarde: {cumulative_value_component}")
                        continue 

                    for value_node in values_node.findall('Value'):
                        component_value = value_node.findtext('ComponentValue')
                        period = value_node.findtext('Period')
                        run = value_node.findtext('Run')

                        record = {
                            'CompanyID': company_id,
                            'CompanyName': company_name,
                            'CompanyNumber': company_number,
                            'PeriodType': period_type,
                            'EmployeeID': employee_id,
                            'EmployeeName': employee_name,
                            'EmployeeNumber': employee_number,
                            'ComponentGuid': component_guid,
                            'ComponentNumber': component_number,
                            'ComponentName': component_name,
                            'CumulativeValue_Component': cumulative_value_component,
                            'ComponentValue': component_value,
                            'Period': period,
                            'Run': run
                        }
                        data.append(record)
            
            df = pd.DataFrame(data)
            return df

        except ET.ParseError as e:
            logging.error(f"Fout bij het parsen van XML voor ReportCompanyEmployeeWageComponent: {e}")
            # Log een deel van de problematische XML
            start_index = max(0, e.offset - 30)
            end_index = min(len(xml_string), e.offset + 30)
            logging.error(f"Problematisch XML (rond positie {e.offset}, lijn {e.lineno}): ...{xml_string[start_index:end_index]}...")
            return pd.DataFrame() 
        except Exception as e:
            logging.error(f"Onverwachte fout bij het converteren van ReportCompanyEmployeeWageComponent XML naar DataFrame: {e}")
            return pd.DataFrame()

    def execute_report_creation(self, company_id, year, max_attempts = 10, poll_interval = 5):
        """
        Vraagt een rapport aan, pollt voor de status, en parseert het resultaat naar een DataFrame.

        Args:
            company_id (str): Het ID van het bedrijf.
            year (str): Het jaar waarvoor het rapport wordt aangevraagd.
            max_attempts (int): Maximaal aantal pogingen om de status te pollen.
            poll_interval (int): Aantal seconden tussen poll pogingen.

        Returns:
            pd.DataFrame: Een DataFrame met de rapportgegevens, of een leeg DataFrame bij een fout.
        """
        task_guid = self.report_request(company_id=company_id, year=year)

        if not task_guid:
            logging.error(f"Kon geen task_guid ontvangen voor company {company_id}, year {year}. Stoppen.")
            return pd.DataFrame()

        logging.info(f"Rapport succesvol aangevraagd met GUID: {task_guid} voor company {company_id}, year {year}")

        for i in range(max_attempts):
            result = self.poll_report_status(task_guid)
            if result:
                status = result.get("status")
                content = result.get("content")
                logging.info(f"Poging {i+1}/{max_attempts}: Status voor task {task_guid} = {status}")

                if status == "Success":
                    if content:
                        df_report = self.parse_company_employee_wage_component_report(content)
                        if not df_report.empty:
                            df_report['Jaar'] = year
                            logging.info(f"Rapport succesvol omgezet naar DataFrame. Shape: {df_report.shape}. 'Jaar' kolom toegevoegd.")
                        else:
                            logging.info("Rapport succesvol opgehaald, maar het parsen resulteerde in een leeg DataFrame.")
                        return df_report
                    else:
                        logging.info("Rapportstatus is 'Success', maar er is geen content beschikbaar.")
                        return pd.DataFrame()
                elif status in ["Executing", "Enqueued"]:
                    if i < max_attempts - 1: # Niet slapen na de laatste poging
                        logging.info(f"Opnieuw proberen over {poll_interval} seconden...")
                        time.sleep(poll_interval)
                    else:
                        logging.info(f"Maximale aantal pogingen ({max_attempts}) bereikt, rapport nog steeds niet 'Success'. Status: {status}")
                        return pd.DataFrame()
                else: # Failed, Cancelled, etc.
                    logging.error(f"Rapportgeneratie gestopt of mislukt. Status: {status}")
                    return pd.DataFrame()
            else:
                logging.error(f"Fout bij ophalen status voor task {task_guid} bij poging {i+1}.")
                if i < max_attempts - 1:
                     time.sleep(poll_interval)
                else:
                    logging.error(f"Maximale aantal pogingen ({max_attempts}) bereikt voor het ophalen van de status.")
                    return pd.DataFrame()
        
        logging.error(f"Rapportverwerking niet succesvol afgerond na {max_attempts} pogingen voor task {task_guid}.")
        return pd.DataFrame()

    def xml_to_dataframe(self, xml_string, element_name, namespace):
        """
        Zet een SOAP XML response om in een pandas DataFrame.
        
        Args:
            xml_string (str): De volledige XML-string van de SOAP-response.
            element_name (str): Het element waarvan je de gegevens wilt extraheren.
            namespace (str): De namespace die moet worden gebruikt bij het zoeken naar de elementen.
        
        Returns:
            pandas DataFrame: Een DataFrame met de geëxtraheerde gegevens.
        """
        try:
            # Verwijder BOM indien aanwezig
            if xml_string.startswith('\ufeff'):
                xml_string = xml_string[1:]
            if xml_string.startswith('ï»¿'): # Voor UTF-8 BOM
                 xml_string = xml_string[3:]

            root = ET.fromstring(xml_string)
            
            namespaces = {'ns': namespace}
            
            items = root.findall(f".//ns:{element_name}", namespaces)
            
            if not items:
                # Probeer zonder namespace prefix als het een default namespace is in het element zelf
                # Moet de namespace in de findall query gebruiken, niet de alias 'ns'
                items_default_ns = root.findall(f".//{{{namespace}}}{element_name}")
                if items_default_ns:
                    items = items_default_ns
                else:
                    logging.info(f"Geen items gevonden voor element '{element_name}' met namespace '{namespace}' (noch met ns: prefix, noch als default namespace). Probeer fallback.")
                    # Probeer het response body element zelf te vinden als fallback (voor simpele list responses)
                    # Aangepaste SOAP namespace voor envelope
                    soap_env_ns = {'soap': 'http://www.w3.org/2003/05/soap-envelope'}
                    if not root.tag.startswith('{http://www.w3.org/2003/05/soap-envelope}'): # Check if root is not already the envelope
                        # If root is not envelope, try to find it. Standard is 'soap:Envelope'
                        # This case should ideally not happen if ET.fromstring(xml_string) works as expected on full response
                        pass # Assuming root IS the envelope or a direct child for now

                    body_node = root.find("soap:Body", soap_env_ns)
                    if body_node is not None:
                        # Zoek naar het eerste kind van Body, dat meestal het 'Result' element is
                        result_container = body_nodefind("*[1]") # Get the first child element regardless of its name
                        if result_container is not None:
                            # Zoek nu naar 'element_name' binnen dit result_container
                            items = result_container.findall(f"{{{namespace}}}{element_name}")
                            if not items:
                                items = result_container.findall(f"ns:{element_name}", namespaces) # Probeer met ns prefix ook nog
                            if not items:
                                logging.info(f"Ook geen items gevonden binnen {result_container.tag}.")
                        else:
                            logging.info("Body node gevonden, maar geen result_container (eerste kind-element).")
                    else:
                        logging.info("SOAP Body node niet gevonden met soap:Body.")

            if not items:
                 logging.info(f"Uiteindelijk geen items gevonden voor '{element_name}' in namespace '{namespace}'. XML-structuur kan afwijken.")
                 return pd.DataFrame()
            
            data = []
            
            for item in items:
                entry = {}
                for child in item:
                    tag_name = child.tag.split('}')[-1] # Haal de lokale naam op
                    entry[tag_name] = child.text
                data.append(entry)
            
            df = pd.DataFrame(data)
            
            # Basic unnesting for simple list of dicts (might need more robust unnesting later)
            for column in df.columns:
                first_valid_entry = next((x for x in df[column] if x is not None), None)
                if isinstance(first_valid_entry, list) and first_valid_entry and isinstance(first_valid_entry[0], dict):
                    try:
                        original_index = df.index # Bewaar de originele index
                        df = df.explode(column, ignore_index=False) # Behoud index tijdens explode
                        
                        # Filter out rows that became None/NaN after explode if original was empty list or list of non-dicts
                        # Then normalize only if there's something to normalize
                        valid_series = df[column].dropna()
                        if not valid_series.empty:
                             normalized_col = pd.json_normalize(valid_series.tolist()).add_prefix(f'{column}_')
                             normalized_col.index = valid_series.index # Zorg voor dezelfde index als valid_series
                             df = df.join(normalized_col, how='left') # Gebruik left join om alle rijen van df te behouden
                        # Drop de originele kolom alleen als er genormaliseerd is of als deze leeg was na explode
                        # Dit voorkomt het verwijderen van de kolom als normalisatie faalt of niet nodig is
                        if not valid_series.empty or df[column].isnull().all():
                             df = df.drop(columns=[column])
                        df = df.reindex(original_index) # Herstel de originele indexvolgorde indien nodig

                    except Exception as e:
                        logging.info(f"Kon kolom {column} niet volledig unnesten/normaliseren: {e}")
            return df
            
        except ET.ParseError as e:
            logging.error(f"Fout bij het parsen van XML: {e}. XML-string: '{xml_string[:200]}...'" )
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Onverwachte fout bij het converteren van XML naar DataFrame: {e}")
            return pd.DataFrame()

    COMPANY_LIST_SOAP_TEMPLATE = Template('''<?xml version="1.0" encoding="utf-8"?>
    <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
      <soap12:Header>
        <AuthHeaderWithDomain xmlns="{{ namespace }}">
          <Username>{{ username }}</Username>
          <Token>{{ token }}</Token>
          <Domain>{{ domain }}</Domain>
        </AuthHeaderWithDomain>
      </soap12:Header>
      <soap12:Body>
        <List_GetAll xmlns="{{ namespace }}" />
      </soap12:Body>
    </soap12:Envelope>''')

    def get_company_list(self):
        """
        Haalt een lijst van alle bedrijven op via de NMBRS CompanyService.

        Returns:
            pd.DataFrame: Een DataFrame met bedrijfsgegevens, of een leeg DataFrame bij een fout.
        """
        service_namespace = "https://api.nmbrs.nl/soap/v3/CompanyService"
        service_path = "CompanyService.asmx"
        # De SOAPAction is de namespace + de operatienaam
        soap_action = f"{service_namespace}/List_GetAll"
        element_name = "Company" # Het te extraheren element in de response

        url = f"{self.base_url}/soap/v3/{service_path}"
        headers = {
            "Content-Type": "application/soap+xml; charset=utf-8", 
            "SOAPAction": soap_action 
        }

        body = self.COMPANY_LIST_SOAP_TEMPLATE.render(
            username=self.username,
            token=self.token,
            domain=self.domain,
            namespace=service_namespace
        )
        
        logging.info(f"Verstuur SOAP request voor Company List_GetAll naar {url}")
        try:
            response = requests.post(url, data=body.encode("utf-8"), headers=headers, timeout=30)
            response.raise_for_status() 

            logging.info(f"Response status: {response.status_code} voor Company List_GetAll")
            if response.status_code == 200 and response.content:
                try:
                    dom = xml.dom.minidom.parseString(response.text)
                    pretty_xml = dom.toprettyxml()
                    logging.debug(f"Company List_GetAll Response XML (eerste 1000 karakters):\n{pretty_xml[:1000]}")
                except Exception as e:
                    logging.info(f"Kon Company List_GetAll response XML niet pretty printen: {e}. Ruwe tekst (eerste 500 kar.): {response.text[:500]}")
                
                df = self.xml_to_dataframe(response.text, element_name, service_namespace)
                if df.empty:
                    logging.info("Company List_GetAll request succesvol, maar resulterende DataFrame is leeg. Controleer XML structuur en parser.")
                else:
                    logging.info(f"Company List_GetAll succesvol omgezet naar DataFrame. Shape: {df.shape}")
                return df
            else:
                # Dit blok wordt mogelijk niet bereikt als raise_for_status() een error werpt voor non-200 codes.
                logging.error(f"Fout bij Company List_GetAll SOAP request: Status {response.status_code}, Content (eerste 500 kar.): {response.text[:500]}")
                return pd.DataFrame()

        except requests.exceptions.HTTPError as http_err:
            # response.text kan hier nuttig zijn voor debugging
            error_content = ""
            if http_err.response is not None:
                error_content = http_err.response.text[:500]
            logging.error(f"HTTP error occurred: {http_err} - {error_content}")
            return pd.DataFrame()
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request error occurred: {req_err}")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Een onverwachte fout trad op tijdens get_company_list: {e}")
            return pd.DataFrame()