import requests
import logging
import xml.etree.ElementTree as ET
import xml.dom.minidom
from jinja2 import Template
import logging
import base64
import time

class SoapManager:
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
            dom = xml.dom.minidom.parseString(response.text)
            pretty_xml = dom.toprettyxml()
            logging.info(f"Response:\n{pretty_xml}")

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
                        logging.warning("Status is 'Success' maar er is geen content.")
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

if __name__ == "__main__":
    

    soap_manager = SoapManager(DOMAIN, USERNAME, TOKEN)
    task_id = soap_manager.report_request(company_id=32885, year=2022)

    if task_id:
        print(f"Task aangemaakt met ID: {task_id}")
        
        for i in range(10):  # max 10 pogingen, 5 seconden tussen elke
            result = soap_manager.poll_report_status(task_id)
            if result:
                if result["status"] == "Success":
                    with open("output.xml", "w", encoding="utf-8") as f:
                        f.write(result["content"])
                    print("Rapport succesvol opgehaald en opgeslagen als output.xml")
                    break
                elif result["status"] in ["Executing", "Enqueued"]:
                    print(f"Poging {i+1}: Status = {result['status']}, opnieuw proberen...")
                else:
                    print(f"Poging {i+1}: Status = {result['status']}, stoppen.")
                    break
            else:
                print("Fout bij ophalen status.")
                break
            time.sleep(5)
    else:
        print("Rapportgeneratie is mislukt.")