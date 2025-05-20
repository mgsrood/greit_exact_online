from zeep import Client

# WSDL van de ReportService
WSDL_URL = "https://api.nmbrs.nl/soap/v3/ReportService.asmx?WSDL"

# Maak de client aan
client = Client(WSDL_URL)

# Print alle beschikbare operaties (methodes)
for service in client.wsdl.services.values():
    print(f"Service: {service.name}")
    for port in service.ports.values():
        operations = port.binding._operations
        for name, operation in operations.items():
            input_signature = operation.input.signature()
            print(f"  - {name}({input_signature})")