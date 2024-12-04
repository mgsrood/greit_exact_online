from datetime import datetime, timedelta

def get_connectors():

    last_year = datetime.now() - timedelta(days=365)
    start_date = last_year.replace(day=1, month=1, hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%S")

    connectors = {
        "GrootboekMutaties": f"Finnit_Grootboekmutaties?filterfieldids=Gewijzigd_Op&filtervalues={start_date}&operatortypes=2",
    }
    
    return connectors

if __name__ == "__main__":
    connectors = get_connectors()
    print(connectors)
