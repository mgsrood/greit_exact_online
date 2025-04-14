from datetime import datetime, timedelta

def get_connectors():

    last_year = datetime.now().year - 1

    connectors = {
        "GrootboekMutaties": f"Finnit_Grootboekmutaties?filterfieldids=Boekjaar&filtervalues={last_year}&operatortypes=2",
    }
    
    return connectors

if __name__ == "__main__":
    connectors = get_connectors()
    print(connectors)
