def get_connectors(laatste_sync):

    connectors = {
        "Divisions": "Finnit_Divisions",
        "GrootboekMutaties": f"Finnit_Grootboekmutaties?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
        "Grootboekrekening": "Finnit_Grootboekrekening",
        "GrootboekRubriek": "Finnit_GrootboekRubriek",
        "Budget": "Finnit_Budget",
    }
    
    return connectors

if __name__ == "__main__":
    connectors = get_connectors("2016-01-01T00:00:00")
    print(connectors)


filter = "?filterfieldids=Gewijzigd_Op&filtervalues=2016-01-01T00:00:00&operatortypes=2"