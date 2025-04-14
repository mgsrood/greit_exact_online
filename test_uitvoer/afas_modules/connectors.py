def get_connectors(laatste_sync):

    connectors = {
        "Divisions": "Finnit_Divisions",
        "GrootboekMutaties": f"Finnit_Grootboekmutaties?filterfieldids=Gewijzigd_Op&filtervalues={laatste_sync}&operatortypes=2",
        "Grootboekrekening": "Finnit_Grootboekrekening",
        "GrootboekRubriek": "Finnit_GrootboekRubriek",
        "Budget": "Finnit_Budget",
        "BudgetProjecten": "Finnit_BudgetProjecten",
        "Urenregistratie": "Finnit_Urenregistratie",
        "Verlof": "Finnit_Verlof",
        "VerzuimUren": "Finnit_VerzuimUren",
        "VerzuimVerloop": "Finnit_VerzuimVerloop",
        "Medewerkers": "Finnit_Medewerkers",
        "Projecten": "Finnit_Projecten",
        "Relaties": "Finnit_Relaties",
        "Contracten": "Finnit_Contracten",
    }
    
    return connectors

if __name__ == "__main__":
    connectors = get_connectors("2016-01-01T00:00:00")
    print(connectors)