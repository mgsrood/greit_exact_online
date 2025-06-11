import requests
import json

def fetch_all_pages(url, headers, endpoint, tabelnaam, limit=1000):
    print(f"Get request begonnen voor {tabelnaam}")
    
    all_data = []
    total_count = 0
    params = {'limit': limit}
    
    while True:
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 404:
            print(f"Geen data gevonden voor {url}")
            break
        
        # Data als JSON ophalen
        data = response.json()
        
        # Resource key afleiden uit endpoint
        resource = endpoint.strip("/").split("/")[-1]
        
        if resource in data and data[resource]:
            batch = data[resource]
            batch_count = len(batch)
            all_data.extend(batch)
            total_count += batch_count
            print(f"Records opgehaald deze batch: {batch_count} | Totaal: {total_count}")
        else:
            print(f"Data ontbreekt of is leeg voor resource '{resource}'.")
            break
        
        # offset_rowid uit headers halen, case-insensitive en whitespace verwijderen
        offset_rowid = None
        offset_rowid = response.headers.get('offset_rowid')
        
        if offset_rowid:
            headers['offset_rowid'] = offset_rowid
        else:
            break
    
    print(f"Einde ophalen. Totaal records: {total_count}")
    return all_data

# Itereren over administraties
admins = df_admin.select("AdministratieID", "AdministratieCode").collect()
for admin in admins:

    # Variabelen definiëren
    administratie_id = admin.AdministratieID
    administratie_code = admin.AdministratieCode
    
    # TabelID's per Administratie inladen
    df_tabel_ids = spark.read.format("delta").load("Files/Delta/TabelAdminKoppeling")
    df_tabel_ids = df_tabel_ids.filter((col("AdministratieID") == administratie_id) & (col("Actief") == True))
    
    # Itereren over tabellen
    tabel_ids = df_tabel_ids.select("TabelID", "TabelNaam").collect()
    for tabel_id in tabel_ids:

        # TabelID en TabelNaam definiëren
        tabelid = tabel_id.TabelID
        tabelnaam = tabel_id.TabelNaam
        
        # Tabelmapping per tabel ophalen
        df_tabel_mapping = spark.read.format("delta").load("Files/Delta/TabellenMapping")
        df_tabel_mapping = df_tabel_mapping.filter(col("TabelID") == tabelid)
        
        # Base URL ophalen en definiëren
        base_url_secret = "APIBaseURL" + ApplicatieNaam + Klantnaam
        try:
            api_base_url = mssparkutils.credentials.getSecret('https://finnitkeyvault.vault.azure.net/', base_url_secret)
        except Exception as e:
            raise RuntimeError(f"De API Base URL ontbreekt of kon niet worden opgehaald: {e}")
        
        # Endpoint ophalen en definiëren
        endpoint_row = df_applicatie_tabellen.filter(col("TabelNaam") == tabelnaam).select("Endpoint").collect()
        if endpoint_row:
            endpoint = endpoint_row[0]['Endpoint']
        else:
            print(f"Geen endpoint gevonden voor tabelnaam {tabelnaam}")

        # AdministratieCode definiëren
        administratie_code = admin.AdministratieCode
        
        # URL samenstellen
        if api_base_url and endpoint and administratie_code:
            url = api_base_url + endpoint + str(administratie_code)
        else:
            raise ValueError(f"Kan URL niet samenstellen. base_url: {api_base_url}, endpoint: {endpoint}, administratie_code: {administratie_code}")

        # Header definiëren
        token_secret = "BearerToken" + ApplicatieNaam + Klantnaam
        
        try:
            bearer_token = mssparkutils.credentials.getSecret('https://finnitkeyvault.vault.azure.net/', token_secret)
        except Exception as e:
            raise RuntimeError(f"De Bearer token ontbreekt of kon niet worden opgehaald: {e}")

        headers = {
        'Authorization1': f'Bearer {bearer_token}',
        }

        # Alle pagina's ophalen
        all_data = fetch_all_pages(url, headers, endpoint, tabelnaam)

        # DataFrame maken
        if all_data:
            df = spark.createDataFrame(all_data)
        else:
            print(f"Data ontbreekt of is leeg. Verder met volgende.")
            continue
        
        # Kolom mapping toepassen
        df = kolom_mapping(df_tabel_mapping, tabelid, df)

        # Kolom typing toepassen
        df = kolom_typing(df_tabel_mapping, tabelid, df)

        # Connectiestring definiëren
        connectiestring_secret = "Connectiestring" + ApplicatieNaam + Klantnaam
        
        try:
            connectie_string = mssparkutils.credentials.getSecret('https://finnitkeyvault.vault.azure.net/', connectiestring_secret)
        except Exception as e:
            raise RuntimeError(f"De connectiestring ontbreekt of kon niet worden opgehaald: {e}")

        # Database bepaling
        database_dict_secret = "DatabasesKlanten"
        
        try:
            database_dict_str = mssparkutils.credentials.getSecret('https://finnitkeyvault.vault.azure.net/', database_dict_secret)
            database_dict = json.loads(database_dict_str)
        except Exception as e:
            raise RuntimeError(f"De database dictionairy ontbreekt of kon niet worden opgehaald: {e}")

        database = None
        for db_name, db_info in database_dict.get("Databases", {}).items():
            klanten = db_info.get("Klanten", [])
            if Klantnaam in klanten:
                database = db_name
                break

        if database is None:
            raise RuntimeError(f"Klantnaam '{Klantnaam}' is niet gevonden in de database dictionary.")

        # TenantID, ClientID en ClientSecret definiëren
        tenant_id_secret = "TenantID" + database
        try:
            tenant_id = mssparkutils.credentials.getSecret('https://finnitkeyvault.vault.azure.net/', tenant_id_secret)
        except Exception as e:
            raise RuntimeError(f"De database dictionairy ontbreekt of kon niet worden opgehaald: {e}")
        
        client_id_secret = "ClientID" + database
        try:
            client_id = mssparkutils.credentials.getSecret('https://finnitkeyvault.vault.azure.net/', client_id_secret)
        except Exception as e:
            raise RuntimeError(f"De database dictionairy ontbreekt of kon niet worden opgehaald: {e}")

        client_secret_secret = "ClientSecret" + database
        try:
            client_secret = mssparkutils.credentials.getSecret('https://finnitkeyvault.vault.azure.net/', client_secret_secret)
        except Exception as e:
            raise RuntimeError(f"De database dictionairy ontbreekt of kon niet worden opgehaald: {e}")

        # Variabelen definiëren
        auth_method = DoelDatabaseVorm

        succes = apply_table_writing(connectie_string, df, tabelnaam, auth_method, tenant_id, client_id, client_secret)
        if succes:
            print()  
            print(f"Tabel {tabelnaam} gevuld")
            print() 