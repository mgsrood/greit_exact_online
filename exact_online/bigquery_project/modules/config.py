from datetime import datetime

def retrieve_connection(client, full_table_id, log_table):
    # Query voor klant connectiepunt
    query_1 = f"""
    SELECT *
    FROM {full_table_id}
    """

    # Voer query uit
    query_job = client.query(query_1)

    status = query_job.state

    # Add row to the log table
    if status == 'DONE':
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_query = f"""
        INSERT INTO {log_table} (Actie, Timestamp)
        VALUES ('Connectiepunten inladen', '{current_datetime}')
        """
        # Voer de query uit
        log_query_job = client.query(log_query)
        log_query_job.result()  # Wacht op voltooiing

        print("Connectiepunten log succesvol toegevoegd.")

    # Resultaten
    results = query_job.result()
    df = results.to_dataframe()

    for index, row in df.iterrows():
        connection = row['connection']
        config_extension = 'config'
        customer_config_table_id = f'{connection}.{config_extension}'
    
    return customer_config_table_id, connection

def retrieve_config(client, customer_config_table_id, connection, log_table):
    # Haal de configuratie gegevens op
    query_2 = f"""
    SELECT *
    FROM {customer_config_table_id}
    """

    # Voer query uit
    query_job = client.query(query_2)

    status = query_job.state

    # Add row to the log table
    if status == 'DONE':
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_query = f"""
        INSERT INTO {log_table} (Actie, Timestamp)
        VALUES ('Configuratiegegevens {connection} inladen', '{current_datetime}')
        """
        # Voer de query uit
        log_query_job = client.query(log_query)
        log_query_job.result()  # Wacht op voltooiing

        print("Configuratie log succesvol toegevoegd.")

    # Resultaten
    results = query_job.result()
    config_df = results.to_dataframe()
    
    # Retourneer de configuratie gegevens
    client_id = config_df['client_id'].values[0]
    client_secret = config_df['client_secret'].values[0]
    access_token = config_df['access_token'].values[0]
    refresh_token = config_df['refresh_token'].values[0]
    redirect_uri = config_df['redirect_uri'].values[0]
    jaren = config_df['jaren'].values[0]

    return client_id, client_secret, access_token, refresh_token

def retrieve_division_code(client, connection, log_table):
    # Haal de divisiecodes 
    division_extension = 'division_codes'
    division_codes_full_table_id = f'{connection}.{division_extension}'

    query_3 = f"""
    SELECT *
    FROM {division_codes_full_table_id}
    """

    # Voer query uit
    query_job = client.query(query_3)

    status = query_job.state

    # Add row to the log table
    if status == 'DONE':
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_query = f"""
        INSERT INTO {log_table} (Actie, Timestamp)
        VALUES ('Divisiecodes {connection} inladen', '{current_datetime}')
        """
        # Voer de query uit
        log_query_job = client.query(log_query)
        log_query_job.result()  # Wacht op voltooiing

        print("Divisiecode log succesvol toegevoegd.")

    # Resultaten
    results = query_job.result()
    division_code_df = results.to_dataframe()
    division_code = division_code_df['division_code'].values[0]

    return division_code