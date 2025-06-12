from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import requests
import logging
import json

class EFManagerError(RuntimeError):
    pass

class EFManager():
    def __init__(self, klantnaam, klantid, base_url, bearer_token):
        self.klantnaam = klantnaam
        self.klantid = klantid
        self.applicatienaam = "ExactFinancials"
        self.base_url = base_url
        self.bearer_token = bearer_token
        
    def administraties_ophalen(self):
        spark = SparkSession.builder.getOrCreate()
        
        df_admin = spark.read.format("delta").load("Files/Delta/Administratie")
        df_admin = df_admin.filter((col("KlantID") == self.klantid) & (col("Actief") == True))
        
        return df_admin
    
    def applicatie_tabellen_ophalen(self):
        spark = SparkSession.builder.getOrCreate()
        
        df_applicatie_tabellen = spark.read.format("delta").load("Files/Delta/Tabellen")
        df_applicatie_tabellen = df_applicatie_tabellen.filter(col("ApplicatieNaam") == self.applicatienaam)
        
        return df_applicatie_tabellen
    
    def initial_get_administraties(self):
        spark = SparkSession.builder.getOrCreate()
        
        # Specifiek endpoint
        endpoint = "/web/rest/system/admdat?name=*&fields=adm_nr,name"
        
        # Request maken
        response = requests.get(f"{self.base_url}{endpoint}", headers={"Authorization1": f"Bearer {self.bearer_token}"})
        
        # Response verwerken
        if response.status_code == 200:
            data = response.json()
            df = spark.createDataFrame(data['admdat'])
            return df
        else:
            logging.error(f"Fout bij het ophalen van de administraties: {response.status_code}")
            raise EFManagerError(f"Fout bij het ophalen van de administraties: {response.status_code}")
        
        