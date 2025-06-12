from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession
import logging

class TransformerError(RuntimeError):
    pass

class Transformer():
    def __init__(self, klantnaam, klantid):
        self.klantnaam = klantnaam
        self.klantid = klantid

    def tabel_mapping_ophalen(self, tabel_id):
        spark = SparkSession.builder.getOrCreate()
        
        df_tabel_mapping = spark.read.format("delta").load("Files/Delta/TabellenMapping")
        df_tabel_mapping = df_tabel_mapping.filter((col("TabelID") == tabel_id))
        
        return df_tabel_mapping
    
    def kolom_typing_toepassen(self, df, tabel_id):
        
        try:
            # Specifieke mapping ophalen
            df_tabel_mapping = self.tabel_mapping_ophalen(tabel_id)
            
            # DoelKolom en DoelDatatype ophalen
            type_conversies = df_tabel_mapping.select("Doelkolom", "DoelDatatype").collect()
            
            # Kolommen casten naar het juiste datatype
            for row in type_conversies:
                col_name = row["Doelkolom"]
                new_type = row["DoelDatatype"]
                if col_name in df.columns and new_type is not None:
                    df = df.withColumn(col_name, col(col_name).cast(new_type))
                    
            return df
        
        except Exception as e:
            logging.error(f"Fout bij het toepassen van de kolom typing: {e}")
            raise TransformerError(f"Fout bij het toepassen van de kolom typing: {e}")
        
    def kolom_mapping_toepassen(self, df, tabel_id):
        try:
            # Specifieke mapping ophalen
            df_tabel_mapping = self.tabel_mapping_ophalen(tabel_id)
            
            # BronKolom en DoelKolom ophalen
            kolommen = {row["Bronkolom"]: row["Doelkolom"] for row in df_tabel_mapping.select("Bronkolom", "Doelkolom").collect()}
            
            # Kolommen in de DataFrame hernoemen
            for old_col, new_col in kolommen.items():
                if old_col in df.columns:
                    df = df.withColumnRenamed(old_col, new_col)

            # DataFrame filteren op relevante kolommen
            doelkolommen = [new_col for new_col in kolommen.values() if new_col in df.columns]
            df = df.select(doelkolommen)
            
            return df
        
        except Exception as e:
            logging.error(f"Fout bij het toepassen van de kolom mapping: {e}")
            raise TransformerError(f"Fout bij het toepassen van de kolom mapping: {e}")
        
    def kolommen_toevoegen(self, df, kolom_naam, waarde):
        try:
            # Kolom toevoegen met naam X en waarde Y
            df = df.withColumn(kolom_naam, lit(waarde))
            
            return df
        
        except Exception as e:
            logging.error(f"Fout bij het toevoegen van de kolom: {e}")
            raise TransformerError(f"Fout bij het toevoegen van de kolom: {e}")
    
    
        
        