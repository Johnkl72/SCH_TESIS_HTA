# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pandas as pd
import re

# Definimos rutas base
RAW_PATH = "/Volumes/workspace/hta_sis/raw"
BRONZE_PATH = "/Volumes/workspace/hta_sis/bronze"

# COMMAND ----------

def standardize_columns(df):
    """Limpia los nombres de las columnas para compatibilidad Delta/Parquet."""
    for col_name in df.columns:
        clean_name = col_name.strip()
        clean_name = re.sub(r'\s+', '_', clean_name)
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '', clean_name)
        clean_name = clean_name.upper()
        df = df.withColumnRenamed(col_name, clean_name)
    return df

# COMMAND ----------

# Lista de archivos transaccionales a ingerir
fact_files = {
    "HTA_FUAS.csv": "bronze_hta",
    "HTA_Diagnosticos.csv": "bronze_diagnosticos",
    "HTA_Medicamentos.csv": "bronze_medicamentos",
    "HTA_Procedimientos.csv": "bronze_procedimientos",
    "HTA_Insumos.csv": "bronze_insumos"
}

for file_name, table_name in fact_files.items():
    print(f"Ingestando {file_name} hacia {table_name}...")
    df_raw = spark.read.format("csv").option("header", True).load(f"{RAW_PATH}/{file_name}")
    df_clean = standardize_columns(df_raw)
    
    (df_clean.write
     .format("delta")
     .mode("overwrite")
     .saveAsTable(f"hta_sis.{table_name}"))

# COMMAND ----------

cat_excel_path = f"{RAW_PATH}/6. Catálogos.xlsx"
sheets = pd.read_excel(cat_excel_path, sheet_name=None, engine="openpyxl")

# Función rápida para limpiar el nombre de la tabla (quitar tildes y caracteres especiales)
def clean_table_name(name):
    clean = name.lower().strip()
    # Reemplazamos vocales con tilde y eñes
    reemplazos = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ñ': 'n'}
    for tilde, sin_tilde in reemplazos.items():
        clean = clean.replace(tilde, sin_tilde)
    # Finalmente quitamos cualquier cosa que no sea letra, número o guion bajo
    clean = re.sub(r'[^a-z0-9_]', '', clean.replace(' ', '_'))
    return clean

for sheet_name, pdf in sheets.items():
    # Limpieza estricta en pandas para los nombres de columnas
    pdf.columns = [re.sub(r'[^a-zA-Z0-9_]', '', c.strip().replace(' ', '_').upper()) for c in pdf.columns]
    
    # Manejo de tipos de datos al pasar a Spark (forzamos todo a String para Bronze)
    df_spark = spark.createDataFrame(pdf.astype(str))
    
    # Generamos el nombre de la tabla seguro
    safe_sheet_name = clean_table_name(sheet_name)
    dim_name = f"dim_{safe_sheet_name}"
    
    print(f"Guardando catálogo '{sheet_name}' como tabla: {dim_name}...")
    
    # Guardado con overwriteSchema activado
    (df_spark.write
     .format("delta")
     .mode("overwrite")
     .option("overwriteSchema", "true") # <--- ESTA ES LA MAGIA
     .saveAsTable(f"hta_sis.{dim_name}"))
