# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import pyspark.sql.functions as F

# COMMAND ----------

silver_hta = (
    spark.read.table("hta_sis.bronze_hta")
    # 1. Predicate Pushdown: Filtramos antes de cargar todo a memoria
    .filter(
        (F.col("DEPARTAMENTO") == "LIMA") & 
        (F.lower(F.col("DISTRITO")).contains("san juan de miraflores"))
    )
    # 2. Column Pruning & Casting: Seleccionamos solo lo requerido y tipamos
    .select(
        F.col("CODIGO_ANONIMIZADO"), # Conservado temporalmente para Gold
        F.col("ID_REGISTRO_REL").cast("long").alias("ID_REGISTRO_REL"), # Llave de cruce
        F.when(F.col("SEXO").contains("FEMENINO"), "F").otherwise("M").alias("SEXO"),
        F.col("EDAD").cast("int"),
        F.col("NIVEL_EESS").cast("int"),
        F.col("CODIGO_SERV_PRESTACIONAL"),
        F.col("SERVICIO_PRESTACIONAL"),
        F.coalesce(F.col("DIAS_HOSP").cast("int"), F.lit(0)).alias("DIAS_HOSP"),
        F.coalesce(F.col("PRES_ART_SISTOLICA").cast("int"), F.lit(0)).alias("PRES_ART_SISTOLICA"),
        F.coalesce(F.col("PRES_ART_DIASTOLICA").cast("int"), F.lit(0)).alias("PRES_ART_DIASTOLICA"),
        F.col("TIPO_PERSONAL_SALUD"),
        F.col("DESTINO_ASEGURADO"),
        F.to_date(F.col("FECHA_ATENCION"), "yyyyMMdd").alias("FECHA_ATENCION"),
        F.when(F.col("FECATE_POST_FECFED") == 'SI', 1).otherwise(0).alias("FECATE_POST_FECFED"),
        F.when(F.col("ES_CAPITA") == 'S', 1).otherwise(0).alias("ES_CAPITA")
    )
)

(silver_hta.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("hta_sis.silver_hta"))

print("✅ silver_hta procesada y guardada.")

# COMMAND ----------

silver_diag = (
    spark.read.table("hta_sis.bronze_diagnosticos")
    .select(
        F.col("ID_REGISTRO_REL").cast("long").alias("ID_REGISTRO_REL"),
        F.col("CODDIA"),
        F.when(F.col("TIPO_DIAGNOSTICO").contains("DEFINITIVO"), "D")
         .when(F.col("TIPO_DIAGNOSTICO").contains("PRESUNTIVO"), "P")
         .otherwise("R").alias("TIPO_DIAGNOSTICO")
    )
)

(silver_diag.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable("hta_sis.silver_diag"))

print("✅ silver_diag procesada y guardada.")

# COMMAND ----------

silver_proced = (
    spark.read.table("hta_sis.bronze_procedimientos")
    .select(
        F.col("ID_REGISTRO_REL").cast("long").alias("ID_REGISTRO_REL"),
        F.col("COD_PROCEDIMIIENTO").alias("COD_PROCEDIMIENTO"), # Renombramos si tenía la doble 'I'
        F.col("CANTIDAD_ENTREGADA").cast("int")
        # FECHA_CORTE y VALOR_NETO excluidos implícitamente al no seleccionarlos
    )
)

(silver_proced.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable("hta_sis.silver_proced"))

print("✅ silver_proced procesada y guardada.")

# COMMAND ----------

silver_medica = (
    spark.read.table("hta_sis.bronze_medicamentos")
    .select(
        F.col("ID_REGISTRO_REL").cast("long").alias("ID_REGISTRO_REL"),
        F.col("COD_MEDICAMENTO"),
        F.col("CANTIDAD_ENTREGADA").cast("int"),
        F.col("VALOR_NETO").cast("double")
    )
)

(silver_medica.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable("hta_sis.silver_medica"))


silver_insu = (
    spark.read.table("hta_sis.bronze_insumos")
    .select(
        F.col("ID_REGISTRO_REL").cast("long").alias("ID_REGISTRO_REL"),
        F.col("COD_INSUMO"),
        F.col("CANTIDAD_ENTREGADA").cast("int"),
        F.col("VALOR_NETO").cast("double")
    )
)

(silver_insu.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable("hta_sis.silver_insu"))

print("✅ silver_medica y silver_insu procesadas y guardadas.")

# COMMAND ----------


