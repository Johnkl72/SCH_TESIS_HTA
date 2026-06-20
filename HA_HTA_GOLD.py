# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# 1. Leemos las tablas maestras y transaccionales desde Silver
df_hta = spark.read.table("hta_sis.silver_hta")
df_diag = spark.read.table("hta_sis.silver_diag")
df_proced = spark.read.table("hta_sis.silver_proced")
df_medica = spark.read.table("hta_sis.silver_medica")
df_insu = spark.read.table("hta_sis.silver_insu")

# 2. Leemos los catálogos limpios
dim_diag = spark.read.table("hta_sis.dim_diagnostico")
dim_proced = spark.read.table("hta_sis.dim_procedimientos")
dim_medica = spark.read.table("hta_sis.dim_medicamentos")
dim_insu = spark.read.table("hta_sis.dim_insumos")

# COMMAND ----------

# 1. Agrupamos Diagnósticos (Priorizamos el Definitivo 'D')
window_diag = Window.partitionBy("ID_REGISTRO_REL").orderBy(
    F.when(F.col("TIPO_DIAGNOSTICO") == "D", 1)
     .when(F.col("TIPO_DIAGNOSTICO") == "P", 2)
     .otherwise(3)
)

diag_agg = (
    df_diag
    .withColumn("rn", F.row_number().over(window_diag))
    .filter(F.col("rn") == 1)
    .join(dim_diag, "CODDIA", "left")
    .select(
        "ID_REGISTRO_REL", 
        F.col("C10_NOMBRE").alias("DIAGNOSTICO_PRINCIPAL"),
        "TIPO_DIAGNOSTICO"
    )
)

# 2. Agrupamos Medicamentos (Añadimos Costo y Nombres Concatenados)
med_joined = df_medica.join(
    dim_medica, 
    df_medica.COD_MEDICAMENTO == dim_medica.COD_MEDICAMENTOS, 
    "left"
)

med_agg = (
    med_joined
    .groupBy("ID_REGISTRO_REL")
    .agg(
        F.count("COD_MEDICAMENTO").alias("CANT_TIPOS_MEDICAMENTOS"),
        F.sum("CANTIDAD_ENTREGADA").alias("TOTAL_MEDICAMENTOS_ENTREGADOS"),
        F.sum("VALOR_NETO").alias("COSTO_TOTAL_MEDICAMENTOS"),
        F.concat_ws(" | ", F.collect_set("NOMBRE_MEDICAMENTO")).alias("NOMBRES_MEDICAMENTOS")
    )
)

# 3. Agrupamos Procedimientos
proced_agg = (
    df_proced
    .groupBy("ID_REGISTRO_REL")
    .agg(F.count("COD_PROCEDIMIENTO").alias("CANT_PROCEDIMIENTOS"))
)

# 4. Agrupamos Insumos (Añadimos el Costo Total)
insu_agg = (
    df_insu
    .groupBy("ID_REGISTRO_REL")
    .agg(
        F.count("COD_INSUMO").alias("CANT_INSUMOS"),
        F.sum("VALOR_NETO").alias("COSTO_TOTAL_INSUMOS")
    )
)

# COMMAND ----------

# Unificamos la tabla de Prestaciones con los resúmenes satélite
df_consolidado = (
    df_hta
    .join(diag_agg, "ID_REGISTRO_REL", "left")
    .join(med_agg, "ID_REGISTRO_REL", "left")
    .join(proced_agg, "ID_REGISTRO_REL", "left")
    .join(insu_agg, "ID_REGISTRO_REL", "left")
    
    # Llenamos nulos para las agregaciones numéricas (incluyendo los costos nuevos)
    .fillna(0.0, subset=[
        "CANT_TIPOS_MEDICAMENTOS", 
        "TOTAL_MEDICAMENTOS_ENTREGADOS", 
        "CANT_PROCEDIMIENTOS", 
        "CANT_INSUMOS",
        "COSTO_TOTAL_MEDICAMENTOS",
        "COSTO_TOTAL_INSUMOS"
    ])
    
    # Llenamos nulos para las variables de texto
    .fillna("SIN DIAGNOSTICO", subset=["DIAGNOSTICO_PRINCIPAL"])
    .fillna("SIN MEDICACION", subset=["NOMBRES_MEDICAMENTOS"])
)

# COMMAND ----------

# 1. Definimos la ventana histórica por paciente ordenado por fecha
window_paciente_hist = Window.partitionBy("CODIGO_ANONIMIZADO").orderBy("FECHA_ATENCION")

df_features = (
    df_consolidado
    # Capturamos el estado de la cita inmediatamente anterior (Variables X puras)
    .withColumn("SIST_ANTERIOR", F.lag("PRES_ART_SISTOLICA").over(window_paciente_hist))
    .withColumn("DIAST_ANTERIOR", F.lag("PRES_ART_DIASTOLICA").over(window_paciente_hist))
    .withColumn("FECHA_ATENCION_ANTERIOR", F.lag("FECHA_ATENCION").over(window_paciente_hist))
    
    .withColumn("DIAS_DESDE_ULTIMA_CITA", F.datediff(F.col("FECHA_ATENCION"), F.col("FECHA_ATENCION_ANTERIOR")))
    
    .fillna(-1, subset=["SIST_ANTERIOR", "DIAST_ANTERIOR", "DIAS_DESDE_ULTIMA_CITA"])
)

# COMMAND ----------

# Celda 5 para Histórico (HA_HTA_GOLD)
df_gold_final = (
    df_features
    # PROTECCIÓN DEL TARGET: Lo renombramos para que sea inconfundible
    .withColumnRenamed("DESTINO_ASEGURADO", "TARGET_DESTINO")
    
    # El Gran Drop: Conservamos CODIGO_ANONIMIZADO, eliminamos el resto
    .drop(
        "ID_REGISTRO_REL",           # ID transaccional purgado
        "FECHA_ATENCION",            # Reemplazada por la variable de Días relativos
        "FECHA_ATENCION_ANTERIOR",   # Variable temporal transitoria purgada
        "CODIGO_SERV_PRESTACIONAL"   # ID redundante, dejamos la descripción
    )
)

# Persistimos en Delta Lake
(df_gold_final.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("hta_sis.gold_prestaciones_ml"))

print("✅ Capa Gold Histórica generada con Código Anonimizado.")
display(spark.read.table("hta_sis.gold_prestaciones_ml").limit(5))

# COMMAND ----------

# Databricks notebook source
import pyspark.sql.functions as F

# 1. Leemos la tabla Gold finalizada
df_export = spark.read.table("hta_sis.gold_prestaciones_ml")

# 2. Definimos la ruta en tus Volumes (asegúrate de tener la carpeta 'gold' o ajusta la ruta)
export_path = "/Volumes/workspace/hta_sis/gold/dataset_angela_ml"

# 3. Forzamos la consolidación en un (1) solo archivo y exportamos
(df_export.coalesce(1)
 .write
 .format("csv")
 .option("header", "true")      # Que incluya los nombres de las columnas
 .option("sep", ",")            # Separador estándar por comas
 .mode("overwrite")
 .save(export_path))

print(f"✅ Dataset exportado exitosamente a CSV en la ruta: {export_path}")

# Opcional: Listamos el contenido de la carpeta para ver el archivo generado
display(dbutils.fs.ls(export_path))
