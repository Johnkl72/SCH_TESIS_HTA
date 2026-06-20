# SCH_TESIS_HTA - Análisis de Hipertensión Arterial (HTA)

## 📋 Descripción del Proyecto

Proyecto de análisis de datos de salud enfocado en la **Hipertensión Arterial (HTA)** utilizando datos del **Seguro Integral de Salud (SIS)** para el distrito de **San Juan de Miraflores, Lima**. El proyecto implementa una arquitectura de datos tipo **Medallion** (Bronze-Silver-Gold) en Databricks para el procesamiento y análisis de atenciones médicas.

## 🏗️ Arquitectura del Proyecto

El proyecto sigue la arquitectura Medallion con tres capas de procesamiento:

```
RAW DATA (Volumes)
    ↓
BRONZE LAYER (Ingesta)
    ↓
SILVER LAYER (Limpieza y Transformación)
    ↓
GOLD LAYER (Agregaciones y Análisis)
```

### 📊 Capas de Datos

#### 🥉 Bronze Layer
**Notebooks:**
- `Bronze layer.ipynb`
- `HTA_BRONZE.ipynb`

**Función:** Ingesta de datos raw desde archivos CSV (FUAS - Formulario Único de Atención de Salud).

**Tablas generadas:**
- `hta_sis.bronze_hta` - Datos de prestaciones de salud
- `hta_sis.bronze_diagnosticos` - Diagnósticos médicos
- `hta_sis.bronze_procedimientos` - Procedimientos realizados
- `hta_sis.bronze_medicamentos` - Medicamentos dispensados
- `hta_sis.bronze_insumos` - Insumos médicos utilizados

**Características:**
- Carga directa desde `/Volumes/workspace/hta_sis/raw/`
- Sin transformaciones, mantiene estructura original
- Formato Delta para versionamiento

#### 🥈 Silver Layer
**Notebooks:**
- `Silver layer.ipynb`
- `HTA_SILVER.ipynb`
- `Silver_creation_of_hd_universal_hta.ipynb`

**Función:** Limpieza, estandarización y filtrado de datos.

**Tablas generadas:**
- `hta_sis.silver_hta` - Prestaciones limpias y filtradas
- `hta_sis.silver_diag` - Diagnósticos estandarizados
- `hta_sis.silver_proced` - Procedimientos normalizados
- `hta_sis.silver_medica` - Medicamentos con cantidades
- `hta_sis.silver_insu` - Insumos con costos

**Transformaciones principales:**
- **Filtros geográficos:** Lima - San Juan de Miraflores
- **Conversión de tipos:** Cast de campos numéricos y fechas
- **Normalización:** Códigos de diagnóstico (P/D/R), sexo (F/M)
- **Imputación:** Valores nulos a 0 para variables numéricas
- **Column pruning:** Selección de columnas relevantes
- **Predicate pushdown:** Optimización de queries

**Campos clave:**
```python
- ID_REGISTRO_REL: Llave de cruce entre tablas
- CODIGO_ANONIMIZADO: Identificador de paciente
- SEXO: F/M
- EDAD: int
- FECHA_ATENCION: date
- PRES_ART_SISTOLICA: int
- PRES_ART_DIASTOLICA: int
- DIAGNOSTICO_PRINCIPAL: string
```

#### 🥇 Gold Layer
**Notebooks:**
- `HA_HTA_GOLD.ipynb`
- `UA_HTA_GOLD.ipynb`

**Función:** Consolidación de datos con agregaciones de negocio.

**Tablas generadas:**
- `hta_sis.gold_hta_consolidado` - Vista unificada de atenciones

**Dimensiones:**
- `hta_sis.dim_diagnostico` - Catálogo CIE-10
- `hta_sis.dim_medicamentos` - Petitorio farmacológico
- `hta_sis.dim_procedimientos` - Catálogo de procedimientos
- `hta_sis.dim_insumos` - Catálogo de insumos

**Agregaciones principales:**
- Diagnóstico principal (prioriza definitivo sobre presuntivo)
- Cantidad de tipos de medicamentos por atención
- Total de medicamentos entregados
- Costo total de medicamentos e insumos
- Cantidad de procedimientos realizados
- Nombres de medicamentos concatenados

**Estructura final:**
```python
# Por cada atención (ID_REGISTRO_REL):
- Datos demográficos (edad, sexo)
- Datos clínicos (presión arterial, días hospitalización)
- Diagnóstico principal + tipo
- Resumen de medicamentos (cantidad, costos, nombres)
- Resumen de procedimientos
- Resumen de insumos (cantidad, costos)
- Datos administrativos (servicio, personal, destino)
```

## 🗂️ Esquema de Base de Datos

**Catalog:** `hta_sis`

**Schemas:**
- `bronze_*` - Tablas raw
- `silver_*` - Tablas limpias
- `gold_*` - Tablas analíticas
- `dim_*` - Tablas de dimensiones

## 🔧 Tecnologías Utilizadas

- **Databricks** - Plataforma de procesamiento
- **Apache Spark / PySpark** - Motor de procesamiento distribuido
- **Delta Lake** - Formato de almacenamiento con ACID
- **Unity Catalog** - Gobernanza de datos
- **Python** - Lenguaje principal
- **SQL** - Consultas y análisis

## 📦 Dependencias

```python
# Librerías principales
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pandas as pd
import openpyxl  # Para lectura de archivos Excel
```

## 🚀 Ejecución del Pipeline

### Orden de ejecución:

1. **Bronze Layer** - Carga inicial de datos
   ```
   Bronze layer.ipynb  → Carga de datos raw
   HTA_BRONZE.ipynb    → Creación de tablas bronze
   ```

2. **Silver Layer** - Limpieza y estandarización
   ```
   Silver layer.ipynb  → Transformaciones iniciales
   HTA_SILVER.ipynb    → Creación de tablas silver
   ```

3. **Gold Layer** - Consolidación y agregaciones
   ```
   HA_HTA_GOLD.ipynb   → Agregaciones principales
   UA_HTA_GOLD.ipynb   → Vistas analíticas
   ```

### Ejecutar pipeline completo:

```python
# Opción 1: Ejecutar notebooks individualmente desde Databricks UI

# Opción 2: Usar dbutils para ejecutar en secuencia
dbutils.notebook.run("Bronze layer", 0)
dbutils.notebook.run("HTA_BRONZE", 0)
dbutils.notebook.run("Silver layer", 0)
dbutils.notebook.run("HTA_SILVER", 0)
dbutils.notebook.run("HA_HTA_GOLD", 0)
dbutils.notebook.run("UA_HTA_GOLD", 0)
```

## 📊 Casos de Uso

### Análisis disponibles:

1. **Perfil epidemiológico de HTA en San Juan de Miraflores**
   - Distribución por edad y sexo
   - Niveles de presión arterial
   - Tendencias temporales

2. **Patrones de medicación**
   - Medicamentos más frecuentes
   - Costos de tratamiento
   - Combinaciones de fármacos

3. **Utilización de servicios**
   - Días de hospitalización
   - Procedimientos asociados
   - Destino del paciente

4. **Diagnósticos asociados**
   - Comorbilidades (CIE-10)
   - Tipo de diagnóstico (definitivo/presuntivo)

## 📈 Consultas de Ejemplo

### Obtener atenciones con diagnóstico de HTA:
```sql
SELECT 
  FECHA_ATENCION,
  EDAD,
  SEXO,
  PRES_ART_SISTOLICA,
  PRES_ART_DIASTOLICA,
  DIAGNOSTICO_PRINCIPAL,
  NOMBRES_MEDICAMENTOS,
  COSTO_TOTAL_MEDICAMENTOS
FROM hta_sis.gold_hta_consolidado
WHERE DIAGNOSTICO_PRINCIPAL LIKE '%HIPERTENS%'
ORDER BY FECHA_ATENCION DESC
```

### Estadísticas de presión arterial por grupo etario:
```sql
SELECT 
  CASE 
    WHEN EDAD < 30 THEN '< 30'
    WHEN EDAD BETWEEN 30 AND 50 THEN '30-50'
    WHEN EDAD BETWEEN 51 AND 70 THEN '51-70'
    ELSE '> 70'
  END AS GRUPO_EDAD,
  AVG(PRES_ART_SISTOLICA) AS PROM_SISTOLICA,
  AVG(PRES_ART_DIASTOLICA) AS PROM_DIASTOLICA,
  COUNT(*) AS TOTAL_ATENCIONES
FROM hta_sis.gold_hta_consolidado
GROUP BY 1
ORDER BY 1
```

## 🔐 Consideraciones de Seguridad

- ✅ Datos anonimizados mediante `CODIGO_ANONIMIZADO`
- ✅ Sin información personal identificable (PII)
- ✅ Cumple con normativas de protección de datos de salud
- ✅ Acceso controlado mediante Unity Catalog

## 📝 Notas Técnicas

### Optimizaciones implementadas:
- **Predicate Pushdown**: Filtros aplicados antes de cargar datos
- **Column Pruning**: Selección de columnas específicas
- **Delta Lake**: Formato optimizado con compresión
- **Partition Pruning**: Filtros por fecha cuando aplica
- **Window Functions**: Para ranking y priorización eficiente

### Calidad de datos:
- Imputación de nulos con valores por defecto
- Validación de rangos de presión arterial
- Estandarización de códigos diagnósticos
- Normalización de categorías

## 🤝 Contribuciones

Para contribuir al proyecto:
1. Seguir la estructura de capas (Bronze → Silver → Gold)
2. Documentar transformaciones en comentarios
3. Mantener nomenclatura consistente
4. Validar calidad de datos después de transformaciones

## 📞 Contacto

Proyecto desarrollado como parte de tesis de investigación en análisis de datos de salud.

---

**Última actualización:** Junio 2026  
**Versión:** 1.0  
**Estado:** En desarrollo