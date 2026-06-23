#!/usr/bin/env python3
"""
Script para convertir la tabla gold HTA a formato Parquet
Autor: Angela - Tesis HTA
Fecha: 2026-06-22
"""

from pyspark.sql import SparkSession
import sys

def convert_gold_to_parquet(
    input_table: str = "hta_sis.gold_prestaciones_ml",
    output_path: str = "/Volumes/workspace/hta_sis/gold/dataset_angela_parquet",
    mode: str = "overwrite"
):
    """
    Convierte tabla Delta Lake a formato Parquet
    
    Args:
        input_table: Tabla Delta Lake de origen (default: hta_sis.gold_prestaciones_ml)
        output_path: Ruta del volumen donde guardar el Parquet
        mode: Modo de escritura (overwrite, append, error, ignore)
    """
    
    print(f"🔄 Iniciando conversión de {input_table} a Parquet...")
    print(f"📂 Ruta de salida: {output_path}")
    
    try:
        # Crear sesión Spark
        spark = SparkSession.builder.getOrCreate()
        
        # Leer tabla Delta
        print(f"\n📖 Leyendo tabla {input_table}...")
        df = spark.read.table(input_table)
        
        # Mostrar información básica
        row_count = df.count()
        col_count = len(df.columns)
        print(f"   ✓ Registros: {row_count:,}")
        print(f"   ✓ Columnas: {col_count}")
        print(f"\n📋 Esquema:")
        df.printSchema()
        
        # Guardar como Parquet
        print(f"\n💾 Guardando como Parquet ({mode} mode)...")
        df.write.mode(mode).parquet(output_path)
        
        print(f"\n✅ Conversión completada exitosamente!")
        print(f"📁 Archivo guardado en: {output_path}")
        
        # Verificar archivos generados
        files = spark.read.parquet(output_path).inputFiles()
        print(f"\n📦 Archivos generados: {len(files)}")
        for i, file in enumerate(files[:5], 1):  # Mostrar primeros 5
            print(f"   {i}. {file.split('/')[-1]}")
        if len(files) > 5:
            print(f"   ... y {len(files) - 5} archivos más")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error durante la conversión: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Permitir parámetros desde línea de comandos
    input_table = sys.argv[1] if len(sys.argv) > 1 else "hta_sis.gold_prestaciones_ml"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/Volumes/workspace/hta_sis/gold/dataset_angela_parquet"
    mode = sys.argv[3] if len(sys.argv) > 3 else "overwrite"
    
    success = convert_gold_to_parquet(input_table, output_path, mode)
    sys.exit(0 if success else 1)
