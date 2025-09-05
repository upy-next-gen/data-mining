#!/usr/bin/env python3
"""
Script para procesar el dataset ENSU y extraer columnas específicas.
"""

import pandas as pd
import os

def process_ensu_dataset():
    # Definir rutas
    input_path = "data/conjunto_de_datos_ensu_2025_2t_csv/conjunto_de_datos_ensu_cb_0625/conjunto_de_datos/conjunto_de_datos_ensu_cb_0625.csv"
    output_path = "data/processed/ensu_selected_columns.csv"
    
    # Verificar que el archivo existe
    if not os.path.exists(input_path):
        print(f"Error: No se encontró el archivo {input_path}")
        return
    
    print(f"Leyendo archivo: {input_path}")
    
    # Leer el CSV
    df = pd.read_csv(input_path)
    
    # Mostrar información básica del dataset
    print(f"Dataset original: {df.shape[0]} filas, {df.shape[1]} columnas")
    print(f"Columnas disponibles: {list(df.columns)[:20]}...")  # Mostrar primeras 20 columnas
    
    # Seleccionar las columnas requeridas
    required_columns = ['NOM_ENT', 'NOM_MUN', 'NOM_CD', 'BP1_1']
    
    # Verificar que todas las columnas existen
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Error: Las siguientes columnas no existen en el dataset: {missing_columns}")
        return
    
    # Crear el nuevo dataset con las columnas seleccionadas
    df_selected = df[required_columns].copy()
    
    print(f"\nDataset procesado: {df_selected.shape[0]} filas, {df_selected.shape[1]} columnas")
    print(f"Columnas seleccionadas: {list(df_selected.columns)}")
    
    # Mostrar estadísticas básicas
    print("\nEstadísticas del dataset procesado:")
    print(f"- Valores únicos en NOM_ENT: {df_selected['NOM_ENT'].nunique()}")
    print(f"- Valores únicos en NOM_MUN: {df_selected['NOM_MUN'].nunique()}")
    print(f"- Valores únicos en NOM_CD: {df_selected['NOM_CD'].nunique()}")
    print(f"- Distribución de BP1_1:")
    print(df_selected['BP1_1'].value_counts().sort_index())
    
    # Mostrar primeras filas
    print("\nPrimeras 5 filas del dataset procesado:")
    print(df_selected.head())
    
    # Crear directorio de salida si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Guardar el dataset procesado
    df_selected.to_csv(output_path, index=False)
    print(f"\nDataset guardado en: {output_path}")
    
    return df_selected

if __name__ == "__main__":
    process_ensu_dataset()