#!/usr/bin/env python3
"""
Script para procesar datos de seguridad de Yucatán desde el dataset ENSU.
"""

import pandas as pd
import os

def process_yucatan_security():
    # Rutas de archivos
    input_path = "data/processed/ensu_selected_columns.csv"
    output_path = "data/processed/yucatan_security_summary.csv"
    
    # Verificar que el archivo procesado existe
    if not os.path.exists(input_path):
        print(f"Error: No se encontró el archivo {input_path}")
        print("Por favor, ejecuta primero process_ensu_data.py")
        return
    
    print(f"Leyendo archivo: {input_path}")
    
    # Leer el CSV procesado
    df = pd.read_csv(input_path)
    
    print(f"Dataset original: {df.shape[0]} filas")
    
    # Convertir BP1_1 a numérico
    df['BP1_1'] = pd.to_numeric(df['BP1_1'], errors='coerce')
    
    # Filtrar por Yucatán (considerando posibles variaciones con/sin tilde)
    df_yucatan = df[df['NOM_ENT'].str.upper().isin(['YUCATAN', 'YUCATÁN'])].copy()
    
    if df_yucatan.empty:
        print("Error: No se encontraron registros para Yucatán")
        print(f"Estados disponibles: {df['NOM_ENT'].unique()}")
        return
    
    print(f"Registros de Yucatán: {df_yucatan.shape[0]} filas")
    
    # Verificar valores únicos de BP1_1 en Yucatán
    print(f"\nValores únicos de BP1_1 en Yucatán: {sorted(df_yucatan['BP1_1'].dropna().unique())}")
    
    # Agrupar por NOM_ENT, NOM_MUN, NOM_CD y calcular totales
    grouped_data = []
    for (nom_ent, nom_mun, nom_cd), group in df_yucatan.groupby(['NOM_ENT', 'NOM_MUN', 'NOM_CD']):
        total_seguros = (group['BP1_1'] == 1).sum()
        total_inseguros = (group['BP1_1'] == 2).sum()
        total_no_responde = (group['BP1_1'] == 9).sum()
        total_respuestas = len(group)
        
        grouped_data.append({
            'NOM_ENT': nom_ent,
            'NOM_MUN': nom_mun,
            'NOM_CD': nom_cd,
            'TOTAL_SEGUROS': total_seguros,
            'TOTAL_INSEGUROS': total_inseguros,
            'TOTAL_NO_RESPONDE': total_no_responde,
            'TOTAL_RESPUESTAS': total_respuestas
        })
    
    grouped = pd.DataFrame(grouped_data)
    
    # Calcular porcentajes
    grouped['PORCENTAJE_SEGUROS'] = (grouped['TOTAL_SEGUROS'] / grouped['TOTAL_RESPUESTAS'] * 100).round(2)
    grouped['PORCENTAJE_INSEGUROS'] = (grouped['TOTAL_INSEGUROS'] / grouped['TOTAL_RESPUESTAS'] * 100).round(2)
    grouped['PORCENTAJE_NO_RESPONDE'] = (grouped['TOTAL_NO_RESPONDE'] / grouped['TOTAL_RESPUESTAS'] * 100).round(2)
    
    # Ordenar columnas según lo solicitado
    column_order = [
        'NOM_ENT', 'NOM_MUN', 'NOM_CD',
        'TOTAL_SEGUROS', 'TOTAL_INSEGUROS', 'TOTAL_NO_RESPONDE',
        'PORCENTAJE_SEGUROS', 'PORCENTAJE_INSEGUROS', 'PORCENTAJE_NO_RESPONDE',
        'TOTAL_RESPUESTAS'
    ]
    
    df_final = grouped[column_order]
    
    # Mostrar estadísticas
    print("\n=== RESUMEN DE RESULTADOS ===")
    print(f"Total de municipios: {df_final['NOM_MUN'].nunique()}")
    print(f"Total de ciudades: {df_final['NOM_CD'].nunique()}")
    print(f"Total de registros agrupados: {len(df_final)}")
    
    print("\nEstadísticas generales de Yucatán:")
    total_seguros = df_final['TOTAL_SEGUROS'].sum()
    total_inseguros = df_final['TOTAL_INSEGUROS'].sum()
    total_no_responde = df_final['TOTAL_NO_RESPONDE'].sum()
    total_general = df_final['TOTAL_RESPUESTAS'].sum()
    
    print(f"- Total de respuestas 'Seguro': {total_seguros:,} ({total_seguros/total_general*100:.1f}%)")
    print(f"- Total de respuestas 'Inseguro': {total_inseguros:,} ({total_inseguros/total_general*100:.1f}%)")
    print(f"- Total de 'No sabe/No responde': {total_no_responde:,} ({total_no_responde/total_general*100:.1f}%)")
    print(f"- Total general de respuestas: {total_general:,}")
    
    # Mostrar municipios con mayor percepción de seguridad
    print("\nTop 5 municipios con mayor porcentaje de seguridad:")
    top_seguros = df_final.nlargest(5, 'PORCENTAJE_SEGUROS')[['NOM_MUN', 'NOM_CD', 'PORCENTAJE_SEGUROS', 'TOTAL_RESPUESTAS']]
    print(top_seguros.to_string(index=False))
    
    # Mostrar municipios con mayor percepción de inseguridad
    print("\nTop 5 municipios con mayor porcentaje de inseguridad:")
    top_inseguros = df_final.nlargest(5, 'PORCENTAJE_INSEGUROS')[['NOM_MUN', 'NOM_CD', 'PORCENTAJE_INSEGUROS', 'TOTAL_RESPUESTAS']]
    print(top_inseguros.to_string(index=False))
    
    # Mostrar primeras filas del dataset final
    print("\nPrimeras 10 filas del dataset final:")
    print(df_final.head(10))
    
    # Crear directorio de salida si no existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Guardar el dataset
    df_final.to_csv(output_path, index=False)
    print(f"\nDataset guardado en: {output_path}")
    
    return df_final

if __name__ == "__main__":
    process_yucatan_security()