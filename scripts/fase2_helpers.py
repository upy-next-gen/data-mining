import pandas as pd

BP1_1_VALID_VALUES = [1, 2, 9]

def normalize_string(s):
    """Normalizes a string for consistent comparison."""
    if not isinstance(s, str):
        return s
    # Use double backslashes for escape sequences
    s = s.replace('\\r', '').replace('\\n', '')
    s = s.replace('Ñ', 'N').replace('ñ', 'n')
    # A more robust way to remove accents
    try:
        s = pd.Series(s).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').iloc[0]
    except IndexError:
        s = ""
    return s.upper().strip()

def validate_columns(df_columns, core_cols):
    """Validates the presence of required columns."""
    missing_cols = [col for col in core_cols if col not in df_columns]
    return {
        "columnas_presentes": not bool(missing_cols),
        "columnas_faltantes": missing_cols,
        "columnas_totales": len(df_columns)
    }

def validate_bp1_1(df):
    """Performs a detailed validation of the BP1_1 column."""
    if 'BP1_1' not in df.columns:
        return {"error": "BP1_1 column not found"}
    
    col = df['BP1_1']
    nulls_count = int(col.isnull().sum())
    total_count = len(col)
    unique_values = col.dropna().unique().tolist()
    
    validos = [v for v in unique_values if v in BP1_1_VALID_VALUES]
    invalidos = [v for v in unique_values if v not in BP1_1_VALID_VALUES]
    
    distribucion = {str(v): int(col.eq(v).sum()) for v in validos}

    return {
        "valores_validos": bool(validos),
        "valores_unicos": unique_values,
        "valores_invalidos": invalidos,
        "distribucion": distribucion,
        "nulls_count": nulls_count,
        "nulls_percentage": round((nulls_count / total_count) * 100, 2) if total_count > 0 else 0,
        "muestra_size": total_count
    }

def validate_yucatan(df):
    """Performs validation related to 'YUCATAN' data."""
    if 'NOM_ENT' not in df.columns:
        return {"error": "NOM_ENT column not found"}

    df['NOM_ENT_NORMALIZED'] = df['NOM_ENT'].apply(normalize_string)
    
    total_registros = len(df)
    yucatan_rows = df[df['NOM_ENT_NORMALIZED'] == 'YUCATAN']
    registros_yucatan = len(yucatan_rows)
    
    return {
        "tiene_yucatan": registros_yucatan > 0,
        "registros_yucatan": registros_yucatan,
        "total_registros": total_registros,
        "porcentaje_yucatan": round((registros_yucatan / total_registros) * 100, 2) if total_registros > 0 else 0,
        "estados_unicos": int(df['NOM_ENT_NORMALIZED'].nunique())
    }
