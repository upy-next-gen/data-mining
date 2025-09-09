# 📊 Reporte de Procesamiento ENSU - Metadatos y Análisis

**Fecha de generación:** 2025-09-08 17:50
**Pipeline ejecutado:** Fases 1-5 completadas

## 📈 Resumen Ejecutivo

### Estadísticas Generales
- **Total archivos analizados:** 80
- **Archivos procesables:** 20 (25%)
- **Archivos con datos de Yucatán:** 9 (45% de los procesables)
- **Total registros de Yucatán:** 2,337
- **Percepción de seguridad promedio:** 60.97%
- **Percepción de inseguridad promedio:** 39.03%

## 🗂️ Archivos SIN Datos de Yucatán (11 archivos)

Estos archivos fueron procesados pero no contenían registros de Yucatán:

| Archivo | Período | Registros Totales | Estado |
|---------|---------|------------------|--------|
| conjunto_de_datos_ensu_cb_0322.csv | 2022 T1 | 23,577 | ⚠️ Sin datos Yucatán |
| conjunto_de_datos_CB_ENSU_12_2021.csv | 2021 T4 | 23,428 | ⚠️ Sin datos Yucatán |
| conjunto_de_datos_ensu_cb_0923.csv | 2023 T3 | 24,493 | ⚠️ Sin datos Yucatán |
| conjunto_de_datos_CB_ENSU_06_2021.csv | 2021 T2 | 22,411 | ⚠️ Sin datos Yucatán |
| conjunto_de_datos_CB_ENSU_09_2021.csv | 2021 T3 | 23,356 | ⚠️ Sin datos Yucatán |
| conjunto_de_datos_ensu_cb_0623.csv | 2023 T2 | 24,435 | ⚠️ Sin datos Yucatán |
| conjunto_de_datos_CB_ENSU_12_2020.csv | 2020 T4 | 22,283 | ⚠️ Sin datos Yucatán |
| conjunto_de_datos_CB_ENSU_03_2023.csv | 2023 T1 | 23,778 | ⚠️ Sin datos Yucatán |
| conjunto_de_datos_CB_ENSU_03_2021.csv | 2021 T1 | 22,307 | ⚠️ Sin datos Yucatán |
| conjunto_de_datos_ensu_cb_0622.csv | 2022 T2 | 23,688 | ⚠️ Sin datos Yucatán |
| conjunto_de_datos_CB_SEC1_2_3_ENSU_09_2020.csv | 2020 T3 | 22,122 | ⚠️ Sin datos Yucatán |

## ✅ Archivos CON Datos de Yucatán Procesados (9 archivos)

| Archivo | Período | Registros Totales | Registros Yucatán | Municipios | Archivo Generado |
|---------|---------|------------------|-------------------|------------|------------------|
| conjunto_de_datos_ensu_cb_0625.csv | 2025 T2 | 23,717 | 255 | 4 | yucatan_2025_T2_CB.csv |
| conjunto_de_datos_ensu_cb_1223.csv | 2023 T4 | 24,064 | 268 | 3 | yucatan_2023_T4_CB.csv |
| conjunto_de_datos_ensu_cb_0324.csv | 2024 T1 | 24,022 | 257 | 3 | yucatan_2024_T1_CB.csv |
| conjunto_de_datos_ensu_cb_0924.csv | 2024 T3 | 24,096 | 258 | 4 | yucatan_2024_T3_CB.csv |
| conjunto_de_datos_ensu_cb_1222.csv | 2022 T4 | 24,402 | 262 | 4 | yucatan_2022_T4_CB.csv |
| conjunto_de_datos_ensu_cb_0624.csv | 2024 T2 | 24,114 | 256 | 4 | yucatan_2024_T2_CB.csv |
| conjunto_de_datos_ensu_cb_0325.csv | 2025 T1 | 23,591 | 258 | 4 | yucatan_2025_T1_CB.csv |
| conjunto_de_datos_ensu_cb_0922.csv | 2022 T3 | 23,618 | 263 | 4 | yucatan_2022_T3_CB.csv |
| conjunto_de_datos_ensu_cb_1224.csv | 2024 T4 | 23,451 | 260 | 4 | yucatan_2024_T4_CB.csv |

## 🚫 Archivos con Errores de Codificación (36 archivos)

Todos los archivos con error UTF-8 son de la carpeta `__MACOSX` (metadatos de macOS):

### Resumen:
- **Total archivos con error UTF-8:** 36
- **Ubicación:** Todos en `data/__MACOSX/`
- **Tipo de error:** `'utf-8' codec can't decode byte 0xa2 in position 37`
- **Acción tomada:** Ignorados automáticamente (no contienen datos relevantes)

### Archivos Rechazados por Columnas Faltantes (24 archivos)

Archivos válidos en formato pero sin las columnas requeridas:

| Tipo de Problema | Cantidad | Columnas Faltantes |
|-----------------|----------|-------------------|
| Sin columna NOM_CD | 12 | Solo falta NOM_CD |
| Sin múltiples columnas | 8 | NOM_CD, NOM_ENT, NOM_MUN |
| Sin BP1_1 | 4 | NOM_CD, NOM_ENT, NOM_MUN, BP1_1 |

## 📅 Cobertura Temporal

### Trimestres con Datos de Yucatán:
- **2022:** T3, T4 (falta T1, T2)
- **2023:** T4 (falta T1, T2, T3)
- **2024:** T1, T2, T3, T4 ✅ (año completo)
- **2025:** T1, T2 (datos más recientes)

### Trimestres sin Datos de Yucatán:
- **2020:** T3, T4
- **2021:** T1, T2, T3, T4 (año completo sin datos)
- **2022:** T1, T2
- **2023:** T1, T2, T3

## 🔍 Observaciones Importantes

1. **Patrón de Datos:** Los datos de Yucatán están disponibles principalmente desde 2022 T3 en adelante, con mejor cobertura en 2024-2025.

2. **Archivos __MACOSX:** 36 archivos son metadatos de macOS que no afectan el procesamiento. Se recomienda eliminar la carpeta `data/__MACOSX` para limpiar el espacio.

3. **Columnas Críticas:** El problema más común en archivos rechazados es la falta de la columna `NOM_CD` (nombre de ciudad).

4. **Valor de NOM_ENT:** Confirmado que el valor correcto es `'YUCATAN'` (mayúsculas sin tilde) en los datos ENSU.

## 📋 Recomendaciones

1. **Limpieza de Datos:**
   ```bash
   # Eliminar archivos __MACOSX innecesarios
   rm -rf data/__MACOSX
   ```

2. **Archivos Faltantes:** Investigar por qué los trimestres de 2020-2021 y algunos de 2022-2023 no contienen datos de Yucatán.

3. **Optimización:** Los 11 archivos sin datos de Yucatán podrían excluirse del procesamiento futuro para mejorar el rendimiento.

4. **Validación:** Verificar si los archivos con columnas faltantes son de versiones anteriores del formato ENSU que podrían procesarse con adaptaciones.

## 📊 Métricas de Calidad

- **Tasa de éxito de procesamiento:** 45% (9/20 archivos válidos con datos)
- **Cobertura de datos:** 10.1% de registros totales son de Yucatán (2,337/23,000 promedio)
- **Consistencia:** Los archivos procesados exitosamente tienen entre 3-4 municipios consistentemente
- **Datos por trimestre:** Promedio de 260 registros de Yucatán por trimestre

---

*Generado automáticamente por el pipeline de procesamiento ENSU*