import argparse
import glob
import hashlib
import json
import os
import sys
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape


def error(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)


def find_data_dir() -> str:
    # Robust search for folder named 'yucatan-inseguridad'
    for root, dirs, _ in os.walk('.', topdown=True):
        if 'yucatan-inseguridad' in dirs:
            return os.path.join(root, 'yucatan-inseguridad')
    error("No se encontró la carpeta yucatan-inseguridad")


def detect_files(data_dir: str, patterns: List[str]) -> List[str]:
    files: List[str] = []
    for pat in patterns:
        files.extend(glob.glob(os.path.join(data_dir, '**', pat), recursive=True))
    # Keep only files, deduplicate
    files = sorted({f for f in files if os.path.isfile(f)})
    return files


def human_int(n: int) -> str:
    return f"{n:,}".replace(',', '_').replace('_', ',')


def md5_of_files(paths: List[str]) -> str:
    h = hashlib.md5()
    for p in sorted(paths):
        try:
            with open(p, 'rb') as fh:
                while True:
                    chunk = fh.read(1024 * 1024)
                    if not chunk:
                        break
                    h.update(chunk)
        except Exception as e:
            # Non-fatal, but report
            print(f"[WARN] No se pudo leer para checksum: {p}: {e}", file=sys.stderr)
    return h.hexdigest()


def load_frames(files: List[str]) -> pd.DataFrame:
    frames = []
    csvs = [f for f in files if f.lower().endswith('.csv')]
    jsons = [f for f in files if f.lower().endswith('.json')]
    pars = [f for f in files if f.lower().endswith('.parquet')]
    tried = 0

    for f in csvs:
        tried += 1
        try:
            frames.append(pd.read_csv(f))
        except Exception as e:
            print(f"[WARN] Error leyendo CSV {f}: {e}", file=sys.stderr)

    # JSON lines or array
    for f in jsons:
        tried += 1
        try:
            frames.append(pd.read_json(f, lines=False))
        except ValueError:
            try:
                frames.append(pd.read_json(f, lines=True))
            except Exception as e:
                print(f"[WARN] Error leyendo JSON {f}: {e}", file=sys.stderr)
        except Exception as e:
            print(f"[WARN] Error leyendo JSON {f}: {e}", file=sys.stderr)

    for f in pars:
        tried += 1
        try:
            frames.append(pd.read_parquet(f))
        except Exception as e:
            print(f"[WARN] Error leyendo Parquet {f}: {e}", file=sys.stderr)

    if not frames:
        error("No se pudieron cargar datasets (CSV/JSON/Parquet)")

    try:
        df = pd.concat(frames, ignore_index=True, sort=False)
    except Exception as e:
        error(f"Fallo al concatenar datasets: {e}")

    return df


def normalize_fields(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # Standardize expected columns if present
    # Year and quarter
    if 'AÑO' in out.columns:
        out['Anio'] = pd.to_numeric(out['AÑO'], errors='coerce').astype('Int64')
    elif 'Anio' not in out.columns:
        out['Anio'] = pd.NA

    if 'TRIMESTRE' in out.columns:
        out['Trimestre'] = out['TRIMESTRE'].astype(str)
    elif 'Trimestre' not in out.columns:
        out['Trimestre'] = pd.NA

    # Municipality and entity fallbacks
    if 'NOM_MUN' not in out.columns and 'MUN_CODE' in out.columns:
        out['NOM_MUN'] = out['MUN_CODE']
    if 'NOM_ENT' not in out.columns and 'ENT_CODE' in out.columns:
        out['NOM_ENT'] = out['ENT_CODE']

    # Percent insecure
    if 'PCT_INSEGUROS' in out.columns:
        out['Pct_Inseguros'] = pd.to_numeric(out['PCT_INSEGUROS'], errors='coerce')
    elif {'TOTAL_INSEGUROS', 'TOTAL_REGISTROS'}.issubset(out.columns):
        out['Pct_Inseguros'] = (pd.to_numeric(out['TOTAL_INSEGUROS'], errors='coerce') /
                                pd.to_numeric(out['TOTAL_REGISTROS'], errors='coerce') * 100)
    else:
        out['Pct_Inseguros'] = pd.NA

    # Date index
    try:
        out['Fecha'] = pd.to_datetime(out['Anio'].astype('Int64').astype(str) + 'Q' + out['Trimestre'].astype(str).str[0], errors='coerce')
    except Exception:
        out['Fecha'] = pd.NaT

    return out


def profile_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for col in df.columns:
        s = df[col]
        dtype = str(s.dtype)
        nulls = int(s.isna().sum())
        null_pct = (nulls / len(df) * 100) if len(df) else 0
        unique = int(s.nunique(dropna=True))
        sample = None
        if unique <= 10:
            sample = ", ".join(map(str, s.dropna().unique()[:10]))
        rows.append({
            'columna': col,
            'tipo': dtype,
            'nulos': nulls,
            '% nulos': round(null_pct, 2),
            'cardinalidad': unique,
            'muestra': sample or ''
        })
    return pd.DataFrame(rows)


def build_charts(df: pd.DataFrame) -> List[Dict[str, Any]]:
    import plotly.express as px
    import plotly.graph_objects as go

    charts = []

    # 1. Line: Pct_Inseguros over Fecha
    if 'Fecha' in df.columns and 'Pct_Inseguros' in df.columns:
        d1 = df[['Fecha', 'Pct_Inseguros']].dropna()
        if not d1.empty:
            s = d1.groupby('Fecha', as_index=False)['Pct_Inseguros'].mean().sort_values('Fecha')
            fig = px.line(s, x='Fecha', y='Pct_Inseguros', markers=True, title='Tendencia de percepción de inseguridad (promedio)')
            fig.update_layout(yaxis_title='% inseguridad', xaxis_title='Fecha')
            charts.append({'id': 'chart_trend', 'title': 'Tendencia', 'spec': fig.to_plotly_json(), 'note': 'Promedio trimestral en todos los municipios.'})

    # 2. Bar: top 10 municipios por Pct_Inseguros promedio
    mun_col = 'NOM_MUN' if 'NOM_MUN' in df.columns else None
    if mun_col and 'Pct_Inseguros' in df.columns:
        d2 = df[[mun_col, 'Pct_Inseguros']].dropna()
        if not d2.empty:
            s = d2.groupby(mun_col)['Pct_Inseguros'].mean().sort_values(ascending=False).head(10).reset_index()
            fig = px.bar(s, x='Pct_Inseguros', y=mun_col, orientation='h', title='Top 10 municipios por percepción de inseguridad (promedio)')
            fig.update_layout(xaxis_title='% inseguridad', yaxis_title='Municipio')
            charts.append({'id': 'chart_top_mun', 'title': 'Top municipios', 'spec': fig.to_plotly_json(), 'note': 'Promedio en el periodo disponible.'})

    # 3. Histogram of Pct_Inseguros
    if 'Pct_Inseguros' in df.columns:
        d3 = df['Pct_Inseguros'].dropna()
        if not d3.empty:
            fig = px.histogram(d3, nbins=30, title='Distribución de percepción de inseguridad')
            fig.update_layout(xaxis_title='% inseguridad', yaxis_title='Frecuencia')
            charts.append({'id': 'chart_hist', 'title': 'Distribución', 'spec': fig.to_plotly_json(), 'note': 'Muestra la dispersión y concentración de valores.'})

    # 4. Boxplot por año
    if 'Anio' in df.columns and 'Pct_Inseguros' in df.columns:
        d4 = df[['Anio', 'Pct_Inseguros']].dropna()
        if not d4.empty:
            fig = px.box(d4, x='Anio', y='Pct_Inseguros', title='Caja y bigotes por año')
            fig.update_layout(xaxis_title='Año', yaxis_title='% inseguridad')
            charts.append({'id': 'chart_box_year', 'title': 'Caja por año', 'spec': fig.to_plotly_json(), 'note': 'Variabilidad interanual.'})

    # 5. Heatmap Año x Trimestre
    if {'Anio', 'Trimestre', 'Pct_Inseguros'}.issubset(df.columns):
        d5 = df[['Anio', 'Trimestre', 'Pct_Inseguros']].dropna()
        if not d5.empty:
            p = d5.pivot_table(index='Anio', columns='Trimestre', values='Pct_Inseguros', aggfunc='mean')
            fig = go.Figure(data=go.Heatmap(z=p.values, x=list(p.columns), y=list(p.index), colorscale='YlOrRd'))
            fig.update_layout(title='Heatmap: % inseguridad (Año x Trimestre)', xaxis_title='Trimestre', yaxis_title='Año')
            charts.append({'id': 'chart_heat', 'title': 'Heatmap temporal', 'spec': fig.to_plotly_json(), 'note': 'Promedio por cruce de año y trimestre.'})

    return charts


def build_tables_html(df: pd.DataFrame, profiling: pd.DataFrame) -> Dict[str, str]:
    # DataTables expects class 'datatable'
    prof_html = profiling.to_html(index=False, classes=['dataframe', 'datatable'])
    describe_html = df.select_dtypes('number').describe(percentiles=[.25, .5, .75]).round(2).to_html(classes=['dataframe', 'datatable'])
    sample_html = df.head(1000).to_html(index=False, classes=['dataframe', 'datatable'])
    return {
        'profiling_table': prof_html,
        'describe_table': describe_html,
        'sample_table': sample_html,
    }


def load_meta(meta_path: str) -> Dict[str, Any]:
    if not meta_path:
        return {}
    try:
        with open(meta_path, 'r', encoding='utf-8') as fh:
            return json.load(fh)
    except Exception as e:
        print(f"[WARN] No se pudo leer meta JSON: {e}", file=sys.stderr)
        return {}


def render_html(context: Dict[str, Any], out_path: str) -> None:
    env = Environment(
        loader=FileSystemLoader('.'),
        autoescape=select_autoescape(['html', 'xml'])
    )
    tpl = env.get_template('templates/report.html.j2')
    html = tpl.render(**context)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as fh:
        fh.write(html)


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description='Generador de reporte HTML (Plotly + DataTables)')
    parser.add_argument('--data', nargs='*', help='Patrones de archivo (globs). Por defecto, CSV/JSON/Parquet en yucatan-inseguridad')
    parser.add_argument('--out', default=os.path.join('reports', 'reporte.html'), help='Ruta de salida del HTML')
    parser.add_argument('--assets', default='assets', help='Carpeta de assets (css/js/img)')
    parser.add_argument('--meta', default=None, help='Ruta a JSON con metadatos (título, autores, logo, notas)')
    args = parser.parse_args(argv)

    data_dir = find_data_dir()
    if args.data:
        patterns = args.data
    else:
        patterns = ['*.csv', '*.json', '*.parquet']

    files = detect_files(data_dir, patterns)
    if not files:
        error(f"No hay archivos de datos en {data_dir} con patrones {patterns}")

    df_raw = load_frames(files)
    df = normalize_fields(df_raw)

    profiling = profile_dataframe(df)
    tables = build_tables_html(df, profiling)

    # KPIs
    kpis = []
    total_rows = len(df)
    kpis.append({'label': 'Filas', 'value': human_int(total_rows), 'note': 'Total de registros combinados'})
    if 'NOM_MUN' in df.columns:
        kpis.append({'label': 'Municipios', 'value': human_int(df['NOM_MUN'].nunique(dropna=True)), 'note': 'Únicos'})
    if 'Anio' in df.columns and df['Anio'].notna().any():
        anios = df['Anio'].dropna().astype(int)
        kpis.append({'label': 'Años', 'value': f"{anios.min()}–{anios.max()}"})
    if 'Pct_Inseguros' in df.columns and df['Pct_Inseguros'].notna().any():
        s = df['Pct_Inseguros'].dropna()
        kpis.append({'label': 'Promedio % inseguridad', 'value': f"{s.mean():.2f}%"})

    # Charts and inline PlotlyJS
    charts = build_charts(df)
    try:
        from plotly.offline import get_plotlyjs
        plotly_js = get_plotlyjs()
    except Exception:
        # Fallback: no inline library (will rely on CDN if added later)
        plotly_js = """
        /*! Plotly.js no incrustado; se requiere conexión o librería local. */
        """

    meta = load_meta(args.meta)
    generation_date = datetime.now().strftime('%Y-%m-%d %H:%M')

    # Context for template
    context = {
        'meta': meta,
        'data_dir': os.path.relpath(data_dir),
        'n_files': len(files),
        'file_types': sorted({os.path.splitext(f)[1].lstrip('.').lower() for f in files}),
        'total_rows': human_int(total_rows),
        'kpis': kpis,
        'charts': charts,
        'charts_json': json.dumps([{k: v for k, v in c.items() if k in ('id', 'spec')} for c in charts], ensure_ascii=False),
        'profiling_table': tables['profiling_table'],
        'describe_table': tables['describe_table'],
        'sample_table': tables['sample_table'],
        'summary_text': meta.get('summary', 'Este reporte resume y visualiza la percepción de inseguridad a partir de los datos disponibles.'),
        'bullets': meta.get('bullets', [
            'Consolidación de múltiples archivos de datos.',
            'KPIs y visualizaciones interactivas con Plotly.',
            'Tablas navegables con DataTables.',
        ]),
        'version': meta.get('version', '1.0.0'),
        'generation_date': generation_date,
        'checksum': md5_of_files(files),
        'plotly_js': plotly_js,
    }

    # Ensure assets path exists (so relative links work)
    if not os.path.isdir(args.assets):
        print(f"[WARN] Carpeta de assets no encontrada: {args.assets}. Se crearán rutas si es necesario.")

    try:
        render_html(context, args.out)
    except Exception as e:
        error(f"No se pudo renderizar el HTML: {e}")

    print(f"Reporte generado en: {args.out}")


if __name__ == '__main__':
    main()

