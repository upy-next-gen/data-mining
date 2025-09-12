import argparse
import json
import os
import sys
import unicodedata
from datetime import datetime
from typing import List, Dict, Any

import pandas as pd

from generate_report import (
    find_data_dir,
    detect_files,
    load_frames,
    normalize_fields,
    build_charts,
    build_tables_html,
    md5_of_files,
    human_int,
)
from jinja2 import Environment, FileSystemLoader, select_autoescape


def error(msg: str) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(1)


def norm(s: str) -> str:
    if not isinstance(s, str):
        return ''
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s.upper().strip()


def render_html(context: Dict[str, Any], out_path: str) -> None:
    env = Environment(loader=FileSystemLoader('.'), autoescape=select_autoescape(['html', 'xml']))
    tpl = env.get_template('templates/report_public.html.j2')
    html = tpl.render(**context)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as fh:
        fh.write(html)


def load_meta(meta_path: str) -> Dict[str, Any]:
    if not meta_path:
        return {}
    try:
        with open(meta_path, 'r', encoding='utf-8') as fh:
            return json.load(fh)
    except Exception:
        return {}


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description='Reporte público y claro centrado en Yucatán')
    parser.add_argument('--entity', default='YUCATAN', help='Entidad a filtrar (ej. YUCATAN)')
    parser.add_argument('--out', default=os.path.join('reports', 'reporte_yucatan_publico.html'))
    parser.add_argument('--assets', default='assets')
    parser.add_argument('--meta', default='meta.json')
    args = parser.parse_args(argv)

    data_dir = find_data_dir()
    patterns = ['*.csv', '*.json', '*.parquet']
    files = detect_files(data_dir, patterns)
    if not files:
        error(f"No hay archivos de datos en {data_dir}")

    df = normalize_fields(load_frames(files))

    # Filtro por entidad: asume columna NOM_ENT o ENT_CODE normalizada
    if 'NOM_ENT' in df.columns:
        mask = df['NOM_ENT'].astype(str).map(norm) == norm(args.entity)
    elif 'ENT_CODE' in df.columns:
        mask = df['ENT_CODE'].astype(str).map(norm) == norm(args.entity)
    else:
        mask = pd.Series([True] * len(df))  # si no existe, asumimos ya filtrado

    df_yuc = df[mask].copy()
    if df_yuc.empty:
        error(f"No se encontraron registros para la entidad: {args.entity}")

    # KPIs amigables
    kpis = [
        {'label': 'Registros', 'value': human_int(len(df_yuc)), 'note': 'Total de filas analizadas'},
    ]
    if 'NOM_MUN' in df_yuc.columns:
        kpis.append({'label': 'Municipios', 'value': human_int(df_yuc['NOM_MUN'].nunique(dropna=True)), 'note': 'Incluidos en el análisis'})
    if 'Pct_Inseguros' in df_yuc.columns and df_yuc['Pct_Inseguros'].notna().any():
        s = df_yuc['Pct_Inseguros'].dropna()
        kpis.append({'label': 'Promedio estatal', 'value': f"{s.mean():.1f}%", 'note': 'Percepción de inseguridad'})

    # Bullets sencillos
    bullets = []
    if 'Fecha' in df_yuc.columns and 'Pct_Inseguros' in df_yuc.columns:
        t = df_yuc[['Fecha', 'Pct_Inseguros']].dropna()
        if not t.empty:
            t = t.groupby('Fecha')['Pct_Inseguros'].mean().sort_index()
            first, last = t.iloc[0], t.iloc[-1]
            delta = last - first
            trend = 'aumentó' if delta > 0 else 'disminuyó' if delta < 0 else 'se mantuvo'
            bullets.append(f"En el periodo analizado, el promedio {trend} {abs(delta):.1f} puntos porcentuales.")

    if 'NOM_MUN' in df_yuc.columns and 'Pct_Inseguros' in df_yuc.columns:
        m = df_yuc[['NOM_MUN', 'Pct_Inseguros']].dropna()
        if not m.empty:
            g = m.groupby('NOM_MUN')['Pct_Inseguros'].mean().sort_values()
            bullets.append(f"Municipio con menor percepción: {g.index[0]} ({g.iloc[0]:.1f}%).")
            bullets.append(f"Municipio con mayor percepción: {g.index[-1]} ({g.iloc[-1]:.1f}%).")

    # Tablas amigables (muestra)
    tables = build_tables_html(df_yuc, pd.DataFrame())

    # Gráficas reutilizando especificaciones de build_charts
    charts = build_charts(df_yuc)

    # Vendor (offline) detection and paths
    out_dir = os.path.dirname(args.out) or '.'
    assets_rel = os.path.relpath(args.assets, start=out_dir)
    vendor_dir = os.path.join(args.assets, 'vendor')
    vendor = {
        'jquery_js': os.path.join(vendor_dir, 'jquery-3.7.1.min.js'),
        'dt_js': os.path.join(vendor_dir, 'datatables.min.js'),
        'dt_css': os.path.join(vendor_dir, 'datatables.min.css'),
        'plotly_js': os.path.join(vendor_dir, 'plotly-2.26.0.min.js'),
    }
    vendor_available = {k: os.path.exists(v) for k, v in vendor.items()}

    meta = {}
    if args.meta:
        meta = load_meta(args.meta)

    context = {
        'meta': meta,
        'data_dir': os.path.relpath(data_dir),
        'n_files': len(files),
        'total_rows': human_int(len(df_yuc)),
        'kpis': kpis,
        'bullets': bullets or [
            'Los valores muestran cómo se sienten las personas, no cuántos delitos hubo.',
            'Compara municipios con cautela: tamaños de muestra y periodos pueden variar.',
        ],
        'charts_json': json.dumps([{k: v for k, v in c.items() if k in ('id', 'spec')} for c in charts], ensure_ascii=False),
        'sample_table': tables['sample_table'],
        'generation_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'assets': assets_rel,
        'vendor': {k: os.path.relpath(v, start=out_dir) for k, v in vendor.items()},
        'vendor_available': vendor_available,
    }

    try:
        render_html(context, args.out)
    except Exception as e:
        error(f"No se pudo crear el HTML: {e}")

    print(f"Reporte público generado: {args.out}")


if __name__ == '__main__':
    main()

