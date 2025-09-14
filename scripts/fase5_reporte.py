import os, sys, shutil, logging
import logging.handlers
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
from string import Template
import json
import unicodedata

# ==============================
# Configuración de logs
# ==============================
def setup_logging():
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, "fase5_reporte.log")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.handlers.TimedRotatingFileHandler(
                log_filename, when='midnight', interval=1, backupCount=7, encoding="utf-8"
            ),
            logging.StreamHandler()
        ]
    )

# ==============================
# Utilidades
# ==============================
def remove_accents(text: str) -> str:
    if isinstance(text, str):
        return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    return text

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Columnas sin acentos, MAYÚSCULAS y AÑO→ANIO; sin espacios."""
    new_cols = []
    for c in df.columns:
        cc = remove_accents(str(c)).strip().upper().replace(' ', '_')
        cc = cc.replace('AÑO', 'ANIO')
        cc = cc.replace('ANO', 'ANIO')
        new_cols.append(cc)
    df.columns = new_cols
    return df

def find_consolidated_path():
    p1 = 'data/yucatan_processed/yucatan_security_consolidado_new.csv'
    p2 = 'data/yucatan_processed/yucatan_security_consolidado.csv'
    if os.path.exists(p1):
        return p1
    if os.path.exists(p2):
        return p2
    raise FileNotFoundError("No encontré ni yucatan_security_consolidado_new.csv ni yucatan_security_consolidado.csv en data/yucatan_processed/")

def safe_mean(series):
    return float(pd.to_numeric(series, errors='coerce').dropna().mean() or 0.0)

# ==============================
# Reporte
# ==============================
def generate_report():
    try:
        logging.info("Starting Phase 5: Reporting")

        reports_dir = 'reports'
        master_dataset_filename = 'dataset_maestro_yucatan.csv'
        html_report_filename = 'analysis_report.html'

        consolidated_csv_path = find_consolidated_path()
        os.makedirs(reports_dir, exist_ok=True)
        master_dataset_path = os.path.join(reports_dir, master_dataset_filename)
        html_report_path = os.path.join(reports_dir, html_report_filename)

        # ---- Cargar dataset
        df = pd.read_csv(consolidated_csv_path, delimiter=',', encoding='utf-8-sig')
        df = norm_cols(df)

        # Período: si no existe, intentamos ANIO/TRIMESTRE
        if 'PERIODO' not in df.columns:
            if 'ANIO' in df.columns and 'TRIMESTRE' in df.columns:
                df['PERIODO'] = df['ANIO'].astype(str) + 'T' + df['TRIMESTRE'].astype(str)
            else:
                raise ValueError("No se encontró PERIODO ni (ANIO, TRIMESTRE) para componerlo.")

        # Filter data up to the current period (accumulated report)
        current_year = datetime.now().year
        current_month = datetime.now().month
        current_quarter = (current_month - 1) // 3 + 1
        current_period_float = float(f"{current_year}.{current_quarter}")

        # Ensure 'PERIODO' column is comparable (e.g., '2025T3' becomes 2025.3)
        df['PERIODO_SORT_FILTER'] = df['PERIODO'].astype(str).str.replace('T','.', regex=False)
        df['PERIODO_SORT_FILTER'] = pd.to_numeric(df['PERIODO_SORT_FILTER'], errors='coerce')

        # Apply the filter
        df = df[df['PERIODO_SORT_FILTER'] <= current_period_float]
        df = df.drop(columns=['PERIODO_SORT_FILTER']) # Clean up the temporary column

        # ---- Chequeos de columnas base
        required = ['NOM_ENT','NOM_MUN','PCT_SEGUROS','PCT_INSEGUROS']
        for col in required:
            if col not in df.columns:
                raise ValueError(f"Falta columna requerida: {col}")

        # ---- Normalización y tipos
        for c in ['NOM_ENT', 'NOM_MUN']:
            if c in df.columns:
                df[c] = df[c].astype(str).map(remove_accents).str.upper().str.strip()

        # Nominal de ciudad si existe
        has_nom_cd = 'NOM_CD' in df.columns
        if has_nom_cd:
            df['NOM_CD'] = df['NOM_CD'].astype(str).map(remove_accents).str.upper().str.strip()

        # Valores numéricos
        df['PCT_SEGUROS'] = pd.to_numeric(df['PCT_SEGUROS'], errors='coerce')
        df['PCT_INSEGUROS'] = pd.to_numeric(df['PCT_INSEGUROS'], errors='coerce')

        # ---- Copia maestra del dataset
        shutil.copy(consolidated_csv_path, master_dataset_path)
        logging.info(f"Master dataset saved to {master_dataset_path}")
        master_dataset_basename = os.path.basename(master_dataset_path)

        # ---- orden por período
        df['PERIODO_SORT'] = df['PERIODO'].astype(str).str.replace('T','.', regex=False)
        df['PERIODO_SORT'] = pd.to_numeric(df['PERIODO_SORT'], errors='coerce')
        df = df.sort_values(by=['NOM_MUN','PERIODO_SORT'], kind='mergesort')

        # ---- Resúmenes sintetizados
        df_mun_avg = (
            df.groupby('NOM_MUN', as_index=False)
              .agg(AVG_PCT_SEGUROS=('PCT_SEGUROS','mean'),
                   AVG_PCT_INSEGUROS=('PCT_INSEGUROS','mean'),
                   N_OBS=('PCT_SEGUROS','count'))
              .sort_values('AVG_PCT_SEGUROS', ascending=False)
        )

        df_period_avg = (
            df.groupby('PERIODO', as_index=False)
              .agg(AVG_PCT_SEGUROS=('PCT_SEGUROS','mean'),
                   AVG_PCT_INSEGUROS=('PCT_INSEGUROS','mean'),
                   N_OBS=('PCT_SEGUROS','count'))
        )
        df_period_avg['PERIODO_SORT'] = pd.to_numeric(df_period_avg['PERIODO'].str.replace('T','.', regex=False), errors='coerce')
        df_period_avg = df_period_avg.sort_values('PERIODO_SORT')

        # ---- Análisis de Volatilidad
        df_volatility = df.groupby('NOM_MUN')['PCT_SEGUROS'].std().reset_index()
        df_volatility.rename(columns={'PCT_SEGUROS': 'VOLATILIDAD'}, inplace=True)
        df_volatility = df_volatility.sort_values('VOLATILIDAD', ascending=False)
        volatility_html = df_volatility.round(2).to_html(index=False, classes='table-pink')

        # ---- Análisis Anual
        df['ANIO'] = df['PERIODO'].str.split('T').str[0]
        df_anual_avg = df.groupby('ANIO').agg(AVG_PCT_SEGUROS=('PCT_SEGUROS','mean')).reset_index()
        fig_anual = px.bar(df_anual_avg, x='ANIO', y='AVG_PCT_SEGUROS', title='Percepción de Seguridad Promedio por Año',
                             labels={'ANIO': 'Año', 'AVG_PCT_SEGUROS': 'Seguridad Promedio (%)'},
                             text=df_anual_avg['AVG_PCT_SEGUROS'].apply(lambda x: f'{x:.2f}%'))
        fig_anual.update_traces(marker_color='#EC407A', textposition='outside')

        # Extremes por período
        if not df_period_avg.empty:
            periodo_max = df_period_avg.loc[df_period_avg['AVG_PCT_SEGUROS'].idxmax(), 'PERIODO']
            periodo_min = df_period_avg.loc[df_period_avg['AVG_PCT_SEGUROS'].idxmin(), 'PERIODO']
            start_period = df_period_avg.iloc[0]
            end_period = df_period_avg.iloc[-1]
            trend_summary_text = f"""La percepción de seguridad promedio comenzó en <b>{start_period['AVG_PCT_SEGUROS']:.2f}%</b> en el período {start_period['PERIODO']} 
                                     y finalizó en <b>{end_period['AVG_PCT_SEGUROS']:.2f}%</b> en {end_period['PERIODO']}. 
                                     El punto más alto se alcanzó en <b>{periodo_max}</b> con un <b>{df_period_avg['AVG_PCT_SEGUROS'].max():.2f}%</b>, 
                                     mientras que el más bajo fue en <b>{periodo_min}</b> con un <b>{df_period_avg['AVG_PCT_SEGUROS'].min():.2f}%</b>."""
        else:
            periodo_max = periodo_min = 'N/A'
            trend_summary_text = "No hay suficientes datos para generar un análisis de tendencia."

        # Top/Bottom municipios
        top_muns = df_mun_avg.head(2).copy()
        bottom_muns = df_mun_avg.tail(2).copy().sort_values('AVG_PCT_SEGUROS', ascending=True)

        # ---- Conclusiones dinámicas
        top_mun_names = top_muns['NOM_MUN'].tolist()
        bottom_mun_names = bottom_muns['NOM_MUN'].tolist()
        conclusion_text = f"""
El análisis de los datos recopilados revela varias tendencias clave.
El trimestre con la percepción de seguridad más alta fue <b>{periodo_max}</b>,
mientras que el trimestre con la percepción más baja fue <b>{periodo_min}</b>.
<br><br>
En el ámbito municipal, los municipios de <b>{', '.join(top_mun_names)}</b>
consistentemente muestran los niveles más altos de percepción de seguridad.
Por otro lado, los municipios de <b>{', '.join(bottom_mun_names)}</b>
presentan áreas de oportunidad significativas, con una percepción de inseguridad más marcada.
<br><br>
Estos resultados sugieren que, si bien hay una base sólida de seguridad en ciertas áreas,
es crucial enfocar los esfuerzos en las zonas con menor percepción de seguridad para entender
las causas subyacentes y desarrollar estrategias efectivas de mejora.
"""

        # Persistir resúmenes
        df_mun_avg.to_csv(os.path.join(reports_dir, 'resumen_municipio.csv'), index=False, encoding='utf-8-sig')
        df_period_avg.to_csv(os.path.join(reports_dir, 'resumen_periodo.csv'), index=False, encoding='utf-8-sig')
        top_muns.to_csv(os.path.join(reports_dir, 'top_municipios.csv'), index=False, encoding='utf-8-sig')
        bottom_muns.to_csv(os.path.join(reports_dir, 'bottom_municipios.csv'), index=False, encoding='utf-8-sig')
        pd.DataFrame({'TRIMESTRE_MAX':[periodo_max],'TRIMESTRE_MIN':[periodo_min]}).to_csv(
            os.path.join(reports_dir, 'outliers_periodo.csv'), index=False, encoding='utf-8-sig'
        )

        # ---- Métricas generales
        prom_seg = safe_mean(df['PCT_SEGUROS'])
        prom_inseg = safe_mean(df['PCT_INSEGUROS'])

        # ---- Figuras (rosa vibes ✨)
        pastel = px.colors.qualitative.Pastel

        # Evolución por municipio (líneas)
        fig_trend = px.line(
            df, x='PERIODO', y='PCT_SEGUROS', color='NOM_MUN',
            title='Evolución de la Percepción de Seguridad por Municipio',
            labels={'PCT_SEGUROS':'% Seguros'}, color_discrete_sequence=pastel
        )

        # Pie general
        fig_pie = go.Figure(data=[go.Pie(
            labels=['Segur@s', 'Insegur@s'],
            values=[prom_seg, prom_inseg],
            hole=.4, marker=dict(colors=['#EC407A', '#F8BBD0'])
        )])
        fig_pie.update_layout(title_text='Percepción General (Promedios)')

        # ---- Mini tablas HTML
        top_html = top_muns.round(2).to_html(index=False, classes='table-pink')
        bottom_html = bottom_muns.round(2).to_html(index=False, classes='table-pink')
        period_html = df_period_avg.round(2)[['PERIODO','AVG_PCT_SEGUROS','AVG_PCT_INSEGUROS','N_OBS']].to_html(index=False, classes='table-pink')
        mun_summary_html = df_mun_avg.round(2).to_html(index=False, classes='table-pink')

        # ---- Serializaciones JS
        fig_trend_json = fig_trend.to_json()
        fig_pie_json = fig_pie.to_json()
        fig_anual_json = fig_anual.to_json()

        unique_periods = sorted(df['PERIODO'].dropna().unique().tolist(), key=lambda x: float(str(x).replace('T','.')))
        unique_muns = sorted(df['NOM_MUN'].dropna().unique().tolist())
        unique_periods_js = json.dumps(unique_periods, ensure_ascii=False)
        unique_muns_js = json.dumps(unique_muns, ensure_ascii=False)

        # Datos para filtro dinámico
        data_for_js = df[['PERIODO','NOM_MUN','PCT_SEGUROS','PCT_INSEGUROS']].round(2).to_dict(orient='records')
        data_for_js_json = json.dumps(data_for_js, ensure_ascii=False)

        # ---- HTML
        html_tpl = Template(r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Percepción de Seguridad en Yucatán</title>
<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
<style>
  :root{
    --pink0:#FFF0F5; --pink1:#FCE4EC; --pink2:#F8BBD0; --pink3:#F48FB1; --pink4:#EC407A; --pink5:#C2185B;
    --dark-bg: #121212; --dark-card: #1E1E1E; --dark-text: #E0E0E0; --dark-border: #424242;
  }
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:0;background:var(--pink0);color:#333; transition: background-color 0.3s, color 0.3s;}
  .container{max-width:1100px;margin:0 auto;padding:24px;}
  .card{background:white;border-radius:14px;box-shadow:0 10px 20px rgba(0,0,0,.08);padding:24px;margin:18px 0; transition: background-color 0.3s;}
  h1{color:var(--pink5);text-align:center;margin:8px 0 4px;}
  h2{color:var(--pink4);border-bottom:3px solid var(--pink2);padding-bottom:6px;margin-top:8px}
  .sub{color:#555;text-align:center;margin-bottom:16px}
  .kpi{display:grid;grid-template-columns:repeat(2,1fr);gap:12px}
  .kpi .card{background:linear-gradient(180deg, var(--pink2), #fff);}
  .pill{display:inline-block;background:var(--pink2);padding:4px 10px;border-radius:999px;color:#7A1B3A;margin-right:6px}
  .hl{background:var(--pink1);padding:8px;border-radius:8px}
  .grid{display:grid;grid-template-columns:1fr;gap:18px}
  @media(min-width:960px){.grid{grid-template-columns:1fr 1fr}}
  .table-pink{border-collapse:collapse;width:100%;font-size:14px}
  .table-pink th,.table-pink td{border:1px solid var(--pink2);padding:8px}
  .table-pink th{background:var(--pink2);color:#7A1B3A}
  .filters{display:flex;gap:12px;flex-wrap:wrap;margin:8px 0}
  select{padding:8px;border:2px solid var(--pink2);border-radius:10px}
  footer{color:#666;text-align:center;margin:40px 0 10px}
  .theme-switch-wrapper {display: flex;align-items: center;justify-content: flex-end;}
  .theme-switch {display: inline-block;height: 24px;position: relative;width: 50px;}
  .theme-switch input {display:none;}
  .slider {background-color: #ccc;bottom: 0;cursor: pointer;left: 0;position: absolute;right: 0;top: 0;transition: .4s;}
  .slider:before {background-color: #fff;bottom: 4px;content: "";height: 16px;left: 4px;position: absolute;transition: .4s;width: 16px;}
  input:checked + .slider {background-color: var(--pink4);}
  input:checked + .slider:before {transform: translateX(26px);}
  .slider.round {border-radius: 34px;}
  .slider.round:before {border-radius: 50%;}
  body.dark-mode {background: var(--dark-bg); color: var(--dark-text);}
  body.dark-mode .card {background: var(--dark-card);}
  body.dark-mode h1 {color: var(--pink3);}
  body.dark-mode h2 {color: var(--pink4); border-bottom-color: var(--dark-border);}
  body.dark-mode .sub {color: #aaa;}
  body.dark-mode .table-pink th, body.dark-mode .table-pink td {border-color: var(--dark-border);}
  body.dark-mode .table-pink th {background: #333; color: var(--pink3);}
  body.dark-mode select {background: var(--dark-card); color: var(--dark-text); border-color: var(--dark-border);}
</style>
</head>
<body>
<div class="container">
  <div class="theme-switch-wrapper">
    <label class="theme-switch" for="checkbox">
      <input type="checkbox" id="checkbox" />
      <div class="slider round"></div>
    </label>
  </div>
  <h1>🌸 Análisis de Percepción de Seguridad en Yucatán</h1>
  <p class="sub">Fuente: <code>$master_dataset_basename</code> • Generado: $fecha_generado</p>
  <div class="card">
    <p>Este reporte presenta un análisis de la percepción de seguridad en varios municipios de Yucatán, basado en datos recopilados a lo largo de diferentes períodos. El objetivo es visualizar tendencias, comparar municipios y extraer insights clave sobre la seguridad en la región.</p>
  </div>
  <div class="kpi">
    <div class="card">
      <h2>🔐 Promedio Seguridad</h2>
      <p class="hl"><strong>$prom_seg%</strong> de personas se sienten <span class="pill">Seguras</span> en promedio.</p>
      <p>Trimestre con mayor seguridad: <strong>$periodo_max</strong></p>
    </div>
    <div class="card">
      <h2>⚠️ Promedio Inseguridad</h2>
      <p class="hl"><strong>$prom_inseg%</strong> se sienten <span class="pill">Inseguras</span> en promedio.</p>
      <p>Trimestre con menor seguridad: <strong>$periodo_min</strong></p>
    </div>
  </div>

  <div class="card">
    <h2>📈 Análisis de Tendencia General</h2>
    <p>$trend_summary_text</p>
  </div>

  <div class="card">
      <h2>🏙️ Resumen por Municipio</h2>
      <p>La siguiente tabla muestra el porcentaje promedio de percepción de seguridad e inseguridad para cada municipio, junto con el número de observaciones (encuestas) consideradas.</p>
      $mun_summary_html
  </div>

  <div class="card">
    <h2>📊 Comparativa Anual</h2>
    <p>Este gráfico de barras muestra la percepción de seguridad promedio para cada año, permitiendo una comparación clara de la evolución a largo plazo.</p>
    <div id="anual"></div>
  </div>

  <div class="card">
    <h2>⚡ Análisis de Volatilidad</h2>
    <p>La volatilidad (calculada como la desviación estándar) mide qué tan estables son los niveles de percepción de seguridad en un municipio. Un valor alto indica grandes fluctuaciones, mientras que un valor bajo sugiere una percepción más consistente a lo largo del tiempo.</p>
    $volatility_html
  </div>

  <div class="card">
    <h2>⏳ Evolución por municipio</h2>
    <p>Este gráfico interactivo permite analizar la tendencia de la percepción de seguridad de forma individual para cada municipio. Use el selector para elegir un municipio y observar su evolución a lo largo de los diferentes períodos.</p>
    <div class="filters">
      <label>Municipio:
        <select id="munSel"></select>
      </label>
    </div>
    <div id="trend"></div>
  </div>

  <div class="grid">
    <div class="card">
      <h2>🔍 Muestra del dataset</h2>
      <div class="filters">
        <label>Municipio:
          <select id="sampleMunSel"></select>
        </label>
      </div>
      <div id="sample_table"></div>
    </div>
    <div class="card">
      <h2>🥇 Top 2 municipios más seguros</h2>
      $top_html
      <h2>🥀 Bottom 2 municipios con menor seguridad</h2>
      $bottom_html
    </div>
  </div>

  <div class="card">
    <h2>🗓️ Resumen por período</h2>
    $period_html
  </div>

  <div class="card">
    <h2>💡 Conclusiones</h2>
    <p>$conclusion_text</p>
  </div>

  <footer>Hecho con cariño en tonos rosa ✨</footer>
</div>

<script>
  // Cargar figuras
  var fig_trend   = $fig_trend_json;
  var fig_pie     = $fig_pie_json;
  var fig_anual   = $fig_anual_json;

  var light_template = 'plotly_white';
  var dark_template = 'plotly_dark';

  Plotly.newPlot('trend', fig_trend.data, fig_trend.layout, {responsive:true});
  Plotly.newPlot('anual', fig_anual.data, fig_anual.layout, {responsive:true});

  // Datos para filtro
  var allData = $data_for_js_json; // [{PERIODO,NOM_MUN,PCT_SEGUROS,PCT_INSEGUROS}]
  var uniqueMuns = $unique_muns_js;

  // Poblar selector de evolución
  var sel = document.getElementById('munSel');
  uniqueMuns.forEach(function(m){ var opt=document.createElement('option'); opt.value=m; opt.text=m; sel.appendChild(opt); });
  if(uniqueMuns.length>0){ sel.value = uniqueMuns[0]; }

  // Poblar selector de muestra
  var sampleSel = document.getElementById('sampleMunSel');
  uniqueMuns.forEach(function(m){ var opt=document.createElement('option'); opt.value=m; opt.text=m; sampleSel.appendChild(opt); });
  if(uniqueMuns.length>0){ sampleSel.value = uniqueMuns[0]; }

  function drawTrend(mun){
    var rows = allData.filter(r => r.NOM_MUN === mun).sort((a,b)=>{
      const fa = parseFloat(String(a.PERIODO).replace('T','.'));
      const fb = parseFloat(String(b.PERIODO).replace('T','.'));
      return fa - fb;
    });
    var x = rows.map(r=>r.PERIODO);
    var yS = rows.map(r=>r.PCT_SEGUROS);
    var yI = rows.map(r=>r.PCT_INSEGUROS);

    var layout = {
      title: 'Evolución para ' + mun,
      xaxis: {title:'Período'},
      yaxis: {title:'Porcentaje'},
      showlegend:true
    };
    Plotly.relayout('trend', { 'layout.template': document.body.classList.contains('dark-mode') ? dark_template : light_template });
    var data = [
      {type:'scatter', mode:'lines+markers', name:'Segur@s',   x:x, y:yS, line:{color:'#EC407A'}},
      {type:'scatter', mode:'lines+markers', name:'Insegur@s', x:x, y:yI, line:{color:'#F8BBD0'}}
    ];
    Plotly.newPlot('trend', data, layout, {responsive:true});
  }

  function drawSampleTable(mun) {
      var table_div = document.getElementById('sample_table');
      var rows = allData.filter(r => r.NOM_MUN === mun);
      var table = '<table class="dataframe table-pink"><thead><tr><th>PERIODO</th><th>NOM_MUN</th><th>PCT_SEGUROS</th><th>PCT_INSEGUROS</th></tr></thead><tbody>';
      rows.slice(0, 15).forEach(function(r) {
          table += `<tr><td>${r.PERIODO}</td><td>${r.NOM_MUN}</td><td>${r.PCT_SEGUROS}</td><td>${r.PCT_INSEGUROS}</td></tr>`;
      });
      table += '</tbody></table>';
      table_div.innerHTML = table;
  }

  // Theme switcher
  const toggleSwitch = document.querySelector('.theme-switch input[type="checkbox"]');
  function switchTheme(e) {
      if (e.target.checked) {
          document.body.classList.add('dark-mode');
          localStorage.setItem('theme', 'dark');
          Plotly.relayout('trend', {template: dark_template});
          Plotly.relayout('anual', {template: dark_template});
      } else {
          document.body.classList.remove('dark-mode');
          localStorage.setItem('theme', 'light');
          Plotly.relayout('trend', {template: light_template});
          Plotly.relayout('anual', {template: light_template});
      }
  }
  toggleSwitch.addEventListener('change', switchTheme, false);

  // Check for saved theme
  const currentTheme = localStorage.getItem('theme');
  if (currentTheme) {
      document.body.classList.toggle('dark-mode', currentTheme === 'dark');
      if (currentTheme === 'dark') {
          toggleSwitch.checked = true;
          Plotly.relayout('trend', {template: dark_template});
          Plotly.relayout('anual', {template: dark_template});
      }
  }

  sel.addEventListener('change', function(){ drawTrend(this.value); });
  sampleSel.addEventListener('change', function(){ drawSampleTable(this.value); });

  // Dibujo inicial
  if(uniqueMuns.length>0){
      drawTrend(uniqueMuns[0]);
      drawSampleTable(uniqueMuns[0]);
  }

</script>
</body>
</html>
""")

        html_rendered = html_tpl.safe_substitute({
            'master_dataset_basename': master_dataset_basename,
            'fecha_generado': datetime.now().strftime('%Y-%m-%d %H:%M'),
            'prom_seg': f"{prom_seg:.2f}",
            'prom_inseg': f"{prom_inseg:.2f}",
            'periodo_max': periodo_max,
            'periodo_min': periodo_min,
            'top_html': top_html,
            'bottom_html': bottom_html,
            'period_html': period_html,
            'trend_summary_text': trend_summary_text,
            'mun_summary_html': mun_summary_html,
            'fig_trend_json': fig_trend_json,
            'fig_pie_json': fig_pie_json,
            'fig_anual_json': fig_anual_json,
            'data_for_js_json': data_for_js_json,
            'unique_muns_js': unique_muns_js,
            'conclusion_text': conclusion_text,
            'volatility_html': volatility_html,
        })

        with open(html_report_path, 'w', encoding='utf-8') as f:
            f.write(html_rendered)

        logging.info(f"HTML report generated at {html_report_path}")

    except Exception as e:
        logging.exception(f"Error during report generation: {e}")
        sys.exit(1)

# ==============================
# Main
# ==============================
if __name__ == '__main__':
    setup_logging()
    generate_report()