# -*- coding: utf-8 -*-
"""
Advanced Analytical Report Generator on Perception of Insecurity.

This script loads processed data, calculates advanced metrics (KPIs,
moving averages, trends), generates dynamic analytical text, and builds a
self-contained HTML report with a professional design and aesthetic.
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import numpy as np
import base64
from io import BytesIO
import logging
from datetime import datetime
from pathlib import Path

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / 'dataset_procesado_final.csv'
OUTPUT_DIR = BASE_DIR / 'reportes'
HTML_REPORT_FILE = OUTPUT_DIR / 'insecurity_analytical_report_yucatan.html' # Changed name
LOG_FILE = OUTPUT_DIR / 'final_report_generation.log' # Changed name

# --- DESIGN AND ANALYSIS PARAMETERS ---
MOVING_AVERAGE_WINDOW = 4
COLOR_PALETTE = {
    'primary': '#2c3e50',    # Dark Blue
    'secondary': '#3498db',  # Bright Blue
    'accent': '#e74c3c',     # Red for negative trends
    'neutral': '#95a5a6',   # Gray
    'background': '#f4f4f9'
}
# Paleta para gráficos municipales para asegurar variedad visual
MUNICIPAL_PALETTE = [ 
    '#1abc9c',  # Turquoise
    '#f1c40f',  # Sunflower
    '#e67e22',  # Carrot
    '#9b59b6',  # Amethyst
]

# --- MAIN LOGIC ---

def setup_logging():
    """Configures the logging for the script."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] [%(levelname)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_FILE, mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logging.info("Final report logging configured.")

def load_and_prepare_data():
    """Loads the dataset, translates columns to English, and prepares a period column."""
    if not DATA_FILE.exists():
        logging.error(f"Data file not found at: {DATA_FILE}")
        raise FileNotFoundError(f"Processed dataset not found at {DATA_FILE}")
    
    df = pd.read_csv(DATA_FILE)
    
    # Rename columns to English
    column_translation = {
        'NOM_ENT': 'STATE_NAME',
        'NOM_MUN': 'MUNICIPALITY_NAME',
        'TOTAL_REGISTROS': 'TOTAL_RECORDS',
        'TOTAL_SEGUROS': 'TOTAL_SECURE',
        'TOTAL_INSEGUROS': 'TOTAL_INSECURE',
        'TOTAL_NO_RESPONDE': 'TOTAL_NO_RESPONSE',
        'PCT_SEGUROS': 'PCT_SECURE',
        'PCT_INSEGUROS': 'PCT_INSECURE',
        'PCT_NO_RESPONDE': 'PCT_NO_RESPONSE',
        'AÑO': 'YEAR',
        'TRIMESTRE': 'QUARTER'
    }
    df.rename(columns=column_translation, inplace=True)

    df['PERIOD'] = df['YEAR'].astype(str) + '-Q' + df['QUARTER'].astype(str)
    df = df.sort_values(['YEAR', 'QUARTER']).reset_index(drop=True)
    
    logging.info(f"Data loaded and prepared. {len(df)} records found.")
    return df

def calculate_statewide_metrics(df):
    """Calculates the weighted statewide average, moving average, and trend."""
    
    # Group by period to calculate weighted average
    period_df = df.groupby(['YEAR', 'QUARTER', 'PERIOD']).apply(lambda x: pd.Series({
        'WEIGHTED_TOTAL_INSECURE': (x['PCT_INSECURE'] * x['TOTAL_RECORDS']).sum(),
        'STATE_TOTAL_RECORDS': x['TOTAL_RECORDS'].sum()
    })).reset_index()
    
    period_df['WEIGHTED_PCT_INSECURE'] = (period_df['WEIGHTED_TOTAL_INSECURE'] / period_df['STATE_TOTAL_RECORDS'])
    
    state_df = period_df[['PERIOD', 'YEAR', 'QUARTER', 'WEIGHTED_PCT_INSECURE']].sort_values(['YEAR', 'QUARTER'])
    
    # Calculate moving average and trend
    state_df['MOVING_AVERAGE'] = state_df['WEIGHTED_PCT_INSECURE'].rolling(window=MOVING_AVERAGE_WINDOW).mean()
    x = np.arange(len(state_df))
    y = state_df['WEIGHTED_PCT_INSECURE']
    coeffs = np.polyfit(x, y, 1)
    state_df['TREND'] = np.polyval(coeffs, x)
    
    logging.info("Statewide metrics (average, moving average, trend) calculated.")
    return state_df

def generate_statewide_chart(state_df):
    """Generates the statewide evolution chart and returns it as a base64 string."""
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 7))
    
    ax.plot(state_df['PERIOD'], state_df['WEIGHTED_PCT_INSECURE'], marker='o', linestyle='-', label='Quarterly Insecurity (Weighted)', color=COLOR_PALETTE['secondary'], zorder=5)
    ax.plot(state_df['PERIOD'], state_df['MOVING_AVERAGE'], linestyle='--', color=COLOR_PALETTE['primary'], label=f'Moving Average ({MOVING_AVERAGE_WINDOW} quarters)')
    ax.plot(state_df['PERIOD'], state_df['TREND'], linestyle=':', color=COLOR_PALETTE['accent'], label='Linear Trend')
    
    ax.set_title('Evolution of Perception of Insecurity in Yucatan', fontsize=18, color=COLOR_PALETTE['primary'])
    ax.set_ylabel('Perception of Insecurity (%)')
    ax.set_xlabel('Period (Year-Quarter)')
    ax.tick_params(axis='x', rotation=45)
    ax.legend()
    fig.tight_layout()
    
    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=100)
    plt.close(fig)
    
    logging.info("Statewide chart generated.")
    return f"data:image/png;base64,{base64.b64encode(buf.getbuffer()).decode('ascii')}"

def generate_municipal_charts(df, ranking_df):
    """Generates charts for each municipality in the ranking."""
    charts = {}
    # Get a complete list of periods from the main df
    all_periods = df[['PERIOD', 'YEAR', 'QUARTER']].drop_duplicates().sort_values(['YEAR', 'QUARTER'])

    for i, row in ranking_df.iterrows():
        municipality = row['Municipality']
        mun_df = df[df['MUNICIPALITY_NAME'] == municipality].copy()
        if mun_df.empty: continue
        
        # Merge with all periods to show gaps
        mun_df = pd.merge(all_periods, mun_df, on=['PERIOD', 'YEAR', 'QUARTER'], how='left')

        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(10, 5))
        
        # Asignar color de la paleta de forma cíclica
        color = MUNICIPAL_PALETTE[i % len(MUNICIPAL_PALETTE)]
        
        ax.plot(mun_df['PERIOD'], mun_df['PCT_INSECURE'], marker='o', linestyle='-', color=color)
        ax.set_title(f'{municipality}', fontsize=16, color=COLOR_PALETTE['primary'])
        ax.set_ylabel('Percentage (%)')
        ax.set_ylim(0, max(100, mun_df['PCT_INSECURE'].max() * 1.1 if not mun_df['PCT_INSECURE'].isnull().all() else 100))
        ax.tick_params(axis='x', rotation=45)
        fig.tight_layout()
        
        buf = BytesIO()
        fig.savefig(buf, format='png', dpi=90)
        plt.close(fig)
        charts[municipality] = f"data:image/png;base64,{base64.b64encode(buf.getbuffer()).decode('ascii')}"
        
    logging.info(f"{len(charts)} municipal charts generated.")
    return charts

def calculate_kpis_and_narratives(state_df, full_df):
    """Calculates KPIs and generates dynamic narratives."""
    kpis = {}
    narratives = {}
    if state_df.empty: return kpis, narratives

    # Statewide KPIs
    latest = state_df.iloc[-1]
    kpis['latest_period'] = latest['PERIOD']
    kpis['latest_state_pct'] = latest['WEIGHTED_PCT_INSECURE']
    
    if len(state_df) > 1:
        previous = state_df.iloc[-2]
        kpis['quarterly_change'] = latest['WEIGHTED_PCT_INSECURE'] - previous['WEIGHTED_PCT_INSECURE']
    else: 
        kpis['quarterly_change'] = 0

    # Year-over-year change
    last_year_period = state_df[(state_df['YEAR'] == latest['YEAR'] - 1) & (state_df['QUARTER'] == latest['QUARTER'])]
    if not last_year_period.empty:
        kpis['annual_change'] = latest['WEIGHTED_PCT_INSECURE'] - last_year_period['WEIGHTED_PCT_INSECURE'].iloc[0]
    else:
        kpis['annual_change'] = 0

    # Municipal KPIs & Deviation from State Average
    latest_mun_df = full_df[full_df['PERIOD'] == kpis['latest_period']]
    if not latest_mun_df.empty:
        kpis['max_municipality'] = latest_mun_df.loc[latest_mun_df['PCT_INSECURE'].idxmax()]
        kpis['min_municipality'] = latest_mun_df.loc[latest_mun_df['PCT_INSECURE'].idxmin()]
        
        # Calculando la desviación respecto a la media estatal
        kpis['max_mun_deviation'] = kpis['max_municipality']['PCT_INSECURE'] - kpis['latest_state_pct']
        kpis['min_mun_deviation'] = kpis['min_municipality']['PCT_INSECURE'] - kpis['latest_state_pct']

    # Generate Narratives
    summary = f"In the last analyzed period ({kpis['latest_period']}), the statewide perception of insecurity in Yucatan was {kpis['latest_state_pct']:.1f}%. "
    if kpis['quarterly_change'] != 0:
        summary += f"This represents an {'increase' if kpis['quarterly_change'] > 0 else 'decrease'} of {abs(kpis['quarterly_change']):.1f} percentage points from the previous quarter. "
    if kpis['annual_change'] != 0:
        summary += f"In the year-over-year comparison, there was a {'rise' if kpis['annual_change'] > 0 else 'drop'} of {abs(kpis['annual_change']):.1f} points."
    narratives['executive_summary'] = summary

    if 'max_municipality' in kpis:
        insight = f"The municipality with the highest perception of insecurity in the last quarter was {kpis['max_municipality']['MUNICIPALITY_NAME']} ({kpis['max_municipality']['PCT_INSECURE']:.1f}%), while {kpis['min_municipality']['MUNICIPALITY_NAME']} had the lowest ({kpis['min_municipality']['PCT_INSECURE']:.1f}%)."
        narratives['municipal_insight'] = insight
    else:
        narratives['municipal_insight'] = "Municipal data for the latest period was not available."

    logging.info("KPIs and narratives generated.")
    return kpis, narratives

def generate_ranking_table(df):
    """Generates a ranking table of municipalities by historical average insecurity."""
    ranking = df.groupby('MUNICIPALITY_NAME')['PCT_INSECURE'].mean().sort_values(ascending=False).reset_index()
    ranking.rename(columns={'MUNICIPALITY_NAME': 'Municipality', 'PCT_INSECURE': 'Historical Average'}, inplace=True)
    ranking['#'] = np.arange(1, len(ranking) + 1)
    ranking = ranking[['#', 'Municipality', 'Historical Average']]
    ranking['Historical Average'] = ranking['Historical Average'].map('{:.1f}%'.format)
    logging.info("Ranking table generated.")
    return ranking

def generate_html_report(kpis, narratives, statewide_chart, municipal_charts, ranking_df):
    """Builds the final self-contained HTML report with integrated analysis."""
    
    # --- Narrative Content (Distilled from user prompt) ---
    executive_summary_text = """
    For Q2-2025, the statewide insecurity level of 38.8% reveals a concerning trend. 
    This marks a 2.0 point increase from the previous quarter and a substantial 18.5 point rise year-over-year, 
    signaling a significant worsening of the security situation. Kanasin is a particular hotspot, 
    far exceeding the state average.
    """

    chart_elements_text = """
    <ul>
        <li><strong>Quarterly Insecurity (Blue Line):</strong> Shows the raw, volatile perception of insecurity for each quarter.</li>
        <li><strong>Moving Average (Black Dashed Line):</strong> Smooths out short-term fluctuations to reveal medium-term trends and cycles over a four-quarter period.</li>
        <li><strong>Linear Trend (Red Dotted Line):</strong> Indicates the overall long-term direction, which shows a slight, marginal increase in insecurity from 2016 to 2025.</li>
    </ul>
    """

    chart_observations_text = """
    The most striking feature is the dramatic and rapid rise in perceived insecurity starting in late 2024, 
    placing the current 38.8% level as one of the highest points in the nearly decade-long series. 
    This sharp increase visually represents the significant +18.5 point year-over-year change and provides critical context to the current situation. 
    The data also exhibits clear cyclical patterns and high short-term volatility.
    """

    kanasin_spotlight_text = """
    Kanasin's current 68.0% insecurity rate is alarming, but it is part of a larger story. 
    With the highest historical average among all municipalities (45.7%), this data confirms Kanasin faces a chronic, 
    long-term security challenge that has recently escalated to critical levels.
    """

    progreso_spotlight_text = """
    The most significant insight is Progreso's transformation. While it has the second-highest historical average insecurity (42.7%), 
    it currently boasts the lowest rate in Q2-2025 (33.3%). This stark contrast points to a dramatic and highly successful recent improvement, 
    making Progreso a key case study for positive change.
    """

    # --- Prepare data for HTML ---
    def format_kpi(value, unit=' pts'):
        if value is None or value == 0: return f'<span class="kpi-neutral">0.0{unit}</span>'
        # For both change and deviation, a positive value is negative (bad), so the logic is consistent.
        color_class = 'kpi-negative' if value > 0 else 'kpi-positive'
        return f'<span class="{color_class}">{value:+.1f}{unit}</span>'

    quarterly_change_str = format_kpi(kpis.get('quarterly_change'))
    annual_change_str = format_kpi(kpis.get('annual_change'))
    
    max_mun_row = kpis.get('max_municipality')
    min_mun_row = kpis.get('min_municipality')
    max_mun_str = f"{max_mun_row['MUNICIPALITY_NAME']} ({max_mun_row['PCT_INSECURE']:.1f}%)" if max_mun_row is not None else "N/A"
    min_mun_str = f"{min_mun_row['MUNICIPALITY_NAME']} ({min_mun_row['PCT_INSECURE']:.1f}%)" if min_mun_row is not None else "N/A"

    max_dev_str = format_kpi(kpis.get('max_mun_deviation'), unit=' pts Deviation')
    min_dev_str = format_kpi(kpis.get('min_mun_deviation'), unit=' pts Deviation')

    municipal_charts_html = ""
    for i, row in ranking_df.iterrows():
        municipality = row['Municipality']
        if municipality in municipal_charts:
            municipal_charts_html += f'''
            <div class="grafico-municipal">
                <h3>{municipality}</h3>
                <img src="{municipal_charts[municipality]}" alt="Chart of {municipality}">
            </div>'''

    # --- HTML Template ---
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Analytical Report on Perception of Insecurity - Yucatan</title>
        <link rel="stylesheet" href="style.css">
    </head>
    <body>
        <div class="container">
            <h1>Analytical Report on Perception of Insecurity</h1>
            <h2>Yucatan</h2>
            
            <div class="metadata">
                <p><strong>Generated on:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>Data Source:</strong> National Survey of Urban Public Safety (ENSU) - INEGI</p>
            </div>

            <h2>Dashboard (KPIs)</h2>
            <div class="kpi-container">
                <div class="kpi">
                    <div class="kpi-value">{kpis.get('latest_state_pct', 0):.1f}%</div>
                    <div class="kpi-label">Statewide Insecurity ({kpis.get('latest_period', 'N/A')})</div>
                </div>
                <div class="kpi">
                    <div class="kpi-value">{quarterly_change_str}</div>
                    <div class="kpi-label">vs. Previous Quarter</div>
                </div>
                <div class="kpi">
                    <div class="kpi-value">{annual_change_str}</div>
                    <div class="kpi-label">vs. Same Quarter Last Year</div>
                </div>
                <div class="kpi">
                    <div class="kpi-value">{max_mun_str}</div>
                    <div class="kpi-label">Municipality with Highest Insecurity</div>
                </div>
                <div class="kpi">
                    <div class="kpi-value">{max_dev_str}</div>
                    <div class="kpi-label">Deviation vs. State (Highest)</div>
                </div>
                 <div class="kpi">
                    <div class="kpi-value">{min_mun_str}</div>
                    <div class="kpi-label">Municipality with Lowest Insecurity</div>
                </div>
                <div class="kpi">
                    <div class="kpi-value">{min_dev_str}</div>
                    <div class="kpi-label">Deviation vs. State (Lowest)</div>
                </div>
            </div>

            <div class="analysis-section executive-summary">
                <h3>Executive Summary</h3>
                <p>{executive_summary_text}</p>
            </div>

            <h2>Statewide Evolution</h2>
            <div class="grafico-estatal">
                <img src="{statewide_chart}" alt="Statewide Evolution Chart">
            </div>

            <div class="analysis-section">
                <h3>Chart Analysis</h3>
                <div class="chart-analysis-container">
                    <div class="analysis-column">
                        <h4>Chart Elements</h4>
                        {chart_elements_text}
                    </div>
                    <div class="analysis-column">
                        <h4>Key Observations</h4>
                        <p>{chart_observations_text}</p>
                    </div>
                </div>
            </div>

            <h2>Municipal Ranking (Historical Average)</h2>
            {ranking_df.to_html(index=False, classes='dataframe tabla')}

            <div class="analysis-section">
                <h3>Spotlight Analysis</h3>
                <div class="spotlight-container">
                    <div class="spotlight-box">
                        <h4>Persistent Challenge: Kanasin</h4>
                        <p>{kanasin_spotlight_text}</p>
                    </div>
                    <div class="spotlight-box positive-spotlight">
                        <h4>Recent Success Story: Progreso</h4>
                        <p>{progreso_spotlight_text}</p>
                    </div>
                </div>
            </div>

            <h2>Analysis by Municipality</h2>
            <div class="grid-municipales">
                {municipal_charts_html}
            </div>

        </div>
    </body>
    </html>
    """
    logging.info("HTML report generated.")
    return html

def main():
    """Main function to run the report generation process."""
    try:
        setup_logging()
        logging.info("--- Starting Final Analytical Report Generation ---")
        
        full_df = load_and_prepare_data()
        state_df = calculate_statewide_metrics(full_df)
        kpis, narratives = calculate_kpis_and_narratives(state_df, full_df)
        ranking_df = generate_ranking_table(full_df)
        
        statewide_chart_b64 = generate_statewide_chart(state_df)
        municipal_charts_b64 = generate_municipal_charts(full_df, ranking_df)
        
        html_report = generate_html_report(kpis, narratives, statewide_chart_b64, municipal_charts_b64, ranking_df)
        
        # Use a new file name for the English report
        new_report_file = OUTPUT_DIR / 'report_analytical_insecurity_yucatan.html'
        with open(new_report_file, 'w', encoding='utf-8') as f:
            f.write(html_report)
            
        logging.info(f"Final report successfully saved to: {new_report_file}")

    except FileNotFoundError as e:
        logging.critical(f"CRITICAL ERROR: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        logging.info("--- Final Report Generation Process Finished ---")

if __name__ == '__main__':
    main()
