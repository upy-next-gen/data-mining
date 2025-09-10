
import pandas as pd
import glob
import os
import logging
import plotly.graph_objects as go

def generate_report():
    """Generates an HTML report with analysis of the processed data."""
    # --- 1. Setup Logging ---
    log_file = 'log_analisis_yucatan.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w'),
            logging.StreamHandler()
        ]
    )
    logging.info("Report generation process started.")

    # --- 2. Load and Concatenate Data ---
    source_dir = os.path.join('Data', 'yucatan-inseguridad')
    processed_files = glob.glob(os.path.join(source_dir, 'procesado_*.csv'))
    if not processed_files:
        logging.error("No processed files found. Aborting report generation.")
        return

    df = pd.concat((pd.read_csv(f) for f in processed_files), ignore_index=True)
    logging.info(f"Loaded and concatenated {len(processed_files)} processed files.")

    # --- 3. Data Preparation ---
    # Create a proper date for plotting
    df['TRIMESTRE'] = df['TRIMESTRE'].astype(str)
    df['DATE'] = pd.to_datetime(df['AÑO'].astype(str) + '-' + (df['TRIMESTRE'].astype(int) * 3).astype(str) + '-01')
    df = df.sort_values('DATE')
    logging.info("Data prepared for time series analysis.")

    # --- 4. State-Level Analysis ---
    state_level = df.groupby('DATE').apply(
        lambda x: (x['PCT_INSEGUROS'] * x['TOTAL_REGISTROS']).sum() / x['TOTAL_REGISTROS'].sum()
    ).reset_index(name='PCT_INSEGUROS_ESTATAL')
    logging.info("Calculated state-level weighted average for insecurity perception.")

    # --- 5. Visualization ---
    # State-level plot
    fig_state = go.Figure()
    fig_state.add_trace(go.Scatter(x=state_level['DATE'], y=state_level['PCT_INSEGUROS_ESTATAL'],
                                 mode='lines+markers', name='State Average'))
    fig_state.update_layout(title='Evolution of Insecurity Perception in Yucatán (State Level)',
                          xaxis_title='Date', yaxis_title='Perception of Insecurity (%)')
    logging.info("Generated state-level plot.")

    # Municipal-level plot
    fig_municipal = go.Figure()
    for mun in df['NOM_MUN'].unique():
        df_mun = df[df['NOM_MUN'] == mun]
        fig_municipal.add_trace(go.Scatter(x=df_mun['DATE'], y=df_mun['PCT_INSEGUROS'],
                                         mode='lines+markers', name=mun))
    fig_municipal.update_layout(title='Evolution of Insecurity Perception in Yucatán (Municipal Level)',
                              xaxis_title='Date', yaxis_title='Perception of Insecurity (%)')
    logging.info("Generated municipal-level plot.")

    # --- 6. HTML Report Generation ---
    report_path = 'reporte_yucatan_evolucion.html'
    logging.info(f"Generating HTML report at {report_path}...")

    # Prepare data table for HTML
    summary_table = df.to_html(index=False, classes='table table-striped')

    html_content = f"""
    <html>
        <head>
            <title>Insecurity Perception Report - Yucatán</title>
            <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
        </head>
        <body>
            <div class="container">
                <h1 class="mt-5">Insecurity Perception Evolution in Yucatán</h1>
                
                <h2 class="mt-5">State Level Analysis</h2>
                <p>This chart shows the weighted average of the perception of insecurity for the entire state of Yucatán over time.</p>
                {fig_state.to_html(full_html=False, include_plotlyjs='cdn')}

                <h2 class="mt-5">Municipal Level Analysis</h2>
                <p>This chart shows the evolution of the perception of insecurity for each municipality available in the datasets. Note that municipalities with numeric codes have not been mapped to names yet.</p>
                {fig_municipal.to_html(full_html=False, include_plotlyjs='cdn')}

                <h2 class="mt-5">Methodological Notes & Error Analysis</h2>
                <h3>Processing Errors Found</h3>
                <ul>
                    <li><b>Duplicated Quarters:</b> This was an initial error where multiple source files belonging to the same quarter would overwrite each other. The processing script has been refactored to group all files for a given quarter before processing, resolving this issue.</li>
                    <li><b>Missing Data:</b> Gaps in the charts are present where no data was available for Yucatán for a specific quarter.</li>
                    <li><b>Unmapped Municipalities:</b> For older datasets, municipality names are represented by numeric codes. A catalog would be needed to map these to their proper names.</li>
                </ul>
                <h3>Brainstorming on Survey Methodological Errors</h3>
                <ul>
                    <li><b>Sampling Bias:</b> The survey might not reach all socio-economic segments equally, potentially skewing the perception data.</li>
                    <li><b>Question Wording:</b> The way questions are phrased can influence responses. "Perception of insecurity" can be subjective and interpreted differently by individuals.</li>
                    <li><b>Response Bias:</b> Respondents might be hesitant to express their true feelings about insecurity for fear of repercussions, leading to underreporting.</li>
                    <li><b>Temporal Effects:</b> A recent, highly publicized crime event could temporarily spike the perception of insecurity, not reflecting the long-term trend.</li>
                </ul>

                <h2 class="mt-5">Complete Data Table</h2>
                {summary_table}
            </div>
        </body>
    </html>
    """

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logging.info(f"HTML report successfully generated.")

if __name__ == "__main__":
    generate_report()
