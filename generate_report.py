
import os
import pandas as pd
import glob
from datetime import datetime

def generate_report():
    input_dir = 'data/yucatan-inseguridad'
    output_file = 'insecurity_report.html'

    csv_files = glob.glob(os.path.join(input_dir, 'procesado_*.csv'))
    
    if not csv_files:
        print(f"No processed CSV files found in {input_dir}")
        return

    df_list = []
    for file in csv_files:
        df_list.append(pd.read_csv(file))

    if not df_list:
        print("No data found in CSV files.")
        return

    full_df = pd.concat(df_list, ignore_index=True)
    full_df.sort_values(by=['AÑO', 'TRIMESTRE'], inplace=True)
    full_df.reset_index(drop=True, inplace=True)

    # --- Prepare data for HTML ---
    labels = [f"{row['AÑO']}-Q{row['TRIMESTRE']}" for index, row in full_df.iterrows()]
    insecurity_data = list(full_df['PCT_INSEGUROS'])

    table_header = "".join([f"<th>{col}</th>" for col in full_df.columns])
    table_rows = ""
    for index, row in full_df.iterrows():
        table_rows += "<tr>"
        for col in full_df.columns:
            # Format percentage columns to 2 decimal places
            if "PCT" in col:
                table_rows += f"<td>{row[col]:.2f}</td>"
            else:
                table_rows += f"<td>{row[col]}</td>"
        table_rows += "</tr>\n"

    summary_text = ""
    for index, row in full_df.iterrows():
        summary_text += f"<p>In the quarter {row['AÑO']}-Q{row['TRIMESTRE']}, the perception of insecurity in {row['NOM_ENT']} ({row.get('NOM_MUN', 'N/A')}) was {row['PCT_INSEGUROS']:.2f}%.</p>\n"

    # --- Generate HTML Parts ---

    css_style = '''
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; margin: 0; padding: 0; background: #f8f9fa; color: #212529; }
        .container { max-width: 1200px; margin: auto; padding: 1rem; }
        header { background: #343a40; color: #fff; padding: 1.5rem 0; text-align: center; border-bottom: 5px solid #007bff; }
        header h1 { margin: 0; font-size: 2.5rem; }
        header p { margin: 0; font-size: 1rem; color: #adb5bd; }
        section { background: #fff; margin: 1.5rem 0; padding: 1.5rem; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        h2 { color: #007bff; border-bottom: 2px solid #dee2e6; padding-bottom: 0.5rem; margin-top: 0; }
        table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
        th, td { padding: 12px 15px; text-align: left; border-bottom: 1px solid #dee2e6; }
        thead th { background-color: #e9ecef; font-weight: 600; }
        tbody tr:nth-of-type(even) { background-color: #f8f9fa; }
        tbody tr:hover { background-color: #f1f3f5; }
        canvas { max-width: 100%; }
        @media (max-width: 768px) {
            body { font-size: 14px; }
            .container { padding: 0.5rem; }
            h1 { font-size: 2rem; }
            th, td { padding: 8px 10px; }
        }
    </style>
    '''

    html_head = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Yucatan Security Perception Report</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        {css_style}
    </head>
    """

    html_body = f"""
    <body>
        <header>
            <h1>Yucatan Security Perception Report</h1>
            <p>Generated on: {datetime.now().strftime('%Y-%m-%d')}</p>
        </header>
        <div class="container">
            <section>
                <h2>Quarterly Summary</h2>
                {summary_text}
            </section>
            <section>
                <h2>Insecurity Perception Trend (%)</h2>
                <canvas id="insecurityChart"></canvas>
            </section>
            <section>
                <h2>Detailed Data</h2>
                <div style="overflow-x:auto;">
                    <table>
                        <thead>
                            <tr>
                                {table_header}
                            </tr>
                        </thead>
                        <tbody>
                            {table_rows}
                        </tbody>
                    </table>
                </div>
            </section>
            <section>
                <h2>Conclusion</h2>
                <p>
                    The data shows the evolution of the perception of insecurity in Yucatan across different quarters. 
                    The interactive chart above visualizes these changes, allowing for the identification of trends, peaks, and periods of improvement.
                    The table provides a detailed breakdown of the metrics for each period. Further analysis could correlate these trends with specific events or policies.
                </p>
            </section>
        </div>
    """

    js_script = f"""
    <script>
        const ctx = document.getElementById('insecurityChart').getContext('2d');
        const insecurityChart = new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: {labels},
                datasets: [{{ 
                    label: '% of Insecurity Perception',
                    data: {insecurity_data},
                    backgroundColor: 'rgba(220, 53, 69, 0.2)',
                    borderColor: 'rgba(220, 53, 69, 1)',
                    borderWidth: 2,
                    pointBackgroundColor: '#fff',
                    pointBorderColor: 'rgba(220, 53, 69, 1)',
                    tension: 0.1
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: true,
                scales: {{
                    y: {{
                        beginAtZero: true,
                        ticks: {{
                            callback: function(value) {{ return value + '%' }}
                        }}
                    }}
                }}
            }}
        }});
    </script>
    """

    html_foot = """
    </body>
    </html>
    """

    full_html = html_head + html_body + js_script + html_foot

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(full_html)
    
    print(f"Report generated successfully: {output_file}")

if __name__ == '__main__':
    generate_report()
