
import os
import pandas as pd
import glob
from datetime import datetime
import matplotlib.pyplot as plt

def generate_markdown_report():
    input_dir = 'data/yucatan-inseguridad'
    output_md_file = 'insecurity_report.md'
    output_chart_file = 'insecurity_chart.png'

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

    # --- 1. Generate Chart Image ---
    labels = [f"{row['AÑO']}-Q{row['TRIMESTRE']}" for index, row in full_df.iterrows()]
    insecurity_data = list(full_df['PCT_INSEGUROS'])

    plt.figure(figsize=(12, 6))
    plt.plot(labels, insecurity_data, marker='o', linestyle='-', color='r')
    plt.title('Insecurity Perception Trend in Yucatan', fontsize=16)
    plt.xlabel('Quarter', fontsize=12)
    plt.ylabel('Perception of Insecurity (%)', fontsize=12)
    plt.xticks(rotation=45, ha="right")
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout() # Adjust layout to make room for rotated x-axis labels
    plt.savefig(output_chart_file)
    print(f"Chart saved to {output_chart_file}")

    # --- 2. Generate Markdown Content ---
    md_content = f"""
# Yucatan Security Perception Report

**Generated on:** {datetime.now().strftime('%Y-%m-%d')}

---

## Quarterly Summary

"""

    for index, row in full_df.iterrows():
        md_content += f"- **{row['AÑO']}-Q{row['TRIMESTRE']}**: The perception of insecurity in {row['NOM_ENT']} ({row.get('NOM_MUN', 'N/A')}) was **{row['PCT_INSEGUROS']:.2f}%**.\n"

    md_content += """

---

## Insecurity Perception Trend

![Insecurity Chart](insecurity_chart.png)

---

## Detailed Data

"""

    md_content += full_df.to_markdown(index=False)

    md_content += """

---

## Conclusion

The data and chart above illustrate the fluctuations in the public's perception of insecurity in Yucatan over several quarters. 
Key periods of high or low insecurity can be identified, providing a basis for more in-depth analysis. 
This report serves as a quantitative summary of these trends.

"""

    # --- 3. Write Markdown File ---
    with open(output_md_file, 'w', encoding='utf-8') as f:
        f.write(md_content)
    
    print(f"Markdown report generated successfully: {output_md_file}")

if __name__ == '__main__':
    generate_markdown_report()
