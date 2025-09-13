# Assignment : A Methodological Compendium and Critical Review of the Yucatan Insecurity Analysis

## Abstract

This document provides a definitive methodological account of the Yucatan Insecurity Analysis project. The project's primary objective was to process, analyze, and visualize nearly a decade of public insecurity perception data (2015-2025) for the state of Yucatan, Mexico, sourced from the National Survey of Urban Public Safety (ENSU). The process involved a complex data curation and ETL (Extract, Transform, Load) pipeline to handle significant inconsistencies in historical data schemas and file naming conventions. The final output is a sophisticated, self-contained HTML analytical report featuring a dynamic narrative, a weighted statewide insecurity index, trend analysis, and a novel "Municipal Performance Index" for contextual benchmarking. This monograph details the entire project lifecycle, from initial data processing challenges and their resolutions to the analytical and visualization strategies employed. It concludes with a critical review of the methodology and data source, and provides recommendations for future work.

---

## 1.0 Introduction & Project Charter

### 1.1 Project Goal

The core objective of this project was to develop an automated pipeline to ingest, process, analyze, and visualize the evolution of public insecurity perception in Yucatan. The final deliverable is a high-quality, auto-generated analytical report that serves not as a mere data repository, but as a business intelligence tool, contextualizing trends and providing actionable insights at both the state and municipal levels.

### 1.2 Data Source

The foundational dataset for this analysis is the **National Survey of Urban Public Safety (ENSU)**, published by Mexico's National Institute of Statistics and Geography (INEGI). This survey provides quarterly data on the public's perception of security across various municipalities.

---

## 2.0 Data Curation and Processing

The transformation of raw, inconsistent source files into a single, coherent dataset was the most challenging phase of the project. The process, executed by the `procesar_csv.py` script, evolved significantly to overcome unforeseen data quality issues.

### 2.1 Initial ETL (Extract, Transform, Load) Strategy

The initial design of the ETL pipeline was based on the assumption of data consistency across all years. The plan involved five phases:
1.  **Discovery:** Scan the source directory (`dataru/`) for files matching a single pattern.
2.  **Validation:** Ensure each file contained the columns `NOM_ENT`, `NOM_MUN`, and `BP1_1`, and filter for `YUCATAN`.
3.  **Processing:** Group by municipality and calculate insecurity percentages.
4.  **Intermediate Generation:** Enrich each processed file with `AÑO` and `TRIMESTRE` metadata extracted from the filename and save it to an intermediate directory.
5.  **Consolidation:** Merge all intermediate files into `dataset_procesado_final.csv`.

### 2.2 Obstacles and Methodological Refinements

The initial strategy failed, revealing significant inconsistencies in the historical data. The pipeline was iteratively hardened to address these challenges.

*   **Obstacle 1: Legacy Data Schema (2015-2016)**
    *   **Problem:** Data from 2015-2016 was being completely ignored.
    *   **Root Cause:** These older files lacked `NOM_ENT` and `NOM_MUN` columns and used different geographic (`CD`) and question (`P1`) identifiers. The validation logic was correctly discarding them.
    *   **Resolution:** The processing script was made "bilingual." A schema detection mechanism was introduced. For legacy files, it now applies a hard-coded transformation, mapping `CD='8'` (for 2015) and `CD='52'` (for 2016) to Mérida, Yucatan, and renaming the relevant columns to match the modern schema.

*   **Obstacle 2: Evolving File Naming Conventions (2022+)**
    *   **Problem:** Data from mid-2022 onwards was not being discovered.
    *   **Root Cause:** The file naming convention changed (e.g., `...ensu_cb_0623.csv`), which did not match the script's static search pattern.
    *   **Resolution:** The file discovery function was updated to use multiple `glob` patterns, unifying the results to ensure all relevant files were ingested, regardless of their naming convention.

*   **Obstacle 3: Metadata Parsing Failure**
    *   **Problem:** The newly discovered files (2022+) were processed but assigned a default `AÑO`/`TRIMESTRE`.
    *   **Root Cause:** The regular expressions used for metadata extraction did not account for the new `...MMYY.csv` format.
    *   **Resolution:** An additional regex pattern was added to correctly parse month and year from the new filenames and calculate the corresponding quarter.

### 2.3 Final Data Schema

The resulting `dataset_procesado_final.csv` is a clean, unified dataset with the following key columns:
-   `NOM_ENT`: State Name
-   `NOM_MUN`: Municipality Name
-   `TOTAL_REGISTROS`: Total survey responses for the period/municipality.
-   `PCT_INSEGUROS`: Percentage of respondents feeling insecure.
-   `AÑO`: Year
-   `TRIMESTRE`: Quarter

---

## 3.0 Analytical Methodology

The analysis, performed by `generar_reporte_avanzado.py`, was designed to add layers of context to the raw data.

### 3.1 Statewide Metrics

To create a single, reliable statewide metric, a **weighted average** was calculated for each quarter, using `TOTAL_REGISTROS` as the weight. This gives more importance to municipalities with more survey respondents. To analyze this new time series, two trend metrics were calculated:
-   **4-Quarter Moving Average:** To smooth out seasonal noise and reveal the underlying medium-term trend.
-   **Linear Trend Line:** To show the overall, long-term direction of insecurity perception across the entire dataset.

### 3.2 Municipal Benchmarking

Two forms of municipal analysis were implemented:
1.  **Historical Ranking:** A simple average of `PCT_INSEGUROS` over all periods for each municipality was calculated to create a historical performance ranking.
2.  **Municipal Performance Index:** A more advanced KPI was developed to provide contextual benchmarking. For the latest quarter, the insecurity level of the highest and lowest municipalities is compared against the statewide average for that same quarter. This "deviation" metric reveals whether a municipality is performing significantly better or worse than the state as a whole.

---

## 4.0 Visualization and Reporting

The final deliverable is a self-contained HTML file, `report_analytical_insecurity_yucatan.html`, architected for clarity and aesthetic impact.

### 4.1 Report Architecture

-   **Technology:** The report is generated using Python with the `pandas` and `matplotlib` libraries. All charts are rendered as base64-encoded PNGs and embedded directly into the HTML, making the file fully portable.
-   **Design:** A professional, responsive dark theme was implemented via a dedicated `style.css` file. Key design choices include:
    -   **Typography:** Use of the 'Inter' font for clean, modern readability.
    -   **Layout:** A containerized, single-column layout with CSS Grid for responsive municipal chart displays.
    -   **Color Palette:** A defined color scheme is used for consistency. A dynamic palette is used for municipal charts to ensure visual differentiation.

### 4.2 Narrative Integration

A key feature of the report is the integration of qualitative analysis directly alongside the data. The report includes a distilled **Executive Summary**, a two-column **Chart Analysis** section, and a **Spotlight Analysis** to highlight the most significant data stories (Kanasin and Progreso), transforming it from a dashboard into a true analytical tool.

---

## 5.0 Critical Review and Methodological Considerations

A rigorous analysis requires a critique of its own tools and methods.

### 5.1 Critique of the Data Source (ENSU)

The ENSU survey, while valuable, has intrinsic limitations that must be considered during interpretation:
-   **Perception vs. Reality:** The survey measures the *perception* of insecurity, a subjective metric influenced by personal experience, media, and word-of-mouth. It is not a direct measure of actual crime rates.
-   **Sampling and Representativeness:** As a survey, it is subject to potential sampling biases. The representativeness of the sample across different demographics and urban/rural areas within a municipality can affect the results.
-   **Media Influence:** Public perception is highly susceptible to media coverage. A spike in news reports on crime, even if the underlying crime rate is stable, can cause a significant increase in perceived insecurity.

### 5.2 Analysis of Potential Processing Errors

A review of the project's own data processing pipeline reveals areas for consideration:
-   **Character Encoding:** The `AÑO` (with `Ñ`) vs. `ANO` issue was a significant technical challenge. The resolution was to programmatically normalize column names upon data loading (`df.columns.str.replace('Ñ', 'N')`), which proved to be a robust solution against `KeyError` exceptions.
-   **Handling of Missing Data:** The current strategy for municipalities that do not report in every quarter is an implicit fill-with-nulls, achieved by merging municipal data against a complete list of all periods. While this prevents data loss, alternative strategies like statistical interpolation could be considered for certain types of time-series analysis, though they would introduce their own set of assumptions.
-   **Weighting Logic:** The statewide average is weighted by `TOTAL_REGISTROS`. This is a sound approach, but a more complex model could consider demographic weights if such data were available, potentially yielding an even more accurate representation of the statewide perception.

---

## 6.0 Conclusion and Future Work

### 6.1 Summary of Findings

The project successfully created a robust data processing and reporting pipeline. The analysis revealed a significant long-term increase in perceived insecurity in Yucatan, with a particularly sharp spike in the most recent year. The "Spotlight Analysis" successfully identified Kanasin as a municipality with chronic, worsening security challenges and Progreso as a remarkable story of recent, dramatic improvement.

### 6.2 Recommendations for Further Analysis

The current report is a solid foundation, but several avenues for deeper analysis exist:
-   **Interactive Visualizations:** Migrating the charts from static `matplotlib` images to an interactive library like `Plotly` or `Bokeh` would allow users to hover for details, zoom, and toggle data series, significantly enhancing the user experience.
-   **Correlation with Crime Statistics:** A major step would be to acquire official crime statistics for the same period and perform a correlation analysis to investigate the relationship between perceived insecurity and actual crime rates.
-   **Time-Series Forecasting:** With nearly a decade of clean, quarterly data, time-series models (like ARIMA or Prophet) could be applied to forecast future trends in insecurity perception.
-   **Deeper Narrative Analysis:** The narrative generation could be expanded to automatically detect and comment on more complex patterns, such as changes in municipal rankings or periods of record-high or -low insecurity.
