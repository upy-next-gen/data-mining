# 📊 Data Mining - Análisis de Percepción de Inseguridad Yucatán

![Python](https://img.shields.io/badge/python-v3.13-blue.svg) ![Pandas](https://img.shields.io/badge/pandas-v2.3.2-green.svg) ![Plotly](https://img.shields.io/badge/plotly-v6.3.0-orange.svg) ![Scikit-learn](https://img.shields.io/badge/scikit--learn-v1.7.2-red.svg) ![UV](https://img.shields.io/badge/uv-package%20manager-purple.svg)

## 🔍 Overview

Este proyecto implementa un pipeline completo de análisis de datos para estudiar patrones temporales de percepción de inseguridad en municipios de Yucatán, México. Utilizando datos de la Encuesta Nacional de Seguridad Urbana (ENSU) del INEGI (2015-2025), el sistema procesa, analiza y visualiza tendencias de seguridad urbana a través de modelos estadísticos y una aplicación web interactiva.

El proyecto aborda el desafío de convertir datos gubernamentales complejos y dispersos en insights accionables mediante técnicas de data science, machine learning y visualización avanzada. La solución final es una herramienta interactiva que permite a investigadores, policy makers y ciudadanos explorar patrones de inseguridad percibida de manera intuitiva y científicamente rigurosa.

## ✨ Key Achievements

- **📊 127 registros procesados** de múltiples datasets ENSU con normalización automática
- **🗺️ 4 municipios analizados** (Mérida, Kanasín, Progreso, Umán) con cobertura temporal 2016-2025
- **📈 Modelos predictivos** implementados con regresión lineal y métricas R² (0.032-0.321)
- **🌐 Aplicación web interactiva** con navegación por pestañas y gráficas Plotly dinámicas
- **🎯 Predicciones temporales** para 4 trimestres futuros (2025-2026)
- **📋 Documentación completa** con reportes técnicos detallados y código reproducible
- **🔧 Pipeline robusto** con manejo de errores, debugging avanzado y optimización de performance

## 🛠️ Tech Stack

### **Core Technologies**
- **🐍 Python 3.13** - Lenguaje principal para análisis y procesamiento
- **🐼 Pandas 2.3.2** - Manipulación y análisis de datos estructurados
- **📊 Plotly 6.3.0** - Visualizaciones interactivas web-ready
- **🤖 Scikit-learn 1.7.2** - Modelos de regresión lineal y métricas estadísticas
- **📦 UV** - Gestión moderna de dependencias y entornos virtuales

### **Architecture & Design**
- **🌐 HTML/CSS/JavaScript** - Frontend interactivo con navegación dinámica
- **📱 Responsive Design** - Interfaz adaptativa con branding UPY institucional
- **🔄 JSON Embedded** - Arquitectura de datos embebida para eliminación de dependencias
- **⚡ Performance Optimized** - Lazy loading y renderizado eficiente de gráficas

## 📈 Results & Impact

### **Statistical Analysis Results**
| Municipio | Registros | R² Score | Tendencia | Percepción Promedio | Predicción 2026 |
|-----------|-----------|----------|-----------|---------------------|-----------------|
| **Mérida** | 33 | 0.032 | ↘ Decreciente | 70.8% | 68.3% |
| **Kanasín** | 33 | 0.321 | ↗ Creciente | 78.1% | 81.2% |
| **Progreso** | 33 | 0.073 | ↘ Decreciente | 67.2% | 65.8% |
| **Umán** | 28 | 0.056 | ↗ Creciente | 75.4% | 76.1% |

### **Key Insights Discovered**
- **🎯 Kanasín** presenta la mayor correlación temporal (R²=0.321) con tendencia creciente preocupante
- **🌊 Progreso** mantiene los niveles más bajos de percepción de inseguridad (67.2%)
- **🏛️ Mérida** muestra tendencia decreciente notable a pesar de su alta percepción base
- **📊 Limitaciones de modelos lineales** evidencian complejidad multifactorial del fenómeno

## 🚀 Installation & Usage

### **Prerequisites**
- Python 3.11+ installed on your system
- UV package manager (recommended) or pip

### **Quick Start**
```bash
# Clone the repository
git clone https://github.com/upy-next-gen/data-mining.git
cd data-mining

# Install dependencies with UV (recommended)
uv install

# Or install with pip
pip install pandas plotly scikit-learn

# Generate interactive HTML report
uv run python generate_html_report_v2.py

# Open the generated report
open reporte_analisis_yucatan.html
```

### **Development Workflow**
```bash
# Process raw ENSU data (if needed)
uv run python process_yucatan_insecurity.py

# Unify processed datasets
uv run python unify_yucatan_data.py

# Generate final interactive report
uv run python generate_html_report_v2.py
```

## 📁 Project Architecture

```
data-mining/
├── 📊 processed data/           # Datasets procesados y unificados
│   ├── unified_yucatan_data.csv # Dataset principal (127 registros)
│   └── procesado_*.csv          # 33 archivos individuales procesados
├── 📋 reports/                  # Documentación técnica
│   ├── reporte_procesamiento.md # Reporte Fase 1: Data Processing
│   └── reporte_analisis.md      # Reporte Fase 2: Analysis & Visualization
├── 🌐 web/                      # Aplicación web interactiva
│   └── reporte_analisis_yucatan.html
├── 🐍 scripts/                  # Pipeline de procesamiento
│   ├── process_yucatan_insecurity.py    # Procesamiento inicial
│   ├── unify_yucatan_data.py            # Unificación de datos
│   └── generate_html_report_v2.py       # Generador reporte final
├── 📈 resources/                # Assets y visualizaciones
├── 🗂️ raw data/                 # Datos originales ENSU (80+ archivos)
└── ⚙️ pyproject.toml            # Configuración de dependencias
```

## 📊 Documentation & Reports

### **📋 Technical Reports**
- **[📄 Reporte de Procesamiento](reporte_procesamiento.md)** - Documentación completa del pipeline de data processing, challenges técnicos y soluciones implementadas
- **[📈 Reporte de Análisis](reporte_analisis.md)** - Análisis técnico del desarrollo de visualizaciones, modelos estadísticos y debugging del sistema interactivo

### **🌐 Interactive Deliverables**
- **[🔗 Reporte HTML Interactivo](reporte_analisis_yucatan.html)** - Aplicación web con gráficas Plotly dinámicas, navegación por pestañas y estadísticas en tiempo real

### **🐍 Core Scripts**
- **[⚙️ process_yucatan_insecurity.py](process_yucatan_insecurity.py)** - Pipeline principal de procesamiento de datos ENSU
- **[🔗 unify_yucatan_data.py](unify_yucatan_data.py)** - Sistema de unificación y consolidación de datasets
- **[🌐 generate_html_report_v2.py](generate_html_report_v2.py)** - Generador de aplicación web interactiva

## 🎓 Academic Context

**Institución:** Universidad Politécnica de Yucatán (UPY)  
**Materia:** Data Mining  
**Periodo:** Septiembre 2025  
**Estudiante:** This  

### **Competencias Técnicas Demostradas**
- **🔍 Data Science Pipeline:** Desde datos crudos hasta insights visuales accionables
- **📊 Statistical Modeling:** Implementación de regresión lineal con validación de métricas
- **🌐 Full-Stack Development:** Integración Python backend con frontend JavaScript interactivo
- **🔧 Problem Solving:** Debugging sistemático y optimización de performance
- **📋 Technical Documentation:** Reportes académicos y documentación de código profesional

### **Valor Profesional**
Este proyecto demuestra capacidades end-to-end en ciencia de datos aplicada a problemas de política pública, combinando rigor metodológico con implementación técnica avanzada. La metodología desarrollada es replicable y escalable para otros contextos de análisis de encuestas longitudinales y percepción ciudadana.

---

**📧 Contacto:** this@upy.edu.mx  
**🔗 Repositorio:** [upy-next-gen/data-mining](https://github.com/upy-next-gen/data-mining)  
**📅 Última actualización:** 14 de Septiembre de 2025
