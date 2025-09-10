
import os
import pandas as pd

def generar_reporte_html():
    """
    Lee todos los archivos CSV del directorio 'data/yucatan-inseguridad',
    los combina y genera un reporte HTML.
    """
    input_dir = os.path.join('data', 'yucatan-inseguridad')
    output_file = 'reporte_yucatan.html'

    # Verificar si el directorio de entrada existe
    if not os.path.isdir(input_dir):
        print(f"Error: El directorio '{input_dir}' no existe.")
        return

    # Encontrar todos los archivos CSV en el directorio
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    if not csv_files:
        print(f"No se encontraron archivos CSV en '{input_dir}'.")
        return

    # Leer y combinar todos los archivos CSV
    df_list = []
    for file in csv_files:
        file_path = os.path.join(input_dir, file)
        try:
            df = pd.read_csv(file_path)
            df_list.append(df)
        except Exception as e:
            print(f"Error al leer el archivo {file}: {e}")

    if not df_list:
        print("No se pudieron leer los datos de los archivos CSV.")
        return

    combined_df = pd.concat(df_list, ignore_index=True)

    # Generar el contenido HTML
    html_table = combined_df.to_html(classes='table table-striped', index=False)
    html_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Reporte de Inseguridad en Yucatán</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    </head>
    <body>
        <div class="container">
            <h1 class="mt-4">Reporte de Datos de Inseguridad en Yucatán</h1>
            <p>Este reporte combina los datos de los archivos CSV encontrados en la carpeta <code>data/yucatan-inseguridad</code>.</p>
            <div class="table-responsive">
                {html_table}
            </div>
        </div>
    </body>
    </html>
    """

    # Escribir el archivo HTML
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"Reporte generado exitosamente en '{output_file}'")
    except Exception as e:
        print(f"Error al escribir el archivo HTML: {e}")

if __name__ == '__main__':
    generar_reporte_html()
