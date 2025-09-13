import os
import urllib.request

ASSETS = [
    ("https://code.jquery.com/jquery-3.7.1.min.js", "assets/vendor/jquery-3.7.1.min.js"),
    ("https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js", "assets/vendor/datatables.min.js"),
    ("https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css", "assets/vendor/datatables.min.css"),
    ("https://cdn.plot.ly/plotly-2.26.0.min.js", "assets/vendor/plotly-2.26.0.min.js"),
]


def fetch(url: str, dest: str) -> None:
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    print(f"Descargando {url} -> {dest}")
    urllib.request.urlretrieve(url, dest)


def main() -> None:
    for url, dest in ASSETS:
        try:
            fetch(url, dest)
        except Exception as e:
            print(f"[WARN] No se pudo descargar {url}: {e}")
    print("Listo. Actualiza el reporte para usar librerías locales.")


if __name__ == "__main__":
    main()

