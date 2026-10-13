"""
load.py
Etapa de Carga del pipeline ETL — Pensión Obligatoria AFP
----------------------------------------------------------
Guarda los tres conjuntos de datos procesados en data/processed/
como archivos CSV listos para análisis o visualización.
"""

import os
import pandas as pd
from datetime import datetime

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")


def load(processed: dict[str, pd.DataFrame]) -> None:
    """
    Recibe el diccionario de DataFrames transformados y los guarda en CSV.
    Agrega una columna 'fecha_carga' con la marca de tiempo de ejecución.
    """
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    fecha_carga = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    OUTPUT_FILES = {
        "master_empresas":   "master_empresas.csv",
        "metricas_asesores": "metricas_asesores.csv",
        "dispersion":        "dispersion_afiliados.csv",
    }

    for key, filename in OUTPUT_FILES.items():
        if key not in processed:
            print(f"  [WARN] '{key}' no encontrado en processed; se omite.")
            continue

        df = processed[key].copy()
        df["fecha_carga"] = fecha_carga

        out_path = os.path.join(PROCESSED_DIR, filename)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")

        print(f"  [OK] {key:20s} -> {filename}  ({len(df):,} filas, {len(df.columns)} columnas)")

    print(f"\n  Archivos guardados en: {os.path.abspath(PROCESSED_DIR)}")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from extract.extract import extract
    from transform.transform import transform
    print("=== CARGA (prueba directa) ===")
    raw = extract()
    processed = transform(raw)
    load(processed)
