"""
extract.py
Etapa de Extracción del pipeline ETL — Pensión Obligatoria AFP
---------------------------------------------------------------
Lee los cuatro archivos CSV desde data/raw/ y valida que existan
y tengan las columnas mínimas esperadas.
"""

import os
import pandas as pd

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")

SOURCES = {
    "asignacion":  {
        "file": "emp_asig_mensual.csv",
        "required_cols": ["emp_hash64", "periodo_asignacion",
                          "id_posicion_lider_equipo", "id_posicion_ejecutivo",
                          "id_tipo_ejecutivo"],
    },
    "segmentacion": {
        "file": "emp_segmentacion.csv",
        "required_cols": ["emp_hash64", "desc_segmento_empleador_actual"],
    },
    "monitoreo": {
        "file": "afi_monitoreo_po.csv",
        "required_cols": ["emp_hash64", "posicion_venta", "salario_tope",
                          "afi_hash64", "periodo"],
    },
    "jerarquia": {
        "file": "jerarquia_comercial.csv",
        "required_cols": ["skEstructuraComercial", "descRegional", "idEjecutivo",
                          "descCanal", "descAgrupador", "nombreEjecutivo"],
    },
}


def extract() -> dict[str, pd.DataFrame]:
    """
    Lee cada fuente CSV y retorna un diccionario con DataFrames crudos.
    Lanza FileNotFoundError si algún archivo falta.
    Lanza ValueError si faltan columnas obligatorias.
    """
    dataframes = {}

    for name, meta in SOURCES.items():
        path = os.path.join(RAW_DIR, meta["file"])

        if not os.path.exists(path):
            raise FileNotFoundError(
                f"[EXTRACT] Archivo no encontrado: {path}\n"
                f"Coloque el CSV original en data/raw/{meta['file']}"
            )

        df = pd.read_csv(path, dtype=str)

        missing = [c for c in meta["required_cols"] if c not in df.columns]
        if missing:
            raise ValueError(
                f"[EXTRACT] Columnas faltantes en '{meta['file']}': {missing}"
            )

        dataframes[name] = df
        print(f"  [OK] {name:15s} — {len(df):>7,} filas  |  {path.split('raw/')[-1]}")

    return dataframes


if __name__ == "__main__":
    print("=== EXTRACCIÓN ===")
    dfs = extract()
    for k, v in dfs.items():
        print(f"  {k}: {v.shape}")
