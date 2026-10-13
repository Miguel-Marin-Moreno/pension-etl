"""
pipeline.py
Orquestador del pipeline ETL — Pensión Obligatoria AFP
------------------------------------------------------
Ejecuta las tres etapas en orden: Extract → Transform → Load

Uso:
    python pipeline.py

Prerequisitos:
    - Python 3.11+
    - pip install -r requirements.txt
    - Archivos CSV en data/raw/  (ver README sección 6)
"""

import sys
import time
from extract.extract import extract
from transform.transform import transform
from load.load import load


def run_pipeline():
    print("=" * 55)
    print("  PIPELINE ETL — PENSIÓN OBLIGATORIA AFP")
    print("=" * 55)
    start = time.time()

    # ── 1. EXTRACT ────────────────────────────────────────────
    print("\n[1/3] EXTRACCIÓN")
    try:
        raw = extract()
    except (FileNotFoundError, ValueError) as e:
        print(f"\n  ERROR en extracción:\n  {e}")
        sys.exit(1)

    # ── 2. TRANSFORM ──────────────────────────────────────────
    print("\n[2/3] TRANSFORMACIÓN")
    try:
        processed = transform(raw)
    except Exception as e:
        print(f"\n  ERROR en transformación:\n  {e}")
        sys.exit(1)

    # ── 3. LOAD ───────────────────────────────────────────────
    print("\n[3/3] CARGA")
    try:
        load(processed)
    except Exception as e:
        print(f"\n  ERROR en carga:\n  {e}")
        sys.exit(1)

    elapsed = time.time() - start
    print(f"\n{'=' * 55}")
    print(f"  Pipeline completado en {elapsed:.1f} segundos.")
    print(f"  Resultados en:  data/processed/")
    print("=" * 55)


if __name__ == "__main__":
    run_pipeline()
