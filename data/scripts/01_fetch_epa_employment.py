#!/usr/bin/env python3
"""
01_fetch_epa_employment.py
Descarga datos de empleo de la EPA (tabla 65967) del INE.
Genera series temporales y snapshot del último periodo.
"""

import argparse
import sys
import time
from pathlib import Path

import pandas as pd
import requests

# Importar configuración
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import EPA_URL, EPA_MULTIPLIER, RAW_DIR

# Mapeo de FK_Periodo a trimestre
PERIODO_TO_QUARTER = {
    355000: "Q1",
    355004: "Q2",
    355008: "Q3",
    355012: "Q4",
}

OUTPUT_TIMESERIES = RAW_DIR / "epa_employment.csv"
OUTPUT_LATEST = RAW_DIR / "epa_latest.csv"

MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos
TIMEOUT = 30  # segundos


def fetch_epa_data(force: bool = False) -> list:
    """Descarga datos de la EPA del INE con reintentos."""
    if not force and OUTPUT_TIMESERIES.exists() and OUTPUT_LATEST.exists():
        print("  [INFO] Ficheros de empleo EPA ya existen. Usa --force para volver a descargar.")
        return []

    print(f"  [INFO] Descargando datos EPA desde {EPA_URL} ...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(EPA_URL, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()
            print(f"  [OK] Descargados {len(data)} series del INE.")
            return data
        except requests.exceptions.RequestException as e:
            print(f"  [ERROR] Intento {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                print(f"  [INFO] Reintentando en {RETRY_DELAY}s ...")
                time.sleep(RETRY_DELAY)
            else:
                print("  [ERROR] No se pudo descargar la EPA tras varios intentos.")
                raise SystemExit(1)
    return []


def parse_quarter(anyo: int, fk_periodo: int) -> str:
    """Convierte año y FK_Periodo al formato YYYY-QX."""
    quarter = PERIODO_TO_QUARTER.get(fk_periodo)
    if quarter is None:
        # Enfoque modular: los códigos conocidos son 355000,355004,355008,355012
        # Intentar deducir del offset respecto a 355000
        offset = (fk_periodo - 355000) % 16
        quarter_map = {0: "Q1", 4: "Q2", 8: "Q3", 12: "Q4"}
        quarter = quarter_map.get(offset, f"P{fk_periodo}")
    return f"{anyo}-{quarter}"


def parse_occupation_name(nombre: str) -> str | None:
    """
    Extrae el nombre de la ocupación del campo Nombre.
    Formato esperado: '... Ambos sexos. {ocupación}. Personas.'
    """
    # Filtrar: solo registros con "Ambos sexos" y "Personas" (no "Porcentaje")
    if "Ambos sexos" not in nombre:
        return None
    if "Personas" not in nombre:
        return None
    if "Porcentaje" in nombre:
        return None

    # Extraer la parte entre "Ambos sexos. " y ". Personas."
    try:
        after_sexos = nombre.split("Ambos sexos. ")[1]
        occupation = after_sexos.split(". Personas")[0]
        return occupation.strip()
    except (IndexError, ValueError):
        return None


def process_series(raw_data: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Procesa las series del INE y genera DataFrames."""
    rows = []

    for serie in raw_data:
        nombre = serie.get("Nombre", "")
        occupation = parse_occupation_name(nombre)
        if occupation is None:
            continue

        data_points = serie.get("Data", [])
        for point in data_points:
            anyo = point.get("Anyo")
            fk_periodo = point.get("FK_Periodo")
            valor = point.get("Valor")

            if anyo is None or fk_periodo is None or valor is None:
                continue

            period = parse_quarter(anyo, fk_periodo)
            employed = round(valor * EPA_MULTIPLIER)

            rows.append({
                "occupation_name": occupation,
                "period": period,
                "employed": employed,
            })

    if not rows:
        print("  [AVISO] No se encontraron datos de empleo válidos.")
        return pd.DataFrame(), pd.DataFrame()

    df = pd.DataFrame(rows)
    print(f"  [INFO] {len(df)} registros de empleo procesados para {df['occupation_name'].nunique()} ocupaciones.")

    # Ordenar por ocupación y periodo
    df = df.sort_values(["occupation_name", "period"]).reset_index(drop=True)

    # Snapshot del último periodo para cada ocupación
    # Determinar el último periodo global
    latest_period = df["period"].max()
    print(f"  [INFO] Último periodo disponible: {latest_period}")

    df_latest = (
        df.sort_values("period")
        .groupby("occupation_name")
        .last()
        .reset_index()[["occupation_name", "period", "employed"]]
    )

    return df, df_latest


def main():
    parser = argparse.ArgumentParser(description="Descarga datos de empleo EPA del INE")
    parser.add_argument("--force", action="store_true", help="Forzar nueva descarga aunque existan ficheros")
    args = parser.parse_args()

    print("=" * 60)
    print("01 - Descarga de datos de empleo EPA (INE tabla 65967)")
    print("=" * 60)

    raw_data = fetch_epa_data(force=args.force)
    if not raw_data:
        if not args.force and OUTPUT_TIMESERIES.exists():
            print("  [OK] Usando datos existentes en caché.")
            return
        print("  [ERROR] No hay datos para procesar.")
        return

    df_timeseries, df_latest = process_series(raw_data)

    if df_timeseries.empty:
        print("  [ERROR] No se generaron datos de empleo.")
        return

    # Guardar ficheros
    RAW_DIR.mkdir(exist_ok=True)
    df_timeseries.to_csv(OUTPUT_TIMESERIES, index=False, encoding="utf-8")
    print(f"  [OK] Serie temporal guardada en {OUTPUT_TIMESERIES} ({len(df_timeseries)} filas)")

    df_latest.to_csv(OUTPUT_LATEST, index=False, encoding="utf-8")
    print(f"  [OK] Snapshot último periodo guardado en {OUTPUT_LATEST} ({len(df_latest)} filas)")

    print("=" * 60)
    print("  Paso 01 completado.")
    print("=" * 60)


if __name__ == "__main__":
    main()
