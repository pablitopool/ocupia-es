#!/usr/bin/env python3
"""
03_compute_outlook.py
Calcula la perspectiva de crecimiento (outlook) para cada ocupación
basándose en la variación interanual del empleo.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Importar configuración
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import RAW_DIR, OUTLOOK_THRESHOLDS

INPUT_FILE = RAW_DIR / "epa_employment.csv"
OUTPUT_FILE = RAW_DIR / "outlook.csv"


def classify_outlook(pct: float) -> str:
    """Clasifica el porcentaje de cambio según los umbrales configurados."""
    for desc, (lower, upper) in OUTLOOK_THRESHOLDS.items():
        if lower <= pct < upper:
            return desc
    return "Estable"


def compute_yoy_change(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula el cambio interanual para cada ocupación.
    Prefiere Q4 vs Q4, si no, usa el trimestre más reciente disponible.
    Aplica suavizado con media móvil de 4 trimestres.
    """
    results = []

    for occupation, group in df.groupby("occupation_name"):
        group = group.sort_values("period").copy()

        # Extraer año y trimestre
        group["year"] = group["period"].str[:4].astype(int)
        group["quarter"] = group["period"].str[-2:]

        # Intentar calcular media móvil de 4 trimestres para suavizar
        if len(group) >= 4:
            group["employed_smooth"] = group["employed"].rolling(window=4, min_periods=2).mean()
        else:
            group["employed_smooth"] = group["employed"]

        # Buscar los dos años más recientes
        years = sorted(group["year"].unique())
        if len(years) < 2:
            # No hay suficientes datos para calcular variación interanual
            results.append({
                "occupation_name": occupation,
                "outlook_pct": 0.0,
                "outlook_desc": "Estable",
            })
            continue

        latest_year = years[-1]
        prev_year = years[-2]

        # Preferir Q4 para la comparación interanual
        latest_q4 = group[(group["year"] == latest_year) & (group["quarter"] == "Q4")]
        prev_q4 = group[(group["year"] == prev_year) & (group["quarter"] == "Q4")]

        if not latest_q4.empty and not prev_q4.empty:
            latest_val = latest_q4.iloc[0]["employed_smooth"]
            prev_val = prev_q4.iloc[0]["employed_smooth"]
        else:
            # Si Q4 no está disponible, usar el trimestre más reciente común
            latest_data = group[group["year"] == latest_year].sort_values("quarter")
            prev_data = group[group["year"] == prev_year].sort_values("quarter")

            if latest_data.empty or prev_data.empty:
                results.append({
                    "occupation_name": occupation,
                    "outlook_pct": 0.0,
                    "outlook_desc": "Estable",
                })
                continue

            # Buscar trimestres comunes
            common_quarters = set(latest_data["quarter"]) & set(prev_data["quarter"])
            if common_quarters:
                # Usar el trimestre más tardío en común
                best_q = max(common_quarters)
                latest_val = latest_data[latest_data["quarter"] == best_q].iloc[0]["employed_smooth"]
                prev_val = prev_data[prev_data["quarter"] == best_q].iloc[0]["employed_smooth"]
            else:
                # Usar el último disponible de cada año
                latest_val = latest_data.iloc[-1]["employed_smooth"]
                prev_val = prev_data.iloc[-1]["employed_smooth"]

        # Calcular porcentaje de cambio
        if prev_val > 0:
            pct_change = ((latest_val - prev_val) / prev_val) * 100
        else:
            pct_change = 0.0

        pct_change = round(pct_change, 2)
        outlook_desc = classify_outlook(pct_change)

        results.append({
            "occupation_name": occupation,
            "outlook_pct": pct_change,
            "outlook_desc": outlook_desc,
        })

    return pd.DataFrame(results)


def main():
    parser = argparse.ArgumentParser(description="Calcula perspectiva de crecimiento del empleo")
    parser.add_argument("--force", action="store_true", help="Forzar recálculo aunque exista fichero")
    args = parser.parse_args()

    print("=" * 60)
    print("03 - Cálculo de perspectiva de crecimiento (outlook)")
    print("=" * 60)

    if not args.force and OUTPUT_FILE.exists():
        print("  [INFO] Fichero de outlook ya existe. Usa --force para recalcular.")
        print("  [OK] Usando datos existentes en caché.")
        return

    if not INPUT_FILE.exists():
        print(f"  [ERROR] No se encontró {INPUT_FILE}")
        print("  [INFO] Ejecuta primero: python3 01_fetch_epa_employment.py")
        raise SystemExit(1)

    print(f"  [INFO] Leyendo datos de empleo desde {INPUT_FILE} ...")
    df = pd.read_csv(INPUT_FILE)
    print(f"  [INFO] {len(df)} registros para {df['occupation_name'].nunique()} ocupaciones.")

    df_outlook = compute_yoy_change(df)

    if df_outlook.empty:
        print("  [ERROR] No se pudieron calcular perspectivas.")
        return

    # Ordenar por porcentaje de cambio (descendente)
    df_outlook = df_outlook.sort_values("outlook_pct", ascending=False).reset_index(drop=True)

    # Guardar
    RAW_DIR.mkdir(exist_ok=True)
    df_outlook.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"  [OK] Perspectivas guardadas en {OUTPUT_FILE} ({len(df_outlook)} filas)")

    # Resumen por categoría
    print("\n  Resumen de perspectivas:")
    for desc in OUTLOOK_THRESHOLDS.keys():
        count = len(df_outlook[df_outlook["outlook_desc"] == desc])
        if count > 0:
            print(f"    {desc}: {count} ocupaciones")

    print("=" * 60)
    print("  Paso 03 completado.")
    print("=" * 60)


if __name__ == "__main__":
    main()
