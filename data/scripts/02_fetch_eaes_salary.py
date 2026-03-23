#!/usr/bin/env python3
"""
02_fetch_eaes_salary.py
Descarga datos salariales de la EAES (tabla 28186) del INE.
Genera tabla de salarios por código CNO-11 (2 dígitos).
"""

import argparse
import json
import sys
import time
from pathlib import Path

import pandas as pd
import requests

# Importar configuración
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import EAES_URL, RAW_DIR, DEFAULT_SALARIES

# Cargar mapeo CNO-11
CNO11_PATH = Path(__file__).resolve().parent / "cno11_mapping.json"
OUTPUT_FILE = RAW_DIR / "eaes_salaries.csv"

MAX_RETRIES = 3
RETRY_DELAY = 5
TIMEOUT = 30


def load_cno11_mapping() -> list[dict]:
    """Carga el mapeo CNO-11 desde el fichero JSON."""
    with open(CNO11_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["occupations"]


def fetch_eaes_data(force: bool = False) -> list:
    """Descarga datos salariales de la EAES del INE con reintentos."""
    if not force and OUTPUT_FILE.exists():
        print("  [INFO] Fichero de salarios EAES ya existe. Usa --force para volver a descargar.")
        return []

    print(f"  [INFO] Descargando datos EAES desde {EAES_URL} ...")
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(EAES_URL, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()
            print(f"  [OK] Descargadas {len(data)} series de la EAES.")
            return data
        except requests.exceptions.RequestException as e:
            print(f"  [ERROR] Intento {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                print(f"  [INFO] Reintentando en {RETRY_DELAY}s ...")
                time.sleep(RETRY_DELAY)
            else:
                print("  [ERROR] No se pudo descargar la EAES. Usando salarios por defecto.")
                return []
    return []


EAES_SALARY_MAP = {
    "Directores y gerentes": "1",
    "Técnicos y profesionales científicos e intelectuales de la salud y la enseñanza": "2a",
    "Otros técnicos y profesionales científicos e intelectuales": "2b",
    "Técnicos; profesionales de apoyo": "3",
    "Empleados de oficina que no atienden al público": "4a",
    "Empleados de oficina que atienden al público": "4b",
    "Trabajadores de los servicios de restauración y comercio": "5a",
    "Trabajadores de los servicios de salud y el cuidado de personas": "5b",
    "Trabajadores de los servicios de protección y seguridad": "5c",
    "Trabajadores cualificados en el sector agrícola, ganadero, forestal y pesquero": "6",
    "Trabajadores cualificados de la construcción, excepto operadores de máquinas": "7a",
    "Trabajadores cualificados de las industrias manufactureras, excepto operadores de instalaciones y máquinas": "7b",
    "Operadores de instalaciones y maquinaria fijas, y montadores": "8a",
    "Conductores y operadores de maquinaria móvil": "8b",
    "Trabajadores no cualificados en servicios (excepto transportes)": "9a",
    "Peones de la agricultura, pesca, construcción, industrias manufactureras y transportes": "9b",
    "Ocupaciones militares": "0",
}

# Mapeo de subgrupo EAES a códigos CNO-11 de 2 dígitos
EAES_TO_CNO2 = {
    "1":  ["11", "12", "13", "14", "15"],
    "2a": ["21", "22"],
    "2b": ["23", "24", "25", "26", "27", "28"],
    "3":  ["31", "32", "33", "34", "35", "36", "37", "38"],
    "4a": ["41", "43"],
    "4b": ["42", "44"],
    "5a": ["51", "52", "55"],
    "5b": ["53"],
    "5c": ["54"],
    "6":  ["61", "62", "63", "64"],
    "7a": ["71", "72"],
    "7b": ["73", "74", "75", "76", "77", "78"],
    "8a": ["81", "82"],
    "8b": ["83", "84"],
    "9a": ["91", "92", "93", "94"],
    "9b": ["95", "96", "97", "98"],
    "0":  [],
}


def parse_salary_series(raw_data: list) -> dict[str, float]:
    """
    Parsea las series salariales del INE.
    Formato real: '{ocupación}. Total. Salario medio bruto. Total Nacional. Dato base.'
    Devuelve dict: {código_cno2: salario_medio_anual}
    """
    # Primero, extraer salarios por grupo EAES
    eaes_salaries = {}

    for serie in raw_data:
        nombre = serie.get("Nombre", "")

        # Filtrar: queremos "Total" (ambos sexos) y "Salario medio bruto"
        if ". Total." not in nombre:
            continue
        if "Salario medio bruto" not in nombre:
            continue

        # Extraer nombre del grupo (primera parte antes de ". Total.")
        group_name = nombre.split(". Total.")[0].strip()

        # Buscar en nuestro mapeo
        eaes_key = EAES_SALARY_MAP.get(group_name)
        if eaes_key is None:
            continue

        # Obtener valor
        data_points = serie.get("Data", [])
        if not data_points:
            continue

        latest = max(data_points, key=lambda x: (x.get("Anyo", 0), x.get("FK_Periodo", 0)))
        valor = latest.get("Valor")
        if valor is not None and valor > 0:
            eaes_salaries[eaes_key] = round(valor, 2)
            print(f"    EAES '{group_name}' -> clave {eaes_key}: {valor:.2f} EUR/año")

    # Distribuir salarios a códigos CNO-11 de 2 dígitos
    salary_by_cno2 = {}
    for eaes_key, salary in eaes_salaries.items():
        cno2_codes = EAES_TO_CNO2.get(eaes_key, [])
        for code in cno2_codes:
            salary_by_cno2[code] = salary

    return salary_by_cno2


def build_salary_table(salary_map: dict[str, float], occupations: list[dict]) -> pd.DataFrame:
    """
    Construye tabla de salarios por código CNO-11 (2 dígitos).
    salary_map ya tiene {código_cno2: salario}.
    Usa DEFAULT_SALARIES como fallback por categoría (1 dígito).
    """
    rows = []
    for occ in occupations:
        code = occ["code"]
        title = occ["title"]
        category = occ["category"]

        # Buscar salario: primero por código 2-dígitos, luego default por categoría
        salary = salary_map.get(code)
        if salary is not None:
            source = "EAES"
        else:
            salary = DEFAULT_SALARIES.get(category, 0)
            source = "default"

        rows.append({
            "cno_code": code,
            "occupation_name": title,
            "category": category,
            "salary": round(salary),
            "source": source,
        })

    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="Descarga datos salariales EAES del INE")
    parser.add_argument("--force", action="store_true", help="Forzar nueva descarga aunque existan ficheros")
    args = parser.parse_args()

    print("=" * 60)
    print("02 - Descarga de datos salariales EAES (INE tabla 28186)")
    print("=" * 60)

    occupations = load_cno11_mapping()
    print(f"  [INFO] Cargadas {len(occupations)} ocupaciones CNO-11.")

    raw_data = fetch_eaes_data(force=args.force)

    if not raw_data:
        if not args.force and OUTPUT_FILE.exists():
            print("  [OK] Usando datos existentes en caché.")
            return

        # Usar salarios por defecto
        print("  [INFO] Usando salarios por defecto (DEFAULT_SALARIES).")
        salary_map = {}
    else:
        salary_map = parse_salary_series(raw_data)
        print(f"  [INFO] Obtenidos salarios para {len(salary_map)} grupos EAES.")

    df = build_salary_table(salary_map, occupations)

    # Guardar
    RAW_DIR.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"  [OK] Tabla de salarios guardada en {OUTPUT_FILE} ({len(df)} filas)")

    # Resumen
    eaes_count = len(df[df["source"] == "EAES"])
    default_count = len(df[df["source"] == "default"])
    print(f"  [INFO] Salarios EAES: {eaes_count}, Salarios por defecto: {default_count}")

    print("=" * 60)
    print("  Paso 02 completado.")
    print("=" * 60)


if __name__ == "__main__":
    main()
