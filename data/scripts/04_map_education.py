#!/usr/bin/env python3
"""
04_map_education.py
Asigna nivel educativo a cada ocupación CNO-11 (2 dígitos)
basándose en los requisitos típicos de formación.
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

# Importar configuración
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import RAW_DIR, EDUCATION_LEVELS

CNO11_PATH = Path(__file__).resolve().parent / "cno11_mapping.json"
OUTPUT_FILE = RAW_DIR / "education.csv"

# Mapeo estático: código CNO-11 (2 dígitos) -> nivel educativo (1-8)
# Basado en las definiciones de competencia de la CNO-11 y los requisitos
# típicos de formación para cada grupo ocupacional en España.
EDUCATION_MAP = {
    # 1x - Directores y gerentes: nivel 6-7 (Grado/Máster)
    "11": 7,  # Directores ejecutivos, altos cargos -> Máster
    "12": 7,  # Directores de departamentos administrativos -> Máster
    "13": 6,  # Directores de producción y operaciones -> Grado
    "14": 6,  # Directores de alojamiento, restauración y comercio -> Grado
    "15": 6,  # Directores de otras empresas de servicios -> Grado

    # 2x - Profesionales científicos e intelectuales: nivel 6-8
    "21": 7,  # Profesionales de la salud -> Máster (medicina, enfermería avanzada)
    "22": 6,  # Profesionales de la enseñanza -> Grado
    "23": 7,  # Ciencias físicas, químicas, matemáticas, ingenierías -> Máster
    "24": 6,  # Profesionales TI -> Grado
    "25": 7,  # Profesionales del derecho -> Máster
    "26": 6,  # Especialistas en organización y comercialización -> Grado
    "27": 7,  # Profesionales de ciencias sociales -> Máster
    "28": 6,  # Profesionales de cultura y espectáculo -> Grado

    # 3x - Técnicos y profesionales de apoyo: nivel 5-6
    "31": 5,  # Técnicos de ciencias e ingenierías -> FP Superior
    "32": 5,  # Supervisores en ingeniería de minas, manufactura -> FP Superior
    "33": 5,  # Técnicos sanitarios -> FP Superior
    "34": 6,  # Profesionales de apoyo en finanzas -> Grado
    "35": 5,  # Representantes y agentes comerciales -> FP Superior
    "36": 5,  # Profesionales de apoyo a gestión administrativa -> FP Superior
    "37": 5,  # Técnicos TIC -> FP Superior
    "38": 5,  # Profesionales de apoyo servicios jurídicos, sociales -> FP Superior

    # 4x - Empleados contables y administrativos: nivel 4-5
    "41": 4,  # Empleados en servicios contables y financieros -> Bachillerato/FP Medio
    "42": 4,  # Empleados de bibliotecas, correos -> Bachillerato/FP Medio
    "43": 4,  # Otros empleados administrativos -> Bachillerato/FP Medio
    "44": 4,  # Empleados de agencias de viajes, recepcionistas -> Bachillerato/FP Medio

    # 5x - Trabajadores de servicios y comercio: nivel 2-4
    "51": 3,  # Trabajadores de restauración -> FP Básica
    "52": 2,  # Dependientes en tiendas -> ESO
    "53": 4,  # Trabajadores de salud y cuidado de personas -> Bachillerato/FP Medio
    "54": 4,  # Trabajadores de protección y seguridad -> Bachillerato/FP Medio
    "55": 3,  # Otros trabajadores de servicios personales -> FP Básica

    # 6x - Trabajadores agrícolas y pesqueros: nivel 2-3
    "61": 3,  # Trabajadores cualificados agrícolas -> FP Básica
    "62": 3,  # Trabajadores cualificados ganaderos -> FP Básica
    "63": 3,  # Trabajadores cualificados agropecuarios mixtos -> FP Básica
    "64": 2,  # Trabajadores cualificados forestales, pesqueros -> ESO

    # 7x - Artesanos y trabajadores de manufactura: nivel 3-4
    "71": 3,  # Trabajadores en obras de construcción -> FP Básica
    "72": 3,  # Trabajadores de acabado de construcciones -> FP Básica
    "73": 4,  # Soldadores, chapistas, montadores -> Bachillerato/FP Medio
    "74": 4,  # Mecánicos y ajustadores de maquinaria -> Bachillerato/FP Medio
    "75": 4,  # Trabajadores de electricidad y electrotecnología -> Bachillerato/FP Medio
    "76": 3,  # Mecánicos de precisión, ceramistas, vidrieros -> FP Básica
    "77": 3,  # Trabajadores de alimentación, bebidas -> FP Básica
    "78": 3,  # Trabajadores de madera, textil, confección -> FP Básica

    # 8x - Operadores de instalaciones y maquinaria: nivel 3-4
    "81": 4,  # Operadores de instalaciones y maquinaria fijas -> Bachillerato/FP Medio
    "82": 3,  # Montadores y ensambladores -> FP Básica
    "83": 3,  # Maquinistas, operadores de maquinaria agrícola -> FP Básica
    "84": 3,  # Conductores de vehículos -> FP Básica

    # 9x - Ocupaciones elementales: nivel 1-2
    "91": 1,  # Empleados domésticos -> Sin cualificación formal
    "92": 1,  # Otro personal de limpieza -> Sin cualificación formal
    "93": 1,  # Ayudantes de preparación de alimentos -> Sin cualificación formal
    "94": 1,  # Recogedores de residuos, vendedores callejeros -> Sin cualificación formal
    "95": 2,  # Peones agrarios, forestales -> ESO
    "96": 2,  # Peones de construcción y minería -> ESO
    "97": 2,  # Peones de industrias manufactureras -> ESO
    "98": 2,  # Peones del transporte, descargadores -> ESO
}


def load_cno11_mapping() -> list[dict]:
    """Carga el mapeo CNO-11 desde el fichero JSON."""
    with open(CNO11_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["occupations"]


def build_education_table(occupations: list[dict]) -> pd.DataFrame:
    """Construye la tabla de educación para todas las ocupaciones."""
    rows = []

    for occ in occupations:
        code = occ["code"]
        title = occ["title"]

        level = EDUCATION_MAP.get(code)
        if level is None:
            # Fallback: asignar nivel basado en el primer dígito
            first_digit = code[0]
            default_levels = {
                "1": 6, "2": 6, "3": 5, "4": 4, "5": 3,
                "6": 3, "7": 3, "8": 3, "9": 1, "0": 4,
            }
            level = default_levels.get(first_digit, 3)
            print(f"  [AVISO] Código {code} no encontrado en EDUCATION_MAP, usando nivel por defecto: {level}")

        label = EDUCATION_LEVELS.get(level, "Desconocido")

        rows.append({
            "cno_code": code,
            "occupation_name": title,
            "education_level": level,
            "education_label": label,
        })

    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser(description="Asigna nivel educativo a ocupaciones CNO-11")
    parser.add_argument("--force", action="store_true", help="Forzar regeneración aunque exista fichero")
    args = parser.parse_args()

    print("=" * 60)
    print("04 - Mapeo de nivel educativo por ocupación CNO-11")
    print("=" * 60)

    if not args.force and OUTPUT_FILE.exists():
        print("  [INFO] Fichero de educación ya existe. Usa --force para regenerar.")
        print("  [OK] Usando datos existentes en caché.")
        return

    occupations = load_cno11_mapping()
    print(f"  [INFO] Cargadas {len(occupations)} ocupaciones CNO-11.")

    df = build_education_table(occupations)

    # Guardar
    RAW_DIR.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"  [OK] Tabla de educación guardada en {OUTPUT_FILE} ({len(df)} filas)")

    # Resumen por nivel educativo
    print("\n  Distribución por nivel educativo:")
    for level in sorted(df["education_level"].unique()):
        label = EDUCATION_LEVELS.get(level, "?")
        count = len(df[df["education_level"] == level])
        print(f"    Nivel {level} ({label}): {count} ocupaciones")

    print("=" * 60)
    print("  Paso 04 completado.")
    print("=" * 60)


if __name__ == "__main__":
    main()
