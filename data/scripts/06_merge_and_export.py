#!/usr/bin/env python3
"""
06_merge_and_export.py
Fusiona todos los datos intermedios y genera el fichero data.json final.

Usa un mapeo EXPLÍCITO de nombres EPA → códigos CNO-11 para evitar
errores de fuzzy matching. La EPA tiene datos a múltiples niveles de
agregación y hay que seleccionar solo las hojas (nivel más granular).
"""

import argparse
import json
import re
import sys
import unicodedata
from datetime import date
from pathlib import Path

import pandas as pd

# Importar configuración
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import RAW_DIR, OUTPUT_DIR, CNO_CATEGORIES, EDUCATION_LEVELS, OUTLOOK_THRESHOLDS

CNO11_PATH = Path(__file__).resolve().parent / "cno11_mapping.json"
OUTPUT_FILE = OUTPUT_DIR / "data.json"

# Ficheros intermedios
EPA_LATEST_FILE = RAW_DIR / "epa_latest.csv"
EPA_TIMESERIES_FILE = RAW_DIR / "epa_employment.csv"
SALARY_FILE = RAW_DIR / "eaes_salaries.csv"
OUTLOOK_FILE = RAW_DIR / "outlook.csv"
EDUCATION_FILE = RAW_DIR / "education.csv"
AI_EXPOSURE_FILE = RAW_DIR / "ai_exposure.csv"

# ── MAPEO EXPLÍCITO: nombre EPA → código(s) CNO-11 ──
# Verificado manualmente contra la estructura de la EPA tabla 65967.
# La EPA tiene entradas a nivel 1-dígito (agregados), sub-agregados, y hojas.
# Solo usamos hojas. Donde una hoja EPA corresponde a múltiples CNO-11 sub-códigos
# que no se desglosan más, la asignamos al código padre.
# Donde varios EPA → 1 CNO, se suman los empleados.
EPA_TO_CNO = {
    # ── Grupo 1: Dirección y Gerencia ──
    "Miembros del poder ejecutivo y de los cuerpos legislativos; directivos de la Administración Pública y organizaciones de interés social; directores ejecutivos": "11",
    "Directores de departamentos administrativos y comerciales": "12",
    "Directores de producción y operaciones": "13",
    "Directores y gerentes de empresas de alojamiento, restauración y comercio": "14",
    "Directores y gerentes de otras empresas de servicios no clasificados bajo otros epígrafes": "15",

    # ── Grupo 2: Profesionales Científicos ──
    "Profesionales de la salud": "21",
    "Profesionales de la enseñanza infantil, primaria, secundaria y postsecundaria": "22",
    "Otros profesionales de la enseñanza": "22",  # Se suma al anterior
    "Profesionales de la ciencias físicas, químicas, matemáticas y de las ingenierías": "23",
    "Profesionales de las tecnologías de la información": "24",
    "Profesionales en derecho": "25",
    "Especialistas en organización de la Administración Pública y de las empresas y en la comercialización": "26",
    "Profesionales en ciencias sociales": "27",
    "Profesionales de la cultura y el espectáculo": "28",

    # ── Grupo 3: Técnicos y Profesionales de Apoyo ──
    "Técnicos de las ciencias y de las ingenierías": "31",
    "Supervisores en ingeniería de minas, de industrias manufactureras y de la construcción": "32",
    "Técnicos sanitarios y profesionales de las terapias alternativas": "33",
    "Profesionales de apoyo en finanzas y matemáticas": "34",
    "Representantes, agentes comerciales y afines": "35",
    "Profesionales de apoyo a la gestión administrativa; técnicos de las fuerzas y cuerpos de seguridad": "36",
    "Técnicos de las tecnologías de la información y las comunicaciones (TIC)": "37",
    "Profesionales de apoyo de servicios jurídicos, sociales, culturales, deportivos y afines": "38",

    # ── Grupo 4: Empleados Contables y Administrativos ──
    "Empleados en servicios contables, financieros, y de servicios de apoyo a la producción y al transporte": "41",
    "Empleados de bibliotecas, servicios de correos y afines": "42",
    "Otros empleados administrativos sin tareas de atención al público": "43",
    "Empleados de agencias de viajes, recepcionistas y telefonistas; empleados de ventanilla y afines (excepto taquilleros)": "44",
    "Empleados administrativos con tareas de atención al público no clasificados bajo otros epígrafes": "44",  # Se suma

    # ── Grupo 5: Trabajadores de Servicios y Comercio ──
    "Trabajadores asalariados de los servicios de restauración": "51",
    "Camareros y cocineros propietarios": "51",  # Se suma
    "Dependientes en tiendas y almacenes": "52",
    "Comerciantes propietarios de tiendas": "52",  # Se suma
    "Cajeros y taquilleros (excepto bancos)": "52",  # Se suma
    "Vendedores (excepto en tiendas y almacenes)": "52",  # Se suma
    "Trabajadores de los cuidados a las personas en servicios de salud": "53",
    "Otros trabajadores de los cuidados a las personas": "53",  # Se suma
    "Trabajadores de los servicios de protección y seguridad": "54",
    "Trabajadores de los servicios personales": "55",

    # ── Grupo 6: Trabajadores Agrícolas y Pesqueros ──
    "Trabajadores cualificados en actividades agrícolas": "61",
    "Trabajadores cualificados en actividades ganaderas, (incluidas avícolas, apícolas y similares)": "62",
    "Trabajadores cualificados en actividades agropecuarias mixtas": "63",
    "Trabajadores cualificados en actividades forestales, pesqueras y cinegéticas": "64",

    # ── Grupo 7: Artesanos y Trabajadores de Manufactura ──
    "Trabajadores en obras estructurales de construcción y afines": "71",
    "Trabajadores de acabado de construcciones e instalaciones (excepto electricistas), pintores y afines": "72",
    "Soldadores, chapistas, montadores de estructuras metálicas, herreros, elaboradores de herramientas y afines": "73",
    "Mecánicos y ajustadores de maquinaria": "74",
    "Trabajadores especializados en electricidad y electrotecnología": "75",
    "Mecánicos de precisión en metales, ceramistas, vidrieros, artesanos y trabajadores de artes gráficas": "76",
    "Trabajadores de la industria de la alimentación, bebidas y tabaco": "77",
    "Trabajadores de la madera, textil, confección, piel, cuero, calzado y otros operarios en oficios": "78",

    # ── Grupo 8: Operadores de Instalaciones y Maquinaria ──
    "Operadores de instalaciones y maquinaria fijas": "81",
    "Montadores y ensambladores en fábricas": "82",
    "Maquinistas de locomotoras, operadores de maquinaria agrícola y de equipos pesados móviles, y marineros": "83",
    "Conductores de vehículos para el transporte urbano o por carretera": "84",

    # ── Grupo 9: Ocupaciones Elementales ──
    "Empleados domésticos": "91",
    "Otro personal de limpieza": "92",
    "Ayudantes de preparación de alimentos": "93",
    "Recogedores de residuos urbanos, vendedores callejeros y otras ocupaciones elementales en servicios": "94",
    "Peones agrarios, forestales y de la pesca": "95",
    "Peones de la construcción y de la minería": "96",
    "Peones de las industrias manufactureras": "97",
    "Peones del transporte, descargadores y reponedores": "98",

    # ── Grupo 0: Ocupaciones Militares ──
    "Ocupaciones militares": None,  # No hay código 2-dígitos, ignorar
}

# Entradas EPA que son agregados (ignorar)
EPA_AGGREGATES = {
    "Total",
    "Directores y gerentes",
    "Técnicos y profesionales científicos e intelectuales",
    "Técnicos y profesionales científicos e intelectuales de la salud y la enseñanza",
    "Otros técnicos y profesionales científicos e intelectuales",
    "Técnicos; profesionales de apoyo",
    "Empleados contables, administrativos y otros empleados de oficina",
    "Empleados de oficina que no atienden al público",
    "Empleados de oficina que atienden al público",
    "Trabajadores de los servicios de restauración, personales, protección y vendedores",
    "Trabajadores de los servicios de restauración y comercio",
    "Trabajadores de los servicios de salud y el cuidado de personas",
    "Trabajadores cualificados en el sector agrícola, ganadero, forestal y pesquero",
    "Artesanos y trabajadores cualificados de las industrias manufactureras y la construcción (excepto operadores de instalaciones y maquinaria)",
    "Trabajadores cualificados de la construcción, excepto operadores de máquinas",
    "Trabajadores cualificados de las industrias manufactureras, excepto operadores de instalaciones y máquinas",
    "Operadores de instalaciones y maquinaria, y montadores",
    "Operadores de instalaciones y maquinaria fijas, y montadores",
    "Conductores y operadores de maquinaria móvil",
    "Ocupaciones elementales",
    "Trabajadores no cualificados en servicios (excepto transportes)",
    "Peones de la agricultura, pesca, construcción, industrias manufactureras y transportes",
}


def load_cno11_mapping() -> list[dict]:
    with open(CNO11_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["occupations"]


def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")
    text = re.sub(r"-+", "-", text)
    return text


def build_ine_url(cno_code: str) -> str:
    return "https://www.ine.es/jaxiT3/Datos.htm?t=65967#!tabs-tabla"


def main():
    parser = argparse.ArgumentParser(description="Fusiona datos y genera data.json")
    parser.add_argument("--force", action="store_true", help="Forzar regeneración")
    args = parser.parse_args()

    print("=" * 60)
    print("06 - Fusión de datos y exportación a data.json")
    print("=" * 60)

    if not args.force and OUTPUT_FILE.exists():
        print("  [INFO] data.json ya existe. Usa --force para regenerar.")
        return

    cno_occupations = load_cno11_mapping()
    print(f"  [INFO] Cargadas {len(cno_occupations)} ocupaciones CNO-11.")

    # Verificar ficheros intermedios
    required = {
        "Empleo EPA": EPA_LATEST_FILE,
        "Salarios EAES": SALARY_FILE,
        "Outlook": OUTLOOK_FILE,
        "Educación": EDUCATION_FILE,
        "Exposición IA": AI_EXPOSURE_FILE,
    }
    missing = [f"    - {n}: {p}" for n, p in required.items() if not p.exists()]
    if missing:
        print("  [ERROR] Faltan ficheros:\n" + "\n".join(missing))
        raise SystemExit(1)

    # Cargar datos
    df_employment = pd.read_csv(EPA_LATEST_FILE)
    df_salary = pd.read_csv(SALARY_FILE)
    df_outlook = pd.read_csv(OUTLOOK_FILE)
    df_education = pd.read_csv(EDUCATION_FILE)
    df_ai = pd.read_csv(AI_EXPOSURE_FILE)

    # ── Mapeo explícito EPA → CNO-11 (con suma de empleos) ──
    print("\n  [INFO] Mapeando EPA → CNO-11 con mapeo explícito...")
    jobs_by_cno = {}   # code → total employed
    outlook_by_cno = {}  # code → list of (employed, pct)

    # Outlook lookup
    outlook_lookup = {
        row["occupation_name"]: {"pct": row["outlook_pct"], "desc": row["outlook_desc"]}
        for _, row in df_outlook.iterrows()
    }

    unmatched = []
    for _, row in df_employment.iterrows():
        epa_name = row["occupation_name"]
        employed = int(row["employed"])

        if epa_name in EPA_AGGREGATES:
            continue

        cno_code = EPA_TO_CNO.get(epa_name)
        if cno_code is None:
            if epa_name not in EPA_TO_CNO:
                unmatched.append(f"    ⚠ No mapeado: '{epa_name}' ({employed:,})")
            continue

        jobs_by_cno[cno_code] = jobs_by_cno.get(cno_code, 0) + employed

        # Para outlook, guardamos weighted average
        ol = outlook_lookup.get(epa_name, {"pct": 0.0, "desc": "Estable"})
        if cno_code not in outlook_by_cno:
            outlook_by_cno[cno_code] = []
        outlook_by_cno[cno_code].append((employed, ol["pct"]))

    if unmatched:
        print(f"  [AVISO] {len(unmatched)} nombres EPA sin mapear:")
        for u in unmatched[:10]:
            print(u)

    # Lookups por código
    salary_by_code = dict(zip(df_salary["cno_code"].astype(str), df_salary["salary"]))
    edu_by_code = {
        str(r["cno_code"]): {"level": r["education_level"], "label": r["education_label"]}
        for _, r in df_education.iterrows()
    }
    ai_by_code = {
        str(r["cno_code"]): {"score": r["ai_exposure"], "rationale": r["ai_rationale"]}
        for _, r in df_ai.iterrows()
    }

    # ── Construir ocupaciones ──
    print("\n  [INFO] Construyendo datos finales...")
    occupations = []

    for occ in cno_occupations:
        code = occ["code"]
        title = occ["title"]
        cat_digit = occ["category"]
        cat_info = CNO_CATEGORIES.get(cat_digit, {"label": "Desconocida", "slug": "desconocida"})

        jobs = jobs_by_cno.get(code, 0)
        pay = salary_by_code.get(code, 0)
        edu = edu_by_code.get(code, {"level": 3, "label": "FP Básica"})
        ai = ai_by_code.get(code, {"score": 5, "rationale": "Sin datos específicos."})

        # Outlook: media ponderada si hay múltiples entradas EPA para este código
        ol_entries = outlook_by_cno.get(code, [])
        if ol_entries:
            total_w = sum(e for e, _ in ol_entries)
            if total_w > 0:
                outlook_pct = sum(e * p for e, p in ol_entries) / total_w
            else:
                outlook_pct = sum(p for _, p in ol_entries) / len(ol_entries)
        else:
            outlook_pct = 0.0

        # Clasificar outlook
        outlook_desc = "Estable"
        for desc, (lower, upper) in OUTLOOK_THRESHOLDS.items():
            if lower <= outlook_pct < upper:
                outlook_desc = desc
                break

        occupations.append({
            "title": title,
            "slug": slugify(title),
            "cno_code": code,
            "category": cat_info["slug"],
            "category_label": cat_info["label"],
            "jobs": jobs,
            "pay": int(pay) if pd.notna(pay) else 0,
            "outlook": round(outlook_pct, 2),
            "outlook_desc": outlook_desc,
            "education": edu["label"],
            "education_level": int(edu["level"]) if pd.notna(edu["level"]) else 3,
            "exposure": int(ai["score"]) if pd.notna(ai["score"]) else 5,
            "exposure_rationale": ai["rationale"] if pd.notna(ai.get("rationale", "")) else "Sin datos específicos.",
            "url": build_ine_url(code),
        })

    occupations.sort(key=lambda x: x["cno_code"])
    total_employed = sum(o["jobs"] for o in occupations)

    # ── Validación ──
    print("\n  [INFO] Validando datos...")
    errs = []
    for occ in occupations:
        if occ["pay"] <= 0:
            errs.append(f"    Salario <= 0: CNO {occ['cno_code']}")
        if not (0 <= occ["exposure"] <= 10):
            errs.append(f"    Exposición fuera de rango: CNO {occ['cno_code']}: {occ['exposure']}")
        if not (1 <= occ["education_level"] <= 8):
            errs.append(f"    Educación fuera de rango: CNO {occ['cno_code']}: {occ['education_level']}")

    # Validar total contra EPA
    epa_total_row = df_employment[df_employment["occupation_name"] == "Total"]
    if not epa_total_row.empty:
        epa_real_total = int(epa_total_row.iloc[0]["employed"])
        diff_pct = abs(total_employed - epa_real_total) / epa_real_total * 100
        print(f"  [CHECK] Total calculado: {total_employed:,} vs EPA Total: {epa_real_total:,} (diff: {diff_pct:.1f}%)")
        if diff_pct > 5:
            errs.append(f"    Total difiere >5% del EPA Total")

    if errs:
        print("  [AVISO] Problemas encontrados:")
        for e in errs:
            print(e)
    else:
        print("  [OK] Validación superada.")

    # Sin datos EPA
    zero_jobs = [o for o in occupations if o["jobs"] == 0]
    if zero_jobs:
        print(f"\n  [INFO] {len(zero_jobs)} ocupaciones con 0 empleados:")
        for o in zero_jobs:
            print(f"    CNO {o['cno_code']} {o['title'][:60]}")

    # Exportar
    output = {
        "meta": {
            "source": "INE (EPA, EAES)",
            "generated": date.today().isoformat(),
            "total_employed": total_employed,
            "currency": "EUR",
        },
        "occupations": occupations,
    }

    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n  [OK] data.json exportado: {OUTPUT_FILE}")
    print(f"  [INFO] {len(occupations)} ocupaciones, {total_employed:,} empleados totales")
    print(f"  [INFO] Tamaño: {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")

    # Resumen por categoría
    print("\n  Resumen por categoría:")
    cats = {}
    for occ in occupations:
        c = occ["category_label"]
        if c not in cats:
            cats[c] = {"n": 0, "jobs": 0}
        cats[c]["n"] += 1
        cats[c]["jobs"] += occ["jobs"]

    for c, info in sorted(cats.items()):
        print(f"    {c}: {info['n']} ocupaciones, {info['jobs']:,} empleados")

    print("=" * 60)
    print("  Paso 06 completado.")
    print("=" * 60)


if __name__ == "__main__":
    main()
