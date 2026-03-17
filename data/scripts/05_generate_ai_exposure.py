#!/usr/bin/env python3
"""
05_generate_ai_exposure.py
Genera puntuaciones de exposición a la IA para cada ocupación CNO-11.
Usa la API de Anthropic si ANTHROPIC_API_KEY está disponible,
en caso contrario usa valores por defecto razonables.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd

# Importar configuración
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import RAW_DIR

CNO11_PATH = Path(__file__).resolve().parent / "cno11_mapping.json"
OUTPUT_FILE = RAW_DIR / "ai_exposure.csv"

# Puntuaciones por defecto de exposición a la IA (0-10)
# 0 = sin impacto, 10 = alta automatización o transformación
DEFAULT_AI_SCORES = {
    # 1x - Directores y gerentes
    "11": {"score": 4, "rationale": "La toma de decisiones estratégicas requiere juicio humano, pero la IA asiste en análisis de datos y planificación."},
    "12": {"score": 5, "rationale": "La gestión administrativa y comercial se ve transformada por IA en análisis, reporting y automatización de procesos."},
    "13": {"score": 5, "rationale": "La dirección de producción se beneficia de IA en optimización de cadenas de suministro y planificación."},
    "14": {"score": 4, "rationale": "La gestión de hostelería y comercio usa IA para previsión de demanda y gestión de inventarios."},
    "15": {"score": 4, "rationale": "La dirección de servicios incorpora IA para análisis de mercado y atención al cliente automatizada."},

    # 2x - Profesionales científicos e intelectuales
    "21": {"score": 5, "rationale": "La IA transforma el diagnóstico médico y la investigación, pero el cuidado del paciente sigue siendo humano."},
    "22": {"score": 4, "rationale": "La enseñanza se complementa con IA para personalización del aprendizaje, pero la interacción humana es clave."},
    "23": {"score": 6, "rationale": "Las ciencias e ingenierías se ven muy impactadas por IA en modelado, simulación y análisis de datos."},
    "24": {"score": 8, "rationale": "Los profesionales TI están altamente expuestos: la IA genera código, automatiza testing y transforma el desarrollo."},
    "25": {"score": 6, "rationale": "La IA automatiza la búsqueda jurídica, revisión de contratos y análisis de jurisprudencia."},
    "26": {"score": 7, "rationale": "Marketing, análisis financiero y gestión empresarial se transforman profundamente con IA generativa y analítica."},
    "27": {"score": 5, "rationale": "Las ciencias sociales usan IA para análisis de datos cualitativos y cuantitativos a gran escala."},
    "28": {"score": 6, "rationale": "La IA generativa impacta fuertemente en diseño, producción de contenido y artes visuales."},

    # 3x - Técnicos y profesionales de apoyo
    "31": {"score": 5, "rationale": "Los técnicos usan IA para diagnóstico y mantenimiento predictivo en ingeniería."},
    "32": {"score": 4, "rationale": "La supervisión en manufactura se apoya en IA para control de calidad y planificación."},
    "33": {"score": 4, "rationale": "Los técnicos sanitarios usan IA para diagnóstico por imagen y gestión de datos clínicos."},
    "34": {"score": 7, "rationale": "Las finanzas y contabilidad están altamente expuestas a automatización por IA."},
    "35": {"score": 5, "rationale": "Los agentes comerciales se apoyan en IA para CRM, predicción de ventas y comunicación."},
    "36": {"score": 6, "rationale": "La gestión administrativa se automatiza significativamente con IA en documentación y procesos."},
    "37": {"score": 7, "rationale": "Los técnicos TIC están muy expuestos a la automatización de tareas rutinarias por IA."},
    "38": {"score": 4, "rationale": "Los servicios sociales y culturales requieren empatía humana, con IA como apoyo analítico."},

    # 4x - Empleados contables y administrativos
    "41": {"score": 8, "rationale": "La contabilidad y servicios financieros rutinarios están entre los más automatizables por IA."},
    "42": {"score": 6, "rationale": "Los servicios de correos y bibliotecas se automatizan con IA para clasificación y gestión."},
    "43": {"score": 8, "rationale": "Las tareas administrativas sin atención al público son altamente automatizables por IA."},
    "44": {"score": 6, "rationale": "La recepción y atención telefónica se transforma con chatbots y asistentes virtuales de IA."},

    # 5x - Trabajadores de servicios y comercio
    "51": {"score": 3, "rationale": "La restauración es principalmente manual y presencial, con IA limitada a gestión de pedidos."},
    "52": {"score": 5, "rationale": "El comercio minorista se transforma con cajas automáticas y recomendaciones por IA, pero requiere presencia."},
    "53": {"score": 3, "rationale": "El cuidado de personas requiere presencia humana y empatía; la IA es solo apoyo."},
    "54": {"score": 3, "rationale": "La seguridad se apoya en IA para videovigilancia, pero la intervención humana es esencial."},
    "55": {"score": 3, "rationale": "Los servicios personales requieren interacción humana directa; la IA tiene impacto limitado."},

    # 6x - Trabajadores agrícolas y pesqueros
    "61": {"score": 3, "rationale": "La agricultura usa IA para agricultura de precisión, pero el trabajo manual sigue siendo necesario."},
    "62": {"score": 2, "rationale": "La ganadería requiere presencia física; la IA ayuda en monitorización pero no reemplaza."},
    "63": {"score": 3, "rationale": "Las actividades agropecuarias mixtas usan IA para optimización, manteniendo componente manual."},
    "64": {"score": 2, "rationale": "Las actividades forestales y pesqueras son principalmente manuales y en entornos variables."},

    # 7x - Artesanos y trabajadores de manufactura
    "71": {"score": 2, "rationale": "La construcción requiere trabajo físico en entornos variables; la IA tiene impacto limitado."},
    "72": {"score": 2, "rationale": "Los acabados de construcción requieren destreza manual que la IA no puede replicar fácilmente."},
    "73": {"score": 3, "rationale": "La soldadura y montaje metálico se automatizan parcialmente con robótica asistida por IA."},
    "74": {"score": 3, "rationale": "El mantenimiento mecánico usa IA para diagnóstico predictivo, pero la reparación es manual."},
    "75": {"score": 3, "rationale": "El trabajo eléctrico requiere presencia física; la IA asiste en diseño y diagnóstico."},
    "76": {"score": 2, "rationale": "Los trabajos de precisión artesanal requieren habilidades manuales únicas."},
    "77": {"score": 3, "rationale": "La industria alimentaria se automatiza parcialmente con IA en control de calidad."},
    "78": {"score": 3, "rationale": "El textil y la madera se automatizan con robótica, pero mantienen componente manual."},

    # 8x - Operadores de instalaciones y maquinaria
    "81": {"score": 4, "rationale": "Los operadores de maquinaria fija se ven impactados por automatización y control por IA."},
    "82": {"score": 4, "rationale": "El ensamblaje en fábricas se automatiza progresivamente con robótica e IA."},
    "83": {"score": 4, "rationale": "La maquinaria agrícola y pesada incorpora automatización con IA y conducción autónoma."},
    "84": {"score": 5, "rationale": "Los conductores están expuestos a vehículos autónomos, aunque la implementación completa es lejana."},

    # 9x - Ocupaciones elementales
    "91": {"score": 1, "rationale": "El trabajo doméstico requiere presencia física y adaptación a entornos variados."},
    "92": {"score": 2, "rationale": "La limpieza se automatiza parcialmente con robots, pero la mayoría de tareas requieren personas."},
    "93": {"score": 2, "rationale": "La preparación básica de alimentos se automatiza parcialmente en cadenas, pero sigue siendo manual."},
    "94": {"score": 1, "rationale": "La recogida de residuos y venta callejera requieren presencia humana y adaptación."},
    "95": {"score": 1, "rationale": "Los peones agrarios realizan trabajo físico no estructurado difícil de automatizar."},
    "96": {"score": 1, "rationale": "Los peones de construcción realizan trabajo físico variado en entornos impredecibles."},
    "97": {"score": 3, "rationale": "Los peones de manufactura realizan tareas repetitivas parcialmente automatizables."},
    "98": {"score": 2, "rationale": "La descarga y reposición se automatiza parcialmente con logística inteligente."},
}


def load_cno11_mapping() -> list[dict]:
    """Carga el mapeo CNO-11 desde el fichero JSON."""
    with open(CNO11_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["occupations"]


def score_with_anthropic(occupations: list[dict]) -> list[dict] | None:
    """
    Usa la API de Anthropic para puntuar la exposición a la IA.
    Requiere ANTHROPIC_API_KEY en las variables de entorno.
    """
    try:
        import anthropic
    except ImportError:
        print("  [AVISO] Paquete 'anthropic' no instalado. Usando valores por defecto.")
        return None

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("  [INFO] ANTHROPIC_API_KEY no configurada. Usando valores por defecto.")
        return None

    client = anthropic.Anthropic(api_key=api_key)
    results = []

    for occ in occupations:
        code = occ["code"]
        title = occ["title"]

        prompt = (
            f"Puntúa del 0 al 10 la exposición de '{title}' (código CNO-11: {code}) "
            f"a la inteligencia artificial. "
            f"0 = sin impacto, 10 = alta automatización o transformación. "
            f'Responde SOLO en JSON: {{"score": N, "rationale": "..."}}'
        )

        try:
            print(f"    Consultando IA para {code} - {title[:50]}...")
            message = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=300,
                messages=[{"role": "user", "content": prompt}],
            )

            content_block = message.content[0]
            response_text = getattr(content_block, "text", "").strip()
            # Extraer JSON de la respuesta
            # Buscar el primer { y último }
            start = response_text.find("{")
            end = response_text.rfind("}") + 1
            if start >= 0 and end > start:
                json_str = response_text[start:end]
                parsed = json.loads(json_str)
                score = max(0, min(10, int(parsed.get("score", 5))))
                rationale = parsed.get("rationale", "Sin justificación disponible.")
            else:
                # Fallback si no se puede parsear
                score = DEFAULT_AI_SCORES.get(code, {}).get("score", 5)
                rationale = "Error al parsear respuesta de IA. Usando valor por defecto."

            results.append({
                "cno_code": code,
                "occupation_name": title,
                "ai_exposure": score,
                "ai_rationale": rationale,
                "source": "anthropic",
            })

            # Pausa para no saturar la API
            time.sleep(0.5)

        except Exception as e:
            print(f"    [ERROR] Error con la API para {code}: {e}")
            default = DEFAULT_AI_SCORES.get(code, {"score": 5, "rationale": "Error de API."})
            results.append({
                "cno_code": code,
                "occupation_name": title,
                "ai_exposure": default["score"],
                "ai_rationale": default["rationale"],
                "source": "default",
            })

    return results


def score_with_defaults(occupations: list[dict]) -> list[dict]:
    """Usa puntuaciones por defecto para la exposición a la IA."""
    results = []

    for occ in occupations:
        code = occ["code"]
        title = occ["title"]

        default = DEFAULT_AI_SCORES.get(code, {"score": 5, "rationale": "Sin datos específicos para esta ocupación."})

        results.append({
            "cno_code": code,
            "occupation_name": title,
            "ai_exposure": default["score"],
            "ai_rationale": default["rationale"],
            "source": "default",
        })

    return results


def main():
    parser = argparse.ArgumentParser(description="Genera puntuaciones de exposición a la IA")
    parser.add_argument("--force", action="store_true", help="Forzar regeneración aunque exista fichero")
    parser.add_argument("--use-api", action="store_true", help="Forzar uso de API Anthropic (requiere ANTHROPIC_API_KEY)")
    args = parser.parse_args()

    print("=" * 60)
    print("05 - Generación de puntuaciones de exposición a la IA")
    print("=" * 60)

    if not args.force and OUTPUT_FILE.exists():
        print("  [INFO] Fichero de exposición IA ya existe. Usa --force para regenerar.")
        print("  [OK] Usando datos existentes en caché.")
        return

    occupations = load_cno11_mapping()
    print(f"  [INFO] Cargadas {len(occupations)} ocupaciones CNO-11.")

    # Intentar usar API de Anthropic si está disponible
    results = None
    api_key = os.environ.get("ANTHROPIC_API_KEY")

    if api_key or args.use_api:
        print("  [INFO] Intentando usar API de Anthropic...")
        results = score_with_anthropic(occupations)

    if results is None:
        print("  [INFO] Usando puntuaciones por defecto.")
        results = score_with_defaults(occupations)

    df = pd.DataFrame(results)

    # Guardar
    RAW_DIR.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print(f"  [OK] Exposición IA guardada en {OUTPUT_FILE} ({len(df)} filas)")

    # Resumen
    api_count = len(df[df["source"] == "anthropic"]) if "source" in df.columns else 0
    default_count = len(df[df["source"] == "default"]) if "source" in df.columns else len(df)
    print(f"  [INFO] Puntuaciones API: {api_count}, Por defecto: {default_count}")

    # Distribución de puntuaciones
    print("\n  Distribución de exposición a la IA:")
    bins = [(0, 2, "Baja (0-2)"), (3, 5, "Media (3-5)"), (6, 8, "Alta (6-8)"), (9, 10, "Muy alta (9-10)")]
    for low, high, label in bins:
        count = len(df[(df["ai_exposure"] >= low) & (df["ai_exposure"] <= high)])
        if count > 0:
            print(f"    {label}: {count} ocupaciones")

    print("=" * 60)
    print("  Paso 05 completado.")
    print("=" * 60)


if __name__ == "__main__":
    main()
