"""
Configuración del pipeline de datos para esjobs.
URLs de APIs del INE, constantes y mapeos CNO-11.
"""

from pathlib import Path

# Directorios
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "raw"
OUTPUT_DIR = BASE_DIR.parent  # data.json va en la raíz del proyecto

RAW_DIR.mkdir(exist_ok=True)

# URLs del INE API
# EPA tabla 65967: Ocupados por grupo de ocupación (CNO-11), sexo y grupo de edad
EPA_TABLE_ID = "65967"
EPA_URL = f"https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{EPA_TABLE_ID}?nult=20"

# EAES tabla 28186: Ganancia media anual por trabajador (por ocupación)
EAES_TABLE_ID = "28186"
EAES_URL = f"https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{EAES_TABLE_ID}?nult=1"

# Multiplicador: datos EPA vienen en miles de personas
EPA_MULTIPLIER = 1000

# Clasificación de perspectiva de crecimiento (% interanual)
OUTLOOK_THRESHOLDS = {
    "Muy alto": (10, float("inf")),
    "Alto": (5, 10),
    "Moderado": (2, 5),
    "Estable": (0, 2),
    "En descenso": (float("-inf"), 0),
}

# Niveles educativos MECES (Marco Español de Cualificaciones para la Educación Superior)
EDUCATION_LEVELS = {
    1: "Sin cualificación formal",
    2: "ESO / Educación básica",
    3: "FP Básica",
    4: "Bachillerato / FP Grado Medio",
    5: "FP Grado Superior",
    6: "Grado universitario",
    7: "Máster universitario",
    8: "Doctorado",
}

# Categorías principales CNO-11 (1 dígito)
CNO_CATEGORIES = {
    "1": {"label": "Dirección y Gerencia", "slug": "direccion-y-gerencia"},
    "2": {"label": "Técnicos y Profesionales Científicos", "slug": "tecnicos-profesionales-cientificos"},
    "3": {"label": "Técnicos y Profesionales de Apoyo", "slug": "tecnicos-profesionales-apoyo"},
    "4": {"label": "Empleados Contables y Administrativos", "slug": "empleados-contables-administrativos"},
    "5": {"label": "Trabajadores de Servicios y Comercio", "slug": "trabajadores-servicios-comercio"},
    "6": {"label": "Trabajadores Agrícolas y Pesqueros", "slug": "trabajadores-agricolas-pesqueros"},
    "7": {"label": "Artesanos y Trabajadores de Manufactura", "slug": "artesanos-manufactura"},
    "8": {"label": "Operadores de Instalaciones y Maquinaria", "slug": "operadores-instalaciones-maquinaria"},
    "9": {"label": "Ocupaciones Elementales", "slug": "ocupaciones-elementales"},
    "0": {"label": "Ocupaciones Militares", "slug": "ocupaciones-militares"},
}

# Mapeo de grupos salariales EAES (17 grandes grupos) a categorías CNO-11
# La EAES usa una clasificación ligeramente diferente, aquí mapeamos
EAES_TO_CNO_MAP = {
    "1 Directores y gerentes": ["1"],
    "2 Técnicos y profesionales científicos e intelectuales": ["2"],
    "3 Técnicos; profesionales de apoyo": ["3"],
    "4 Empleados contables, administrativos y otros empleados de oficina": ["4"],
    "5 Trabajadores de los servicios de restauración, personales, protección y vendedores": ["5"],
    "6 Trabajadores cualificados en el sector agrícola, ganadero, forestal y pesquero": ["6"],
    "7 Artesanos y trabajadores cualificados de las industrias manufactureras y la construcción": ["7"],
    "8 Operadores de instalaciones y maquinaria, y montadores": ["8"],
    "9 Ocupaciones elementales": ["9"],
    "0 Ocupaciones militares": ["0"],
}

# Salarios por defecto (medianas aproximadas) en caso de que la API no devuelva datos
DEFAULT_SALARIES = {
    "1": 60986,
    "2": 38450,
    "3": 29800,
    "4": 23250,
    "5": 18500,
    "6": 19200,
    "7": 24100,
    "8": 24800,
    "9": 15800,
    "0": 32000,
}
