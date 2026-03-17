# OcupIA — Mercado Laboral Español

Visualizador interactivo del mercado laboral español. Treemap de ~54 ocupaciones (clasificación CNO-11) con cuatro capas de datos: perspectiva de crecimiento, salario medio, nivel de formación requerido y exposición a la inteligencia artificial.

Fuentes: INE — EPA (tabla 65967) y EAES (tabla 28186). Exposición IA generada con Claude API.

**Demo:** abre `index.html` directamente en el navegador — no requiere servidor.

---

## Características

- Treemap squarified renderizado en Canvas 2D (sin dependencias externas)
- Cuatro capas de color intercambiables: Perspectiva · Salario · Formación · Exposición IA
- Tooltip con detalles por ocupación y rationale de exposición a IA
- Sidebar con estadísticas y distribución de la capa activa
- Responsive: sidebar oculto en móvil
- Datos estáticos en `data.json` — cero backend

---

## Estructura del proyecto

```
esjobs/
├── index.html          # Aplicación completa (HTML + CSS + JS inline)
├── data.json           # Datos finales (generado por el pipeline)
├── data/
│   ├── requirements.txt
│   ├── raw/            # CSVs intermedios (generados, no versionados)
│   └── scripts/
│       ├── config.py               # URLs INE, constantes, mapeos CNO-11
│       ├── cno11_mapping.json      # Catálogo oficial de ocupaciones
│       ├── 01_fetch_epa_employment.py   # Descarga datos de empleo (EPA)
│       ├── 02_fetch_eaes_salary.py      # Descarga salarios (EAES)
│       ├── 03_compute_outlook.py        # Calcula perspectiva de crecimiento
│       ├── 04_map_education.py          # Asigna niveles educativos MECES
│       ├── 05_generate_ai_exposure.py   # Genera exposición IA via Claude API
│       └── 06_merge_and_export.py       # Fusiona todo → data.json
```

---

## Pipeline de datos

Los scripts se ejecutan en orden. Cada uno genera un CSV en `data/raw/` que el siguiente consume.

```
INE EPA API ──► 01 ──► epa_employment.csv, epa_latest.csv
INE EAES API ─► 02 ──► eaes_salaries.csv
                03 ──► outlook.csv          (% crecimiento interanual)
                04 ──► education.csv         (niveles MECES por CNO)
Claude API ───► 05 ──► ai_exposure.csv      (score 0-10 + rationale)
                06 ──► data.json            (output final)
```

### Setup

```bash
cd data
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export ANTHROPIC_API_KEY=sk-...   # necesario solo para el paso 05
```

### Ejecutar el pipeline completo

```bash
python scripts/01_fetch_epa_employment.py
python scripts/02_fetch_eaes_salary.py
python scripts/03_compute_outlook.py
python scripts/04_map_education.py
python scripts/05_generate_ai_exposure.py
python scripts/06_merge_and_export.py
```

Para regenerar `data.json` aunque ya exista:

```bash
python scripts/06_merge_and_export.py --force
```

### Actualizar solo los datos del INE (sin regenear IA)

```bash
python scripts/01_fetch_epa_employment.py
python scripts/02_fetch_eaes_salary.py
python scripts/03_compute_outlook.py
python scripts/06_merge_and_export.py --force
```

---

## Arquitectura frontend

`index.html` es un fichero único autocontenido. No hay bundler, no hay framework, no hay dependencias npm.

**Decisiones de diseño deliberadas:**
- **Canvas 2D en lugar de SVG/D3**: mejor rendimiento en resize con `devicePixelRatio`, sin overhead del DOM para ~54 elementos.
- **Sin dependencias externas**: el archivo se puede abrir desde cualquier sistema de ficheros sin conexión a internet.
- **Hit-testing manual**: el array `tileRects[]` almacena las coordenadas de cada tile para detectar hover/click, reemplazando el sistema de eventos del DOM.
- **Datos estáticos**: `data.json` se regenera cuando cambian las fuentes, no en cada visita.

### Flujo de renderizado

```
init()
  └─► fetch("data.json")
        └─► render()
              ├─► squarify(occupations, bounds)   → tileRects[]
              ├─► drawTiles(ctx, tileRects)
              └─► drawLabels(ctx, tileRects)

onMouseMove → getTileAt(x, y) → showTooltip / updateSidebar
onClick     → getTileAt(x, y) → window.open(occ.url)
```

---

## Datos

### Campos por ocupación en `data.json`

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `title` | string | Nombre oficial CNO-11 |
| `cno_code` | string | Código CNO-11 a 2 dígitos |
| `category` | string | Slug del grupo principal (1 dígito) |
| `jobs` | number | Ocupados (EPA, último dato disponible) |
| `pay` | number | Salario medio anual en EUR (EAES) |
| `outlook` | number | Crecimiento interanual en % |
| `outlook_desc` | string | Muy alto / Alto / Moderado / Estable / En descenso |
| `education` | string | Nivel MECES requerido |
| `education_level` | number | 1–8 (escala MECES) |
| `exposure` | number | Exposición a IA, 0–10 |
| `exposure_rationale` | string | Explicación generada por Claude |

### Actualización de datos

Los datos del INE se actualizan trimestralmente (EPA) y anualmente (EAES). Para refrescar:

1. Ejecutar pasos 01–03 del pipeline
2. Ejecutar paso 06 con `--force`
3. Verificar el log de validación (diff vs. total EPA < 5%)

---

## Fuentes

- [INE — EPA tabla 65967](https://www.ine.es/jaxiT3/Datos.htm?t=65967) — Ocupados por grupo CNO-11
- [INE — EAES tabla 28186](https://www.ine.es/jaxiT3/Datos.htm?t=28186) — Ganancia media anual
- [CNO-11](https://www.ine.es/dyngs/INEbase/es/operacion.htm?c=Estadistica_C&cid=1254736177033&menu=ultiDatos&idp=1254735976614) — Clasificación Nacional de Ocupaciones
- [Claude API](https://www.anthropic.com) — Generación de scores de exposición a IA
