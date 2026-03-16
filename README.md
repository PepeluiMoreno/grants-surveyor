# GrantsSurveyor

**Banco de trabajo de investigación sobre datos públicos**

GrantsSurveyor es un sistema de investigación asistida por IA que opera sobre
datos extraídos por [OpenDataManager](https://github.com/PepeluiMoreno/opendatamanager).
Su función es construir grafos de conocimiento, evaluar patrones con un motor de
reglas y presentar hallazgos al analista para su revisión y publicación.

## Arquitectura

```
OpenDataManager                    GrantsSurveyor
───────────────                    ──────────────
Fetcher BDNS  → Dataset → webhook → ODMClient
Fetcher RER   → Dataset → webhook → Normalizer
Fetcher [...]                           ↓
                                   RuleEngine + LLM
                                        ↓
                                   GraphStore (Actor/Proceso/Activo)
                                        ↓
                                   Workbench analista
```

**OpenDataManager** gestiona la extracción, transformación y versionado de datos.
**GrantsSurveyor** aporta la inteligencia: motor de reglas, grafo de conocimiento,
evaluación con IA y banco de trabajo para el analista.

## Metadominio genérico

GrantsSurveyor no está acoplado a ningún dominio. El mismo sistema, sin cambios
de código, investiga subvenciones, ventas de inmuebles o contratos públicos.
La semántica específica vive exclusivamente en el `WatchProfile` de cada investigación.

| Tipo | Qué representa |
|------|---------------|
| Actor | Organización, persona, administración, empresa... |
| Proceso | Concesión, contrato, compraventa, nombramiento... |
| Activo | Inmueble, licencia, cargo, patente... |
| Fuente | Dataset de ODM o documento externo |
| Hallazgo | Agregación de señales que supera el umbral de la investigación |

## Instalación

```bash
git clone https://github.com/PepeluiMoreno/grants-surveyor
cd grants-surveyor
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env   # añade ANTHROPIC_API_KEY y ODM_GRAPHQL_URL
```

## Requisitos

- Python 3.11+
- [OpenDataManager](https://github.com/PepeluiMoreno/opendatamanager) corriendo y accesible
- API key de Anthropic

## Niveles de certeza

| Nivel | Rango | Publicable |
|-------|-------|-----------|
| 1 — Estructural | 95–100% | ✅ |
| 2 — Registral | 80–94% | ✅ |
| 3 — Red de afiliaciones | 60–79% | ✅ |
| 4 — Señales léxicas | 40–59% | ❌ |
| 5 — Inferencia débil | 20–39% | ❌ |

Solo niveles 1–3 con confirmación humana explícita son publicables.

## Licencia

AGPL-3.0
