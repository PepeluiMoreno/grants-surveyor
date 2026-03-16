"""
models/rules.py — WatchProfile y árbol de reglas booleanas.

El WatchProfile es la única pieza específica de dominio en GrantsSurveyor.
Define qué busca el agente en términos del metadominio genérico.

Las reglas operan sobre rutas de atributos del grafo:
  "subject.attr[nif]"            — atributo 'nif' del nodo sujeto
  "subject.identifiers.nif"      — identificador directo
  "related.attr[actor_type]"     — atributo de nodo relacionado
  "relation.attr[role]"          — atributo de la relación

El analista nunca edita el YAML directamente.
La UI (Rule Editor) lo genera y serializa.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Union

from pydantic import BaseModel, Field

from .base import EvidenceLevel


# ---------------------------------------------------------------------------
# Nodos del árbol de condiciones
# ---------------------------------------------------------------------------

class LeafCondition(BaseModel):
    """Condición atómica sobre un campo del contexto de evaluación."""
    type: Literal["leaf"] = "leaf"
    field: str      # ruta al campo, ej: "subject.attr[nif]"
    op: Literal[
        "starts_with", "ends_with",
        "contains", "contains_any", "not_contains_any",
        "matches_pattern",
        "eq", "neq", "gte", "lte", "in", "not_in",
        # Operadores de grafo (requieren consulta al GraphStore)
        "has_confirmed_node_of_type",  # nodo relacionado de tipo confirmado
        "connected_to_confirmed",       # conectado a nodo ya confirmado
        "relation_exists",              # existe relación de tipo dado
    ]
    value: Any


class AndCondition(BaseModel):
    type: Literal["and"] = "and"
    conditions: list["RuleNode"]


class OrCondition(BaseModel):
    type: Literal["or"] = "or"
    conditions: list["RuleNode"]


class NotCondition(BaseModel):
    type: Literal["not"] = "not"
    condition: "RuleNode"


class ThresholdCondition(BaseModel):
    """Al menos min_match de las condiciones deben cumplirse."""
    type: Literal["threshold"] = "threshold"
    min_match: int = Field(ge=1)
    conditions: list["RuleNode"]


RuleNode = Union[
    LeafCondition, AndCondition, OrCondition,
    NotCondition, ThresholdCondition,
]

AndCondition.model_rebuild()
OrCondition.model_rebuild()
NotCondition.model_rebuild()
ThresholdCondition.model_rebuild()


# ---------------------------------------------------------------------------
# Regla
# ---------------------------------------------------------------------------

class Rule(BaseModel):
    """
    Una regla de vigilancia definida por el analista.
    Cuando su condición se cumple, genera una Evidence del nivel indicado.
    """
    id: str
    label: str
    level: EvidenceLevel
    condition: RuleNode

    # Plantilla de narrativa; {field}, {value}, {signal} se sustituyen
    explanation_template: str

    # Tipo de nodo al que aplica esta regla
    applies_to: Literal["actor", "process", "asset", "any"] = "any"

    active: bool = True
    author: str = "analyst"
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# WatchProfile
# ---------------------------------------------------------------------------

class WatchProfile(BaseModel):
    """
    Perfil de vigilancia de una investigación.

    Es la única pieza con semántica de dominio en GrantsSurveyor.
    Todo lo demás (grafo, motor de reglas, evaluador) es genérico.

    Se versiona automáticamente con cada modificación.
    Las evidencias registran la versión activa en el momento
    de su creación para garantizar la reproducibilidad.
    """
    id: str
    investigation_id: str
    version: str                    # semver: "1.0.0", "1.1.0"...
    name: str
    query_description: str          # pregunta de vigilancia en lenguaje natural

    rules: list[Rule] = Field(default_factory=list)

    # Vocabulario de señales léxicas para el nodo sujeto
    # (operan sobre canonical_name e identifiers)
    vocabulary_high: list[str] = Field(default_factory=list)    # nivel 2
    vocabulary_medium: list[str] = Field(default_factory=list)  # nivel 4
    exclusions: list[str] = Field(default_factory=list)

    # Umbrales
    publish_threshold: float = Field(ge=0.0, le=1.0, default=0.85)
    min_review_coverage: float = Field(ge=0.0, le=1.0, default=0.70)

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    author: str = "analyst"
