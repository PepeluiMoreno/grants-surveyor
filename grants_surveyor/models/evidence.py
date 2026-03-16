"""
models/evidence.py — Evidencias y hallazgos.

Una Evidencia es una señal individual sobre un nodo del grafo,
con fuente, nivel de certeza y trazabilidad.

Un Finding agrega evidencias sobre un nodo o conjunto de nodos
y genera una narrativa explicativa para el analista.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from .base import EvidenceLevel, EvidenceSource, FindingStatus


class Evidence(BaseModel):
    """
    Señal individual que contribuye a un hallazgo.

    Cada evidencia apunta a su fuente (Source del grafo o dataset ODM)
    y declara qué observó, con qué nivel de certeza.

    El 'signal' es siempre texto legible por el analista —
    nunca un código interno.
    """
    id: str
    investigation_id: str
    finding_id: str                         # FK al Finding que agrega esta evidencia

    # Qué fuente la generó
    evidence_source: EvidenceSource
    source_id: str                          # FK a Source node en el grafo
    odm_dataset_version: str | None = None  # versión ODM si viene de dataset

    # Qué nivel de certeza aporta
    level: EvidenceLevel

    # Qué observó exactamente — texto legible para el analista
    signal: str

    # Fiabilidad intrínseca del tipo de fuente (0–1)
    weight: float = Field(ge=0.0, le=1.0)
    # Certeza sobre esta señal concreta (puede ser menor que el peso)
    confidence: float = Field(ge=0.0, le=1.0)

    # Trazabilidad adicional
    rule_id: str | None = None              # regla que la activó, si aplica
    watch_profile_version: str | None = None

    # Detalle específico según el tipo (libre, para audit trail)
    detail: dict[str, Any] = Field(default_factory=dict)

    created_at: datetime = Field(default_factory=datetime.utcnow)


class Finding(BaseModel):
    """
    Hallazgo sobre un nodo (o conjunto de nodos) que supera el umbral
    de relevancia de la pregunta de vigilancia.

    Es el objeto central que revisa el analista en el Workbench.

    Invariante de publicación:
      publishable = True  solo si:
        - max_level.value <= 2
        - confidence_aggregated >= watch_profile.publish_threshold
        - status == CONFIRMED  (confirmación humana explícita)
    """
    id: str
    investigation_id: str

    # Nodo principal del hallazgo (Actor, Proceso o Asset)
    subject_node_id: str
    subject_node_type: str              # "actor" | "process" | "asset"
    subject_canonical_name: str         # para display sin consultar el grafo

    # Nodos relacionados involucrados en el hallazgo
    related_node_ids: list[str] = Field(default_factory=list)

    # Evidencias
    evidences: list[Evidence] = Field(default_factory=list)

    # Agregados calculados por EvidenceAggregator
    confidence_aggregated: float = Field(ge=0.0, le=1.0, default=0.0)
    max_level: EvidenceLevel = EvidenceLevel.INFERENCE

    # Narrativa generada: primero reglas deterministas, luego LLM
    narrative: str = ""

    # Flujo de revisión
    status: FindingStatus = FindingStatus.PENDING
    requires_human_review: bool = True
    publishable: bool = False           # False hasta confirmación humana

    # Revisión humana
    confirmed_by: str | None = None
    confirmed_at: datetime | None = None
    analyst_notes: str | None = None

    # Metadatos
    watch_profile_version: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
