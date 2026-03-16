"""
models/projects.py — Proyectos, investigaciones y gestión de ciclos.

Jerarquía:
  Project        → contenedor organizativo
    Investigation → unidad de trabajo con agente propio
      WatchProfile → las reglas y vocabulario del agente
      ODMSubscription[] → fuentes de datos suscritas en ODM
      CycleSummary[]    → historial de ejecuciones
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from .base import InvestigationStatus
from .odm import ODMSubscription
from .rules import WatchProfile


class InvestigationMetrics(BaseModel):
    """Métricas acumuladas de una investigación a lo largo de sus ciclos."""
    total_nodes_created: int = 0
    total_findings_generated: int = 0
    total_confirmed: int = 0
    total_discarded: int = 0
    total_investigating: int = 0

    # Calculadas con scikit-learn sobre casos revisados
    precision: float | None = None
    recall: float | None = None
    f1: float | None = None

    findings_by_level: dict[int, int] = Field(default_factory=dict)
    last_updated: datetime = Field(default_factory=datetime.utcnow)


class CycleSummary(BaseModel):
    """Resumen de una ejecución (ciclo) de una investigación."""
    id: str
    investigation_id: str
    cycle_number: int

    started_at: datetime
    finished_at: datetime | None = None

    # Por recurso ODM procesado en este ciclo
    odm_events_received: int = 0
    nodes_created: int = 0
    nodes_updated: int = 0
    findings_generated: int = 0
    findings_reviewed: int = 0
    review_coverage: float = 0.0

    watch_profile_version: str | None = None
    warnings: list[str] = Field(default_factory=list)


class Investigation(BaseModel):
    """
    Unidad de trabajo de GrantsSurveyor.

    Tiene su propio agente (WatchProfile), sus fuentes de datos
    suscritas en OpenDataManager, y su historial de ciclos.
    Pertenece exactamente a un Project.

    El grafo de conocimiento (nodos y relaciones) es compartido
    entre investigaciones del mismo proyecto, pero los findings
    y las evidencias son privados de cada investigación.
    """
    id: str
    project_id: str
    name: str
    description: str = ""
    query_description: str          # la pregunta de vigilancia

    watch_profile: WatchProfile

    # Fuentes de datos suscritas en OpenDataManager
    odm_subscriptions: list[ODMSubscription] = Field(default_factory=list)

    cycles: list[CycleSummary] = Field(default_factory=list)
    metrics: InvestigationMetrics = Field(default_factory=InvestigationMetrics)
    status: InvestigationStatus = InvestigationStatus.ACTIVE

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @property
    def current_cycle_number(self) -> int:
        return len(self.cycles) + 1

    @property
    def last_cycle(self) -> CycleSummary | None:
        return self.cycles[-1] if self.cycles else None

    def get_subscription(self, resource_code: str) -> ODMSubscription | None:
        return next(
            (s for s in self.odm_subscriptions if s.resource_code == resource_code),
            None
        )

    def active_subscriptions(self) -> list[ODMSubscription]:
        return [s for s in self.odm_subscriptions if s.active]


class Project(BaseModel):
    """
    Contenedor organizativo de investigaciones.

    Las investigaciones de un mismo proyecto comparten el grafo
    de conocimiento (nodos Actor, Process, Asset) pero mantienen
    findings, evidencias y reglas completamente separados.
    """
    id: str                         # slug único: "vigilancia-2026"
    name: str
    description: str = ""

    investigation_ids: list[str] = Field(default_factory=list)

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Exportación de agente
# ---------------------------------------------------------------------------

class AgentExport(BaseModel):
    """
    Artefacto exportable de una investigación (.gsagent).

    Contiene el WatchProfile + historial + métricas.
    NO contiene datos del grafo (nodos, relaciones, findings).
    Es transferible entre organizaciones sin restricciones.
    """
    format_version: str = "2.0"     # v2 = metadominio genérico
    exported_at: datetime = Field(default_factory=datetime.utcnow)
    source_investigation_name: str
    source_project_name: str

    watch_profile: WatchProfile
    cycles_summary: list[CycleSummary] = Field(default_factory=list)
    metrics: InvestigationMetrics = Field(default_factory=InvestigationMetrics)
    export_notes: str = ""

    # Fuentes recomendadas (resource_codes de ODM) para esta investigación
    recommended_odm_resources: list[str] = Field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {
            "name": self.source_investigation_name,
            "project": self.source_project_name,
            "format_version": self.format_version,
            "exported_at": self.exported_at.isoformat(),
            "cycles": len(self.cycles_summary),
            "rules": len(self.watch_profile.rules),
            "confirmed_findings": self.metrics.total_confirmed,
            "precision": self.metrics.precision,
            "f1": self.metrics.f1,
            "recommended_resources": self.recommended_odm_resources,
        }


# ---------------------------------------------------------------------------
# Tareas de investigación y alertas
# ---------------------------------------------------------------------------

class InvestigationTask(BaseModel):
    """
    Tarea de investigación manual creada cuando el analista
    selecciona 'Investigar' en el Workbench.
    """
    id: str
    investigation_id: str
    finding_id: str
    subject_node_id: str
    subject_name: str

    title: str
    description: str = ""
    analyst_notes: str = ""

    status: str = "open"    # open | in_progress | resolved | cancelled
    created_by: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    resolved_at: datetime | None = None
    resolution: str | None = None       # confirmed | discarded | inconclusive
    resolution_notes: str = ""


class Alert(BaseModel):
    """
    Alerta activa: notifica al analista cuando un nodo ya confirmado
    aparece en nuevos datos de OpenDataManager.
    """
    id: str
    investigation_id: str
    subject_node_id: str
    subject_name: str

    # resource_codes de ODM que disparan esta alerta
    watch_resource_codes: list[str] = Field(default_factory=list)

    active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    triggers: list[dict[str, Any]] = Field(default_factory=list)
