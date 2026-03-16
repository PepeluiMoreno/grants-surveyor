"""
models/graph.py — Metadominio genérico del grafo de conocimiento.

Principio de diseño:
  Ningún nodo tiene campos de dominio fijos.
  Todo es atributos dinámicos con clave, valor, fuente y confianza.
  El mismo sistema investiga subvenciones, inmuebles o contratos
  sin cambios de código — solo cambia el WatchProfile.

Tipos de nodo:
  Actor   — organización, persona, administración, empresa...
  Process — concesión, contrato, compraventa, nombramiento...
  Asset   — inmueble, licencia, cargo, patente...
  Source  — dataset de ODM o documento externo

Tipos de relación:
  PARTICIPATES_IN — Actor participa en Proceso con un rol
  CONTROLS        — Actor controla Actor o Asset
  TRANSFERS       — Proceso transfiere Asset entre Actores
  SAME_AS         — resolución de identidad entre nodos
  OBSERVED_IN     — cualquier nodo fue observado en una Source
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from .base import NodeType, RelationType


# ---------------------------------------------------------------------------
# Atributo dinámico
# ---------------------------------------------------------------------------

class Attribute(BaseModel):
    """
    Propiedad observable de cualquier nodo, con trazabilidad completa.

    Un Actor puede tener múltiples atributos 'nombre' procedentes de
    distintas fuentes, con distintos valores y confianzas.
    """
    key: str                        # ej: "nif", "nombre", "tipo_entidad"
    value: Any                      # str, int, float, bool, list...
    source_id: str                  # FK a Source o dataset_id de ODM
    confidence: float = Field(ge=0.0, le=1.0, default=1.0)
    observed_at: datetime = Field(default_factory=datetime.utcnow)
    # Versión del dataset de ODM del que procede (si aplica)
    odm_dataset_version: str | None = None


# ---------------------------------------------------------------------------
# Nodos
# ---------------------------------------------------------------------------

class Node(BaseModel):
    """Nodo base del grafo. Todos los tipos heredan de aquí."""

    id: str
    project_id: str
    node_type: NodeType

    # Nombre canónico para visualización (derivado del atributo 'nombre')
    canonical_name: str

    # Identificadores conocidos: {"nif": "R1234567A", "rer_id": "...", ...}
    # Clave de búsqueda rápida — no sustituye a los Attributes completos
    identifiers: dict[str, str] = Field(default_factory=dict)

    # Atributos dinámicos con trazabilidad
    attributes: list[Attribute] = Field(default_factory=list)

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    def get_attr(self, key: str) -> list[Attribute]:
        """Devuelve todos los atributos con una clave dada."""
        return [a for a in self.attributes if a.key == key]

    def best_attr(self, key: str) -> Attribute | None:
        """Devuelve el atributo con mayor confianza para una clave."""
        attrs = self.get_attr(key)
        return max(attrs, key=lambda a: a.confidence) if attrs else None

    def attr_value(self, key: str, default: Any = None) -> Any:
        """Valor del atributo de mayor confianza para una clave."""
        attr = self.best_attr(key)
        return attr.value if attr else default


class Actor(Node):
    """
    Cualquier entidad que participa en procesos o posee activos.

    Ejemplos: organización religiosa, ministerio, persona física,
    empresa, fondo de inversión, administración autonómica.

    El 'tipo' de actor (organización, persona, administración)
    es un Attribute con key="actor_type", no un campo fijo.
    """
    node_type: NodeType = NodeType.ACTOR


class Process(Node):
    """
    Cualquier flujo o transacción con actores participantes.

    Ejemplos: concesión de subvención, adjudicación de contrato,
    compraventa de inmueble, nombramiento en junta directiva,
    inscripción registral.

    El 'tipo' de proceso es un Attribute con key="process_type".
    El 'valor económico' es un Attribute con key="amount".
    La 'fecha' es un Attribute con key="date".
    """
    node_type: NodeType = NodeType.PROCESS


class Asset(Node):
    """
    Cualquier bien, derecho o recurso sobre el que actúan los actores.

    Ejemplos: inmueble (ref. catastral), licencia, patente,
    cargo institucional, contrato.

    El 'tipo' de activo es un Attribute con key="asset_type".
    """
    node_type: NodeType = NodeType.ASSET


class Source(Node):
    """
    Documento o dataset del que procede una o más observaciones.
    Garantiza la trazabilidad de toda afirmación del grafo.

    Puede ser:
    - Un Dataset de OpenDataManager (con odm_dataset_id y version)
    - Un documento externo (BOE, web, PDF)
    """
    node_type: NodeType = NodeType.SOURCE

    # Campos específicos de Source (no como Attributes para facilitar queries)
    url: str | None = None
    odm_dataset_id: str | None = None    # UUID del Dataset en ODM
    odm_resource_code: str | None = None # ej: "bdns_concesiones"
    odm_version: str | None = None       # ej: "1.4.0"
    retrieved_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Relaciones
# ---------------------------------------------------------------------------

class Relation(BaseModel):
    """
    Arista dirigida entre dos nodos del grafo.

    El tipo de relación determina la semántica:
    - PARTICIPATES_IN: Actor → Proceso  (con rol como atributo)
    - CONTROLS:        Actor → Actor | Asset
    - TRANSFERS:       Proceso → Asset
    - SAME_AS:         cualquier nodo → mismo nodo en otra fuente
    - OBSERVED_IN:     cualquier nodo → Source

    Toda relación es trazable a su fuente y tiene nivel de confianza.
    Las relaciones SAME_AS sobre personas requieren confirmación
    humana antes de usarse como evidencia publicable.
    """

    id: str
    project_id: str
    investigation_id: str           # privada de cada investigación

    relation_type: RelationType
    source_node_id: str
    target_node_id: str

    # Atributos de la relación (ej: rol en PARTICIPATES_IN)
    attributes: dict[str, Any] = Field(default_factory=dict)

    # Trazabilidad
    source_id: str                  # FK a Source
    confidence: float = Field(ge=0.0, le=1.0, default=1.0)
    confirmed_by_human: bool = False
    author: str = "agent"           # "agent" o id del analista

    created_at: datetime = Field(default_factory=datetime.utcnow)

    # Convenio para SAME_AS: nunca publishable hasta confirmación humana
    @property
    def is_publishable(self) -> bool:
        if self.relation_type == RelationType.SAME_AS:
            return self.confirmed_by_human
        return True
