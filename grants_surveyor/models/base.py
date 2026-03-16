"""
models/base.py — Enumeraciones y tipos compartidos del metadominio.
"""
from enum import Enum


class EvidenceLevel(int, Enum):
    """
    Nivel de certeza de una evidencia o hallazgo.
    Solo niveles 1–3 son publicables tras confirmación humana.
    """
    STRUCTURAL = 1   # NIF R* — irrefutable
    REGISTRAL  = 2   # Match en registro oficial
    NETWORK    = 3   # Red de afiliaciones confirmada
    LEXICAL    = 4   # Señales léxicas sin confirmación registral
    INFERENCE  = 5   # Inferencia débil — requiere investigación


class EvidenceSource(str, Enum):
    """Origen de una evidencia concreta."""
    ODM_DATASET   = "odm_dataset"     # dato de un Dataset de OpenDataManager
    RULE_ENGINE   = "rule_engine"     # activada por el motor de reglas
    LLM_INFERENCE = "llm"             # inferencia del modelo de lenguaje
    HUMAN         = "human"           # introducida directamente por el analista


class FindingStatus(str, Enum):
    """Estado de un hallazgo en el flujo de revisión humana."""
    PENDING      = "pending"
    CONFIRMED    = "confirmed"
    DISCARDED    = "discarded"
    INVESTIGATING = "investigating"


class InvestigationStatus(str, Enum):
    ACTIVE   = "active"
    PAUSED   = "paused"
    ARCHIVED = "archived"


class NodeType(str, Enum):
    """Tipo de nodo en el grafo de conocimiento."""
    ACTOR   = "actor"    # organización, persona, administración...
    PROCESS = "process"  # concesión, contrato, compraventa...
    ASSET   = "asset"    # inmueble, licencia, cargo...
    SOURCE  = "source"   # documento o dataset de origen


class RelationType(str, Enum):
    """Tipo de relación entre nodos."""
    PARTICIPATES_IN = "participates_in"  # Actor → Proceso
    CONTROLS        = "controls"         # Actor → Actor | Activo
    TRANSFERS       = "transfers"        # Proceso transfiere Activo
    SAME_AS         = "same_as"          # resolución de identidad
    OBSERVED_IN     = "observed_in"      # cualquier nodo → Source


class UpgradePolicy(str, Enum):
    """
    Política de auto-upgrade de DatasetSubscription en OpenDataManager.
    Controla qué versiones acepta GrantsSurveyor automáticamente.
    """
    PATCH = "patch"   # solo cambios de datos, mismo schema
    MINOR = "minor"   # campos nuevos añadidos — aceptable
    MAJOR = "major"   # cambio incompatible — requiere revisión humana
    NONE  = "none"    # versión fijada, nunca actualiza solo
