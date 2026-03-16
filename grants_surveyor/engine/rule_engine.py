"""
engine/rule_engine.py — Motor de reglas booleanas sobre el metadominio.

Evalúa el árbol de condiciones de cada Rule del WatchProfile
contra un EvalContext que contiene un nodo del grafo y sus datos.

El motor es completamente determinista y auditable.
El LLM enriquece la narrativa después, en agent/evaluator.py.
Nunca llama a APIs externas.

Rutas de campo soportadas:
  subject.identifier[nif]          — identificador del nodo sujeto
  subject.attr[nombre]             — atributo del nodo sujeto
  subject.canonical_name           — nombre canónico
  subject.node_type                — tipo de nodo
  relation.attr[role]              — atributo de una relación
  related.attr[actor_type]         — atributo de nodo relacionado
  related.confirmed                — nodo relacionado confirmado en otra investigación
  graph.has_confirmed_connection   — tiene conexión confirmada en el grafo
"""
from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ..models import (
    Actor,
    AndCondition,
    Asset,
    Attribute,
    Evidence,
    EvidenceLevel,
    EvidenceSource,
    LeafCondition,
    NotCondition,
    OrCondition,
    Process,
    Relation,
    Rule,
    RuleNode,
    Source,
    ThresholdCondition,
    WatchProfile,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Contexto de evaluación
# ---------------------------------------------------------------------------

@dataclass
class EvalContext:
    """
    Datos disponibles para evaluar una regla sobre un nodo.

    Se construye una vez por nodo y se reutiliza para todas las reglas.
    El Enricher lo puebla antes de pasarlo al RuleEngine.
    """
    # Nodo principal que se está evaluando
    subject: Actor | Process | Asset | Source

    # Relaciones del nodo en el grafo de la investigación
    relations: list[Relation] = field(default_factory=list)

    # Nodos relacionados (precargados para evitar queries en la evaluación)
    related_nodes: dict[str, Actor | Process | Asset | Source] = field(
        default_factory=dict
    )

    # ¿Tiene alguna conexión confirmada en el grafo del proyecto?
    has_confirmed_connection: bool = False

    # IDs de nodos relacionados ya confirmados en cualquier investigación
    confirmed_related_ids: list[str] = field(default_factory=list)

    def get_field(self, path: str) -> Any:
        """
        Resuelve un campo por su ruta.

        Rutas soportadas:
          subject.identifier[key]       → identifiers[key]
          subject.attr[key]             → mejor atributo con esa clave
          subject.canonical_name        → nombre canónico
          subject.node_type             → tipo de nodo como string
          graph.has_confirmed_connection → bool
          graph.confirmed_related_count  → int
          relation[type].count          → nº de relaciones de ese tipo
          relation[type].attr[key]      → atributo de la primera relación del tipo
        """
        # subject.identifier[key]
        m = re.match(r'^subject\.identifier\[(.+)\]$', path)
        if m:
            return self.subject.identifiers.get(m.group(1))

        # subject.attr[key]
        m = re.match(r'^subject\.attr\[(.+)\]$', path)
        if m:
            attr = self.subject.best_attr(m.group(1))
            return attr.value if attr else None

        # subject.canonical_name
        if path == "subject.canonical_name":
            return self.subject.canonical_name

        # subject.node_type
        if path == "subject.node_type":
            return self.subject.node_type.value

        # graph.has_confirmed_connection
        if path == "graph.has_confirmed_connection":
            return self.has_confirmed_connection

        # graph.confirmed_related_count
        if path == "graph.confirmed_related_count":
            return len(self.confirmed_related_ids)

        # relation[type].count
        m = re.match(r'^relation\[(.+)\]\.count$', path)
        if m:
            rel_type = m.group(1)
            return sum(
                1 for r in self.relations
                if r.relation_type.value == rel_type
            )

        # relation[type].attr[key]
        m = re.match(r'^relation\[(.+)\]\.attr\[(.+)\]$', path)
        if m:
            rel_type, attr_key = m.group(1), m.group(2)
            for r in self.relations:
                if r.relation_type.value == rel_type:
                    return r.attributes.get(attr_key)
            return None

        # related[node_id].attr[key] — atributo de nodo relacionado específico
        m = re.match(r'^related\[(.+)\]\.attr\[(.+)\]$', path)
        if m:
            node_id, attr_key = m.group(1), m.group(2)
            node = self.related_nodes.get(node_id)
            if node:
                attr = node.best_attr(attr_key)
                return attr.value if attr else None
            return None

        # related.confirmed — ¿algún nodo relacionado está confirmado?
        if path == "related.confirmed":
            return bool(self.confirmed_related_ids)

        logger.debug("Campo desconocido en EvalContext: %s", path)
        return None


# ---------------------------------------------------------------------------
# Resultado de evaluación de una regla
# ---------------------------------------------------------------------------

@dataclass
class RuleMatch:
    """Una regla que se ha activado para un nodo."""
    rule: Rule
    level: EvidenceLevel
    explanation: str
    confidence: float
    matched_signals: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Motor de reglas
# ---------------------------------------------------------------------------

class RuleEngine:
    """
    Evalúa todas las reglas activas del WatchProfile contra un EvalContext.

    Uso:
        engine = RuleEngine(watch_profile)
        matches = engine.evaluate(context)
        evidences = engine.to_evidences(matches, subject_node_id, investigation_id)
    """

    def __init__(self, profile: WatchProfile) -> None:
        self.profile = profile
        self._active_rules = [r for r in profile.rules if r.active]

    def evaluate(self, ctx: EvalContext) -> list[RuleMatch]:
        """Evalúa todas las reglas activas. Devuelve solo las que se activan."""
        matches: list[RuleMatch] = []
        for rule in self._active_rules:
            # Filtra por tipo de nodo si la regla lo especifica
            if (rule.applies_to != "any"
                    and rule.applies_to != ctx.subject.node_type.value):
                continue
            try:
                matched, signals = self._eval_node(rule.condition, ctx)
                if matched:
                    explanation = self._render_explanation(
                        rule.explanation_template, ctx, signals
                    )
                    matches.append(RuleMatch(
                        rule=rule,
                        level=rule.level,
                        explanation=explanation,
                        confidence=_level_to_confidence(rule.level),
                        matched_signals=signals,
                    ))
            except Exception as e:
                logger.error("Error evaluando regla '%s': %s", rule.id, e)
        return matches

    def evaluate_lexical(self, ctx: EvalContext) -> list[RuleMatch]:
        """
        Evalúa el vocabulario del WatchProfile contra el nombre canónico
        y los atributos de texto del nodo sujeto.
        """
        matches: list[RuleMatch] = []

        # Texto a evaluar: nombre canónico + atributos de texto relevantes
        text_parts = [ctx.subject.canonical_name]
        for attr_key in ("nombre", "name", "descripcion", "description", "objeto"):
            attr = ctx.subject.best_attr(attr_key)
            if attr and isinstance(attr.value, str):
                text_parts.append(attr.value)
        text = " ".join(text_parts).upper()

        # Comprueba exclusiones primero
        for excl in self.profile.exclusions:
            if excl.upper() in text:
                logger.debug(
                    "Exclusión '%s' activa para '%s' — sin señales léxicas.",
                    excl, ctx.subject.canonical_name
                )
                return []

        # Vocabulario de alta certeza → nivel REGISTRAL
        high_hits = [t for t in self.profile.vocabulary_high if t.upper() in text]
        if high_hits:
            matches.append(RuleMatch(
                rule=_synthetic_rule("vocab_high", EvidenceLevel.REGISTRAL),
                level=EvidenceLevel.REGISTRAL,
                explanation=(
                    f"El nombre o descripción contiene términos de alta asociación: "
                    f"{', '.join(high_hits)}."
                ),
                confidence=_level_to_confidence(EvidenceLevel.REGISTRAL),
                matched_signals=high_hits,
            ))

        # Vocabulario de certeza media → nivel LEXICAL
        medium_hits = [t for t in self.profile.vocabulary_medium if t.upper() in text]
        if medium_hits:
            matches.append(RuleMatch(
                rule=_synthetic_rule("vocab_medium", EvidenceLevel.LEXICAL),
                level=EvidenceLevel.LEXICAL,
                explanation=(
                    f"El nombre o descripción contiene términos de asociación media: "
                    f"{', '.join(medium_hits)}."
                ),
                confidence=_level_to_confidence(EvidenceLevel.LEXICAL),
                matched_signals=medium_hits,
            ))

        return matches

    def to_evidences(
        self,
        matches: list[RuleMatch],
        subject_node_id: str,
        investigation_id: str,
        source_id: str,
        watch_profile_version: str,
    ) -> list[Evidence]:
        """Convierte RuleMatches en Evidence objects para el grafo."""
        evidences = []
        for m in matches:
            ev_source = (
                EvidenceSource.RULE_ENGINE
                if not m.rule.id.startswith("vocab_")
                else EvidenceSource.RULE_ENGINE
            )
            evidences.append(Evidence(
                id=str(uuid.uuid4()),
                investigation_id=investigation_id,
                finding_id="",          # se asigna al construir el Finding
                evidence_source=ev_source,
                source_id=source_id,
                level=m.level,
                signal=m.explanation,
                weight=_level_to_weight(m.level),
                confidence=m.confidence,
                rule_id=m.rule.id,
                watch_profile_version=watch_profile_version,
                detail={"matched_signals": m.matched_signals},
            ))
        return evidences

    # ------------------------------------------------------------------
    # Evaluación recursiva del árbol
    # ------------------------------------------------------------------

    def _eval_node(
        self, node: RuleNode, ctx: EvalContext
    ) -> tuple[bool, list[str]]:
        if isinstance(node, LeafCondition):
            return self._eval_leaf(node, ctx)

        if isinstance(node, AndCondition):
            all_signals: list[str] = []
            for child in node.conditions:
                result, signals = self._eval_node(child, ctx)
                if not result:
                    return False, []
                all_signals.extend(signals)
            return True, all_signals

        if isinstance(node, OrCondition):
            for child in node.conditions:
                result, signals = self._eval_node(child, ctx)
                if result:
                    return True, signals
            return False, []

        if isinstance(node, NotCondition):
            result, _ = self._eval_node(node.condition, ctx)
            return not result, []

        if isinstance(node, ThresholdCondition):
            matched_signals: list[str] = []
            count = 0
            for child in node.conditions:
                result, signals = self._eval_node(child, ctx)
                if result:
                    count += 1
                    matched_signals.extend(signals)
            return count >= node.min_match, matched_signals if count >= node.min_match else []

        logger.error("Tipo de nodo desconocido: %s", type(node))
        return False, []

    def _eval_leaf(
        self, leaf: LeafCondition, ctx: EvalContext
    ) -> tuple[bool, list[str]]:
        value = ctx.get_field(leaf.field)
        op = leaf.op
        expected = leaf.value
        signal = f"{leaf.field} {op} {expected}"

        # Normaliza para comparación de strings
        val_str = str(value).upper() if value is not None else ""

        try:
            if op == "starts_with":
                result = isinstance(value, str) and val_str.startswith(
                    str(expected).upper()
                )
            elif op == "ends_with":
                result = isinstance(value, str) and val_str.endswith(
                    str(expected).upper()
                )
            elif op == "contains":
                result = str(expected).upper() in val_str
            elif op == "contains_any":
                terms = _to_list(expected)
                result = any(t.upper() in val_str for t in terms)
                if result:
                    signal = next(t for t in terms if t.upper() in val_str)
            elif op == "not_contains_any":
                terms = _to_list(expected)
                result = not any(t.upper() in val_str for t in terms)
            elif op == "matches_pattern":
                result = bool(
                    re.search(str(expected), str(value or ""), re.IGNORECASE)
                )
            elif op == "eq":
                result = str(value) == str(expected)
            elif op == "neq":
                result = str(value) != str(expected)
            elif op == "gte":
                result = float(value or 0) >= float(expected)
            elif op == "lte":
                result = float(value or 0) <= float(expected)
            elif op == "in":
                result = value in _to_list(expected)
            elif op == "not_in":
                result = value not in _to_list(expected)
            elif op == "has_confirmed_node_of_type":
                result = any(
                    n.node_type.value == str(expected)
                    for n in ctx.related_nodes.values()
                    if n.id in ctx.confirmed_related_ids
                )
            elif op == "connected_to_confirmed":
                result = ctx.has_confirmed_connection
            elif op == "relation_exists":
                result = any(
                    r.relation_type.value == str(expected)
                    for r in ctx.relations
                )
            else:
                logger.warning("Operador desconocido: %s", op)
                result = False

        except (TypeError, ValueError) as e:
            logger.debug(
                "Error evaluando hoja %s %s %s: %s", leaf.field, op, expected, e
            )
            result = False

        return result, [signal] if result else []

    @staticmethod
    def _render_explanation(
        template: str, ctx: EvalContext, signals: list[str]
    ) -> str:
        try:
            return template.format(
                canonical_name=ctx.subject.canonical_name,
                node_type=ctx.subject.node_type.value,
                signals=", ".join(signals),
                nif=ctx.subject.identifiers.get("nif", "—"),
                nombre=ctx.subject.attr_value("nombre", "—"),
            )
        except KeyError:
            return template


# ---------------------------------------------------------------------------
# Agregador de evidencias
# ---------------------------------------------------------------------------

class EvidenceAggregator:
    """
    Combina un conjunto de evidencias en confianza agregada y nivel máximo.

    Estrategia:
      - Nivel máximo = la evidencia de menor valor numérico (más certeza)
      - Confianza base = max(confidence * weight) de todas las evidencias
      - Bonus por convergencia: cada fuente independiente adicional suma 0.05
      - Resultado siempre acotado a [0.0, 1.0]
    """

    @staticmethod
    def aggregate(evidences: list[Evidence]) -> tuple[float, EvidenceLevel]:
        if not evidences:
            return 0.0, EvidenceLevel.INFERENCE

        max_level = min(e.level for e in evidences)
        base = max(e.confidence * e.weight for e in evidences)

        sources_seen: set[str] = set()
        bonus = 0.0
        for e in sorted(evidences, key=lambda x: x.confidence, reverse=True):
            src = e.evidence_source.value
            if src not in sources_seen:
                sources_seen.add(src)
                if len(sources_seen) > 1:
                    bonus += 0.05 * e.confidence

        return min(1.0, round(base + bonus, 3)), max_level


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _level_to_confidence(level: EvidenceLevel) -> float:
    return {
        EvidenceLevel.STRUCTURAL: 0.97,
        EvidenceLevel.REGISTRAL:  0.87,
        EvidenceLevel.NETWORK:    0.70,
        EvidenceLevel.LEXICAL:    0.50,
        EvidenceLevel.INFERENCE:  0.30,
    }[level]


def _level_to_weight(level: EvidenceLevel) -> float:
    return {
        EvidenceLevel.STRUCTURAL: 1.00,
        EvidenceLevel.REGISTRAL:  0.90,
        EvidenceLevel.NETWORK:    0.75,
        EvidenceLevel.LEXICAL:    0.55,
        EvidenceLevel.INFERENCE:  0.35,
    }[level]


def _to_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    return [value]


def _synthetic_rule(rule_id: str, level: EvidenceLevel) -> Rule:
    """Regla sintética para evidencias generadas por el vocabulario."""
    from ..models import LeafCondition
    return Rule(
        id=rule_id,
        label=rule_id,
        level=level,
        condition=LeafCondition(field="_synthetic", op="eq", value=True),
        explanation_template="",
        active=True,
        author="system",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
