"""
agent/evaluator.py — Evaluador con IA sobre el metadominio genérico.

Tres componentes:

  Evaluator       — evalúa un nodo del grafo y genera un Finding
  Refiner         — propone mejoras al WatchProfile tras revisión humana
  SourceDiscovery — rastrea fuentes relevantes para una investigación

Todas las funciones son async. El llamador usa utils.async_runner.run_sync()
para ejecutar desde Jupyter o código síncrono.

El LLM nunca es la única fuente de evidencia.
El motor de reglas siempre va primero y es determinista.
"""
from __future__ import annotations

import json
import logging
import re
import uuid
from pathlib import Path
from typing import Any

import anthropic
from jinja2 import Environment, FileSystemLoader

from ..engine.rule_engine import (
    EvalContext,
    EvidenceAggregator,
    RuleEngine,
    RuleMatch,
)
from ..models import (
    Actor,
    Asset,
    Attribute,
    Evidence,
    EvidenceLevel,
    EvidenceSource,
    Finding,
    FindingStatus,
    Process,
    Source,
    WatchProfile,
)

logger = logging.getLogger(__name__)

PROMPTS_DIR = Path(__file__).parent / "prompts"
MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 1024


def _get_anthropic_client() -> anthropic.Anthropic:
    """Crea el cliente Anthropic. La API key viene de ANTHROPIC_API_KEY en .env."""
    return anthropic.Anthropic()


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

class Evaluator:
    """
    Evalúa un nodo del grafo contra un WatchProfile y produce un Finding.

    Pipeline:
      1. RuleEngine.evaluate()         → evidencias deterministas
      2. RuleEngine.evaluate_lexical() → vocabulario
      3. Claude (si ambiguo)           → narrativa + inferencia adicional
      4. EvidenceAggregator            → confianza final
      5. Finding                       → objeto para el Workbench
    """

    def __init__(
        self,
        profile: WatchProfile,
        anthropic_client: anthropic.Anthropic | None = None,
        skip_llm_if_certain: bool = True,
    ) -> None:
        self.profile = profile
        self.engine = RuleEngine(profile)
        self.client = anthropic_client or _get_anthropic_client()
        self.skip_llm_if_certain = skip_llm_if_certain
        self._jinja = Environment(
            loader=FileSystemLoader(str(PROMPTS_DIR)),
            autoescape=False,
        )

    async def evaluate(
        self,
        ctx: EvalContext,
        investigation_id: str,
        source_id: str,
    ) -> Finding | None:
        """
        Evalúa un nodo y devuelve un Finding, o None si no hay señales.

        ctx.subject es el nodo a evaluar (Actor, Process o Asset).
        source_id es el ID del nodo Source que originó este dato en el grafo.
        """
        # 1 + 2. Motor de reglas (determinista, sin IO)
        rule_matches = self.engine.evaluate(ctx)
        lexical_matches = self.engine.evaluate_lexical(ctx)
        all_matches = rule_matches + lexical_matches

        if not all_matches and not ctx.has_confirmed_connection:
            return None

        # Convierte matches a evidencias
        evidences: list[Evidence] = self.engine.to_evidences(
            all_matches,
            subject_node_id=ctx.subject.id,
            investigation_id=investigation_id,
            source_id=source_id,
            watch_profile_version=self.profile.version,
        )

        # 3. Claude: solo si hay señales y el caso no es certeza máxima
        llm_result: dict[str, Any] | None = None
        if self._should_call_llm(all_matches):
            try:
                llm_result = await self._call_claude_evaluate(ctx, all_matches)
                if llm_result:
                    llm_ev = self._llm_to_evidence(
                        llm_result, ctx.subject.id, investigation_id, source_id
                    )
                    if llm_ev:
                        evidences.append(llm_ev)
            except Exception as e:
                logger.error(
                    "Error llamando a Claude para nodo %s: %s",
                    ctx.subject.canonical_name, e
                )

        # 4. Agrega evidencias
        confidence, max_level = EvidenceAggregator.aggregate(evidences)

        # 5. Construye el Finding
        return self._build_finding(
            ctx=ctx,
            investigation_id=investigation_id,
            evidences=evidences,
            confidence=confidence,
            max_level=max_level,
            llm_result=llm_result,
            rule_matches=all_matches,
        )

    def _should_call_llm(self, matches: list[RuleMatch]) -> bool:
        if not matches:
            return True
        max_level = min(m.level for m in matches)
        if self.skip_llm_if_certain and max_level == EvidenceLevel.STRUCTURAL:
            return False
        return True

    async def _call_claude_evaluate(
        self,
        ctx: EvalContext,
        rule_matches: list[RuleMatch],
    ) -> dict[str, Any] | None:
        template = self._jinja.get_template("evaluate.j2")
        prompt = template.render(
            profile=self.profile,
            subject=ctx.subject,
            rule_matches=rule_matches,
            related_confirmed=[
                ctx.related_nodes[nid]
                for nid in ctx.confirmed_related_ids
                if nid in ctx.related_nodes
            ],
            relations=ctx.relations,
        )

        import asyncio
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: self.client.messages.create(
                model=MODEL,
                max_tokens=MAX_TOKENS,
                messages=[{"role": "user", "content": prompt}],
            )
        )
        return _parse_json(response.content[0].text)

    def _llm_to_evidence(
        self,
        result: dict[str, Any],
        subject_node_id: str,
        investigation_id: str,
        source_id: str,
    ) -> Evidence | None:
        if not result.get("relevant"):
            return None
        try:
            level = EvidenceLevel(int(result.get("level", 5)))
        except (ValueError, TypeError):
            level = EvidenceLevel.INFERENCE

        return Evidence(
            id=str(uuid.uuid4()),
            investigation_id=investigation_id,
            finding_id="",
            evidence_source=EvidenceSource.LLM_INFERENCE,
            source_id=source_id,
            level=level,
            signal=result.get("narrative", ""),
            weight=0.65,
            confidence=float(result.get("confidence", 0.3)),
            watch_profile_version=self.profile.version,
            detail={
                "signals": result.get("signals", []),
                "analyst_recommendation": result.get("analyst_recommendation"),
                "llm_publishable": result.get("publishable", False),
            },
        )

    def _build_finding(
        self,
        ctx: EvalContext,
        investigation_id: str,
        evidences: list[Evidence],
        confidence: float,
        max_level: EvidenceLevel,
        llm_result: dict[str, Any] | None,
        rule_matches: list[RuleMatch],
    ) -> Finding:
        finding_id = str(uuid.uuid4())

        # Asigna el finding_id a todas las evidencias
        for ev in evidences:
            ev.finding_id = finding_id

        # Narrativa: reglas deterministas primero, LLM después
        if llm_result and llm_result.get("narrative"):
            narrative = _build_narrative_with_llm(
                llm_result, rule_matches, max_level, confidence
            )
        else:
            narrative = _build_narrative_from_rules(
                rule_matches, max_level, confidence
            )

        return Finding(
            id=finding_id,
            investigation_id=investigation_id,
            subject_node_id=ctx.subject.id,
            subject_node_type=ctx.subject.node_type.value,
            subject_canonical_name=ctx.subject.canonical_name,
            related_node_ids=list(ctx.related_nodes.keys()),
            evidences=evidences,
            confidence_aggregated=confidence,
            max_level=max_level,
            narrative=narrative,
            status=FindingStatus.PENDING,
            requires_human_review=(
                max_level.value >= 3
                or confidence < self.profile.publish_threshold
            ),
            publishable=False,  # Nunca hasta confirmación humana
            watch_profile_version=self.profile.version,
        )


# ---------------------------------------------------------------------------
# Refiner
# ---------------------------------------------------------------------------

class Refiner:
    """
    Analiza discrepancias del ciclo y propone mejoras al WatchProfile.
    Las propuestas son sugerencias — el analista las acepta desde la UI.
    """

    def __init__(
        self,
        profile: WatchProfile,
        anthropic_client: anthropic.Anthropic | None = None,
    ) -> None:
        self.profile = profile
        self.client = anthropic_client or _get_anthropic_client()
        self._jinja = Environment(
            loader=FileSystemLoader(str(PROMPTS_DIR)),
            autoescape=False,
        )

    async def propose_refinements(
        self,
        cycle_summary: dict[str, Any],
        false_positives: list[dict[str, Any]],
        false_negatives: list[dict[str, Any]],
        investigating: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        coverage = cycle_summary.get("review_coverage", 0.0)
        if coverage < self.profile.min_review_coverage:
            logger.warning(
                "Cobertura %.0f%% < mínimo %.0f%%. Refiner no ejecutado.",
                coverage * 100, self.profile.min_review_coverage * 100,
            )
            return {
                "skipped": True,
                "reason": (
                    f"Cobertura {coverage:.0%} < "
                    f"mínimo {self.profile.min_review_coverage:.0%}"
                ),
                "cycle_notes": (
                    f"El analista debe revisar al menos el "
                    f"{self.profile.min_review_coverage:.0%} de los hallazgos."
                ),
            }

        template = self._jinja.get_template("refine.j2")
        prompt = template.render(
            profile=self.profile,
            cycle=cycle_summary,
            false_positives=false_positives,
            false_negatives=false_negatives,
            investigating=investigating,
        )

        import asyncio
        loop = asyncio.get_event_loop()
        try:
            response = await loop.run_in_executor(
                None,
                lambda: self.client.messages.create(
                    model=MODEL,
                    max_tokens=MAX_TOKENS,
                    messages=[{"role": "user", "content": prompt}],
                )
            )
            return _parse_json(response.content[0].text)
        except Exception as e:
            logger.error("Error en Refiner: %s", e)
            return None


# ---------------------------------------------------------------------------
# SourceDiscovery
# ---------------------------------------------------------------------------

class SourceDiscovery:
    """
    Rastrea fuentes de datos potencialmente relevantes para una investigación
    y evalúa su estado respecto a OpenDataManager.

    Implementa el requisito del documento de arquitectura:
    «El agente rastrea activamente todas las fuentes de información
    que podrían ser pertinentes para la pregunta de vigilancia.»
    """

    def __init__(
        self,
        anthropic_client: anthropic.Anthropic | None = None,
    ) -> None:
        self.client = anthropic_client or _get_anthropic_client()
        self._jinja = Environment(
            loader=FileSystemLoader(str(PROMPTS_DIR)),
            autoescape=False,
        )

    async def discover(
        self,
        query_description: str,
        available_resources: list[dict[str, Any]],
        fetcher_types: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        """
        Dado una query_description y los resources/fetchers disponibles en ODM,
        devuelve un inventario de fuentes clasificadas en cuatro categorías:
          available        — ya existe en ODM
          needs_config     — fetcher existe, resource no
          needs_fetcher    — fuente pública sin fetcher
          not_accessible   — sin acceso programable
        """
        template = self._jinja.get_template("source_discovery.j2")
        prompt = template.render(
            query_description=query_description,
            available_resources=available_resources,
            fetcher_types=fetcher_types,
        )

        import asyncio
        loop = asyncio.get_event_loop()
        try:
            response = await loop.run_in_executor(
                None,
                lambda: self.client.messages.create(
                    model=MODEL,
                    max_tokens=2048,
                    messages=[{"role": "user", "content": prompt}],
                )
            )
            result = _parse_json(response.content[0].text)
            if result:
                # Clasifica las fuentes por categoría para facilitar el display
                result["by_category"] = _group_by_category(
                    result.get("sources", [])
                )
            return result
        except Exception as e:
            logger.error("Error en SourceDiscovery: %s", e)
            return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_json(text: str) -> dict[str, Any] | None:
    """Extrae y parsea el JSON de la respuesta del LLM."""
    text = text.strip()
    # Elimina bloques de código markdown si los hay
    if text.startswith("```"):
        text = "\n".join(
            l for l in text.split("\n")
            if not l.startswith("```")
        )
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        logger.warning("No se pudo parsear JSON del LLM: %s", text[:200])
        return None


def _build_narrative_with_llm(
    llm_result: dict[str, Any],
    rule_matches: list[RuleMatch],
    max_level: EvidenceLevel,
    confidence: float,
) -> str:
    lines = [f"**Certeza {confidence:.0%} — Nivel {max_level.value}**\n"]
    for m in sorted(rule_matches, key=lambda x: x.level.value):
        lines.append(f"[NIVEL {m.level.value}] {m.explanation}")
    if narrative := llm_result.get("narrative"):
        lines.append(f"\n*Análisis adicional:* {narrative}")
    return "\n".join(lines)


def _build_narrative_from_rules(
    rule_matches: list[RuleMatch],
    max_level: EvidenceLevel,
    confidence: float,
) -> str:
    if not rule_matches:
        return f"Sin evidencias significativas (confianza: {confidence:.0%})."
    lines = [f"**Certeza {confidence:.0%} — Nivel {max_level.value}**\n"]
    for m in sorted(rule_matches, key=lambda x: x.level.value):
        lines.append(f"[NIVEL {m.level.value}] {m.explanation}")
    return "\n".join(lines)


def _group_by_category(
    sources: list[dict[str, Any]]
) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list] = {
        "available": [],
        "needs_config": [],
        "needs_fetcher": [],
        "not_accessible": [],
    }
    for s in sources:
        cat = s.get("category", "not_accessible")
        if cat in groups:
            groups[cat].append(s)
    return groups
