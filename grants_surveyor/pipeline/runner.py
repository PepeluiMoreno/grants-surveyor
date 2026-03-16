"""
pipeline/runner.py — Orquestador principal del ciclo de investigación.

El Runner implementa PipelineHandler del webhook receiver y coordina
todas las fases del pipeline cuando llegan datos de OpenDataManager.

Fases por evento ODM:
  1. VALIDATE   — verifica compatibilidad de schema (bloquea en major upgrade)
  2. FETCH      — descarga registros del dataset via ODMClient
  3. NORMALIZE  — mapea campos ODM al metadominio (Actor/Process/Asset)
  4. ENRICH     — construye EvalContext con relaciones del grafo
  5. EVALUATE   — motor de reglas + LLM → Finding
  6. STORE      — persiste nodos, relaciones y findings en GraphStore
  7. PROPAGATE  — irradia desde nodos confirmados hacia relacionados
  8. ALERT      — dispara alertas para nodos confirmados con nuevos datos

Uso desde Jupyter (sync):
    from grants_surveyor.utils.async_runner import run_sync
    runner = Runner(project_id="...", investigation_id="...")
    run_sync(runner.handle_odm_event(event))

Uso desde código async:
    await runner.handle_odm_event(event)
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable

import anthropic

from ..agent.evaluator import Evaluator, Refiner, SourceDiscovery
from ..engine.rule_engine import EvalContext
from ..graph.store import GraphStore
from ..models import (
    Actor,
    Alert,
    Asset,
    Attribute,
    CycleSummary,
    Evidence,
    EvidenceLevel,
    Finding,
    FindingStatus,
    Investigation,
    InvestigationMetrics,
    InvestigationStatus,
    NodeType,
    ODMSubscription,
    ODMWebhookEvent,
    Process,
    Project,
    Relation,
    RelationType,
    Source,
    UpgradePolicy,
    WatchProfile,
)
from ..odm.client import ODMClient, ODMNormalizer
from ..odm.webhook_receiver import PipelineHandler

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fases del pipeline
# ---------------------------------------------------------------------------

class CyclePhase(Enum):
    VALIDATE  = auto()
    FETCH     = auto()
    NORMALIZE = auto()
    ENRICH    = auto()
    EVALUATE  = auto()
    STORE     = auto()
    PROPAGATE = auto()
    ALERT     = auto()
    DONE      = auto()


# ---------------------------------------------------------------------------
# Configuración del ciclo
# ---------------------------------------------------------------------------

@dataclass
class CycleConfig:
    """Parámetros de ejecución de un ciclo."""
    llm_concurrency: int = 5
    skip_llm_if_certain: bool = True
    # Callback opcional para progreso (phase, message)
    on_progress: Callable[[CyclePhase, str], None] | None = None


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class Runner(PipelineHandler):
    """
    Orquestador del ciclo completo.
    Implementa PipelineHandler para integrarse con el webhook receiver.

    Uso:
        store = GraphStore("data/grants_surveyor.db")
        store.initialize()
        runner = Runner(
            project_id="vigilancia-2026",
            investigation_id="mi-investigacion",
            store=store,
        )
        # Registro en el webhook receiver:
        from grants_surveyor.odm.webhook_receiver import state
        state.configure(store=store, pipeline_handler=runner)
    """

    def __init__(
        self,
        project_id: str,
        investigation_id: str,
        store: GraphStore,
        anthropic_client: anthropic.Anthropic | None = None,
        config: CycleConfig | None = None,
    ) -> None:
        self.project_id = project_id
        self.investigation_id = investigation_id
        self.store = store
        self._anthropic = anthropic_client or anthropic.Anthropic()
        self.config = config or CycleConfig()

    # ------------------------------------------------------------------
    # PipelineHandler interface
    # ------------------------------------------------------------------

    async def handle_odm_event(self, event: ODMWebhookEvent) -> None:
        """
        Punto de entrada desde el webhook receiver.
        Determina qué investigaciones están suscritas a este resource
        y lanza el pipeline para cada una.
        """
        investigation = self.store.get_investigation(self.investigation_id)
        if not investigation:
            logger.error("Investigación '%s' no encontrada.", self.investigation_id)
            return

        sub = investigation.get_subscription(event.resource_code)
        if not sub or not sub.active:
            logger.debug(
                "Investigación '%s' no suscrita a '%s'.",
                self.investigation_id, event.resource_code
            )
            return

        # ── Verifica compatibilidad de versión ──
        if not sub.should_process(event):
            logger.info(
                "Evento '%s' v%s ignorado por política de upgrade '%s'.",
                event.resource_code, event.version, sub.auto_upgrade.value
            )
            return

        if sub.requires_human_review_for(event):
            logger.warning(
                "MAJOR UPGRADE detectado en '%s': %s → %s. "
                "Requiere revisión humana antes de procesar.",
                event.resource_code,
                sub.current_version or "?",
                event.version,
            )
            self.store.log_odm_event(
                investigation_id=self.investigation_id,
                resource_code=event.resource_code,
                version=event.version,
                record_count=event.record_count,
                status="major_upgrade_pending",
            )
            return

        # ── Lanza el ciclo ──
        await self._run_cycle_for_event(event, investigation, sub)

    # ------------------------------------------------------------------
    # Ciclo principal
    # ------------------------------------------------------------------

    async def _run_cycle_for_event(
        self,
        event: ODMWebhookEvent,
        investigation: Investigation,
        sub: ODMSubscription,
    ) -> CycleSummary:
        started_at = datetime.utcnow()
        cfg = self.config

        self._notify(CyclePhase.FETCH, f"Descargando dataset {event.resource_code} v{event.version}...")

        evaluator = Evaluator(
            profile=investigation.watch_profile,
            anthropic_client=self._anthropic,
            skip_llm_if_certain=cfg.skip_llm_if_certain,
        )

        sem = asyncio.Semaphore(cfg.llm_concurrency)

        # Normalizer para este resource
        normalizer = self._get_normalizer(event.resource_code, investigation.watch_profile)

        # Source node para este dataset
        source_node = self._ensure_source_node(event)

        nodes_created = 0
        nodes_updated = 0
        findings_generated = 0
        tasks: list[asyncio.Task] = []

        # ── FETCH + NORMALIZE + EVALUATE ──
        async with ODMClient() as odm:
            async for batch in odm.iter_records(event.dataset_id):
                self._notify(
                    CyclePhase.FETCH,
                    f"Procesando lote de {len(batch)} registros..."
                )
                for record in batch:
                    task = asyncio.create_task(
                        self._process_record(
                            record=record,
                            normalizer=normalizer,
                            source_node=source_node,
                            evaluator=evaluator,
                            investigation_id=self.investigation_id,
                            sem=sem,
                        )
                    )
                    tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.error("Error procesando registro: %s", result)
            elif result:
                created, updated, found = result
                nodes_created += created
                nodes_updated += updated
                findings_generated += found

        # ── PROPAGATE ──
        self._notify(CyclePhase.PROPAGATE, "Propagando desde nodos confirmados...")
        propagated = await self._propagate(evaluator, sem, source_node)
        findings_generated += propagated

        # ── Registra evento como procesado ──
        self.store.log_odm_event(
            investigation_id=self.investigation_id,
            resource_code=event.resource_code,
            version=event.version,
            record_count=event.record_count,
            status="processed",
        )

        # ── Actualiza suscripción ──
        sub.current_version = event.version
        sub.notified_at = datetime.utcnow()

        # ── Cierra ciclo ──
        summary = CycleSummary(
            id=str(uuid.uuid4()),
            investigation_id=self.investigation_id,
            cycle_number=investigation.current_cycle_number,
            started_at=started_at,
            finished_at=datetime.utcnow(),
            odm_events_received=1,
            nodes_created=nodes_created,
            nodes_updated=nodes_updated,
            findings_generated=findings_generated,
        )

        investigation.cycles.append(summary)
        investigation.metrics = self._recompute_metrics(investigation)
        investigation.updated_at = datetime.utcnow()
        self.store.upsert_investigation(investigation)

        self._notify(
            CyclePhase.DONE,
            f"Ciclo completado: {nodes_created} nodos nuevos, "
            f"{findings_generated} hallazgos."
        )
        logger.info(
            "Ciclo completado para '%s' / '%s': %d nodos, %d hallazgos",
            self.investigation_id, event.resource_code,
            nodes_created, findings_generated,
        )
        return summary

    # ------------------------------------------------------------------
    # Procesamiento de un registro
    # ------------------------------------------------------------------

    async def _process_record(
        self,
        record: dict[str, Any],
        normalizer: ODMNormalizer,
        source_node: Source,
        evaluator: Evaluator,
        investigation_id: str,
        sem: asyncio.Semaphore,
    ) -> tuple[int, int, int]:
        """
        Normaliza un registro ODM, resuelve o crea su nodo en el grafo,
        y lo evalúa contra el WatchProfile.

        Devuelve (nodes_created, nodes_updated, findings_generated).
        """
        async with sem:
            # NORMALIZE
            attr_dicts, identifiers = normalizer.normalize(
                record=record,
                source_id=source_node.id,
                dataset_version=source_node.odm_version or "unknown",
            )

            if not identifiers and not attr_dicts:
                return 0, 0, 0

            # Determina tipo de nodo según el resource
            node_type = _resource_to_node_type(normalizer.resource_code)

            # Nombre canónico: usa 'nombre' o primer identificador disponible
            canonical_name = (
                next(
                    (a["value"] for a in attr_dicts if a["key"] == "nombre"),
                    None
                )
                or next(iter(identifiers.values()), "unknown")
            )
            if not canonical_name or canonical_name == "unknown":
                return 0, 0, 0

            # Resolve o crea nodo
            node, created = self._resolve_or_create_node(
                node_type=node_type,
                canonical_name=canonical_name,
                identifiers=identifiers,
                attr_dicts=attr_dicts,
                source_node=source_node,
            )

            # ENRICH — construye contexto del grafo
            relations = self.store.get_relations(node.id, investigation_id=investigation_id)
            related_nodes = self._preload_related(relations)
            confirmed_ids = self.store.get_confirmed_nodes_for_related(node.id)
            has_confirmed = self.store.node_has_confirmed_connection(node.id, investigation_id)

            ctx = EvalContext(
                subject=node,
                relations=relations,
                related_nodes=related_nodes,
                has_confirmed_connection=has_confirmed,
                confirmed_related_ids=[nid for nid, _ in confirmed_ids],
            )

            # EVALUATE
            finding = await evaluator.evaluate(
                ctx=ctx,
                investigation_id=investigation_id,
                source_id=source_node.id,
            )

            if finding:
                self.store.upsert_finding(finding)
                # Crea alerta si es hallazgo de alta certeza
                if (finding.max_level.value <= 2
                        and not self.store.alert_exists_for_node(
                            investigation_id, node.id
                        )):
                    self.store.upsert_alert(Alert(
                        id=str(uuid.uuid4()),
                        investigation_id=investigation_id,
                        subject_node_id=node.id,
                        subject_name=node.canonical_name,
                        watch_resource_codes=[normalizer.resource_code],
                    ))

                self._notify(
                    CyclePhase.EVALUATE,
                    f"Hallazgo: {node.canonical_name} "
                    f"({finding.confidence_aggregated:.0%})"
                )

            return (1 if created else 0), (0 if created else 1), (1 if finding else 0)

    # ------------------------------------------------------------------
    # Propagación
    # ------------------------------------------------------------------

    async def _propagate(
        self,
        evaluator: Evaluator,
        sem: asyncio.Semaphore,
        source_node: Source,
    ) -> int:
        """
        Para cada nodo confirmado, busca nodos relacionados que aún
        no hayan sido evaluados y los procesa.
        Devuelve el número de findings nuevos generados.
        """
        new_findings = 0
        confirmed = self.store.list_findings(
            investigation_id=self.investigation_id,
            status=FindingStatus.CONFIRMED,
            limit=500,
        )

        for confirmed_finding in confirmed:
            # Busca nodos relacionados por relaciones del grafo
            relations = self.store.get_relations(
                confirmed_finding.subject_node_id,
                investigation_id=self.investigation_id,
            )
            for rel in relations:
                other_id = (
                    rel.target_node_id
                    if rel.source_node_id == confirmed_finding.subject_node_id
                    else rel.source_node_id
                )
                # ¿Ya tiene finding en esta investigación?
                existing = self.store.get_findings_for_node(
                    other_id, self.investigation_id
                )
                if existing:
                    continue

                other_node = self.store.get_node(other_id)
                if not other_node:
                    continue

                other_relations = self.store.get_relations(
                    other_id, investigation_id=self.investigation_id
                )
                related_nodes = self._preload_related(other_relations)
                confirmed_ids = self.store.get_confirmed_nodes_for_related(other_id)
                has_confirmed = self.store.node_has_confirmed_connection(
                    other_id, self.investigation_id
                )

                ctx = EvalContext(
                    subject=other_node,
                    relations=other_relations,
                    related_nodes=related_nodes,
                    has_confirmed_connection=has_confirmed,
                    confirmed_related_ids=[nid for nid, _ in confirmed_ids],
                )

                async with sem:
                    finding = await evaluator.evaluate(
                        ctx=ctx,
                        investigation_id=self.investigation_id,
                        source_id=source_node.id,
                    )
                if finding:
                    self.store.upsert_finding(finding)
                    new_findings += 1

        return new_findings

    # ------------------------------------------------------------------
    # Refinamiento
    # ------------------------------------------------------------------

    async def refine_profile(self) -> dict[str, Any] | None:
        """
        Ejecuta el Refiner tras la revisión humana del ciclo.
        Devuelve propuestas de mejora (no las aplica automáticamente).
        """
        investigation = self._load_investigation()
        stats = self.store.get_cycle_stats(self.investigation_id)
        findings = self.store.list_findings(self.investigation_id, limit=500)

        false_positives = [
            {
                "canonical_name": f.subject_canonical_name,
                "confidence": f.confidence_aggregated,
                "signals": [e.signal[:100] for e in f.evidences[:3]],
                "analyst_notes": f.analyst_notes or "",
            }
            for f in findings
            if f.status == FindingStatus.DISCARDED
            and f.confidence_aggregated > 0.5
        ]

        investigating_ctx = [
            {
                "canonical_name": f.subject_canonical_name,
                "analyst_notes": f.analyst_notes or "",
            }
            for f in findings
            if f.status == FindingStatus.INVESTIGATING
        ]

        refiner = Refiner(
            profile=investigation.watch_profile,
            anthropic_client=self._anthropic,
        )
        return await refiner.propose_refinements(
            cycle_summary=stats,
            false_positives=false_positives,
            false_negatives=[],
            investigating=investigating_ctx,
        )

    # ------------------------------------------------------------------
    # Source discovery
    # ------------------------------------------------------------------

    async def discover_sources(
        self,
        query_description: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Rastrea fuentes relevantes para la investigación consultando ODM.
        """
        investigation = self._load_investigation()
        query = query_description or investigation.query_description

        async with ODMClient() as odm:
            available_resources = await odm.list_resources(active_only=True)

        discovery = SourceDiscovery(anthropic_client=self._anthropic)
        return await discovery.discover(
            query_description=query,
            available_resources=available_resources,
            fetcher_types=[],
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_investigation(self) -> Investigation:
        inv = self.store.get_investigation(self.investigation_id)
        if not inv:
            raise ValueError(
                f"Investigación '{self.investigation_id}' no encontrada."
            )
        return inv

    def _ensure_source_node(self, event: ODMWebhookEvent) -> Source:
        """Crea o recupera el nodo Source para este dataset ODM."""
        # Busca por odm_dataset_id
        existing = self.store.find_node_by_identifier(
            self.project_id, "odm_dataset_id", event.dataset_id
        )
        if existing and isinstance(existing, Source):
            return existing

        source = Source(
            id=str(uuid.uuid4()),
            project_id=self.project_id,
            canonical_name=f"{event.resource_code} v{event.version}",
            identifiers={"odm_dataset_id": event.dataset_id},
            url=event.odm_api_url or None,
            odm_dataset_id=event.dataset_id,
            odm_resource_code=event.resource_code,
            odm_version=event.version,
        )
        self.store.upsert_source(source)
        return source

    def _resolve_or_create_node(
        self,
        node_type: NodeType,
        canonical_name: str,
        identifiers: dict[str, str],
        attr_dicts: list[dict[str, Any]],
        source_node: Source,
    ) -> tuple[Actor | Process | Asset, bool]:
        """
        Busca el nodo por sus identificadores. Si no existe, lo crea.
        Actualiza atributos si ya existe.
        Devuelve (nodo, fue_creado).
        """
        # Busca por cada identificador conocido
        existing = None
        for key, value in identifiers.items():
            existing = self.store.find_node_by_identifier(
                self.project_id, key, value
            )
            if existing:
                break

        # Si no encontró por identificador, busca por nombre (score >= 0.9)
        if not existing:
            candidates = self.store.search_nodes_by_name(
                canonical_name, self.project_id, node_type=node_type, limit=1
            )
            if candidates and candidates[0][1] >= 0.9:
                existing = candidates[0][0]

        if existing:
            # Actualiza atributos e identifiers
            existing.identifiers.update(identifiers)
            for ad in attr_dicts:
                self.store.add_attribute(
                    existing.id,
                    Attribute(
                        key=ad["key"],
                        value=ad["value"],
                        source_id=ad["source_id"],
                        confidence=ad.get("confidence", 1.0),
                        odm_dataset_version=ad.get("odm_dataset_version"),
                        observed_at=ad.get("observed_at", datetime.utcnow()),
                    )
                )
            self.store.upsert_node(existing)
            return existing, False

        # Crea nodo nuevo
        attributes = [
            Attribute(
                key=ad["key"],
                value=ad["value"],
                source_id=ad["source_id"],
                confidence=ad.get("confidence", 1.0),
                odm_dataset_version=ad.get("odm_dataset_version"),
                observed_at=ad.get("observed_at", datetime.utcnow()),
            )
            for ad in attr_dicts
        ]

        node_id = str(uuid.uuid4())
        node_kwargs = dict(
            id=node_id,
            project_id=self.project_id,
            canonical_name=canonical_name,
            identifiers=identifiers,
            attributes=attributes,
        )

        if node_type == NodeType.ACTOR:
            node: Actor | Process | Asset = Actor(**node_kwargs)
        elif node_type == NodeType.PROCESS:
            node = Process(**node_kwargs)
        else:
            node = Asset(**node_kwargs)

        self.store.upsert_node(node)
        return node, True

    def _preload_related(
        self, relations: list[Relation]
    ) -> dict[str, Actor | Process | Asset | Source]:
        """Precarga los nodos relacionados para el EvalContext."""
        related: dict[str, Actor | Process | Asset | Source] = {}
        for rel in relations:
            for nid in (rel.source_node_id, rel.target_node_id):
                if nid not in related:
                    node = self.store.get_node(nid)
                    if node:
                        related[nid] = node
        return related

    def _get_normalizer(
        self, resource_code: str, profile: WatchProfile
    ) -> ODMNormalizer:
        """Obtiene el normalizer apropiado para el resource_code."""
        if resource_code == "bdns_concesiones":
            return ODMNormalizer.for_bdns_concesiones()
        if resource_code == "rer_entidades":
            return ODMNormalizer.for_rer_entidades()
        # Normalizer genérico sin mapeo — usa todos los campos tal cual
        return ODMNormalizer(resource_code=resource_code)

    def _recompute_metrics(self, inv: Investigation) -> InvestigationMetrics:
        stats = self.store.get_cycle_stats(self.investigation_id)
        m = inv.metrics
        m.total_confirmed = stats.get("confirmed", 0)
        m.total_findings_generated = stats.get("total_findings", 0)
        m.total_discarded = stats.get("discarded", 0)
        m.total_investigating = stats.get("investigating", 0)
        m.last_updated = datetime.utcnow()
        return m

    def _notify(self, phase: CyclePhase, message: str) -> None:
        if self.config.on_progress:
            self.config.on_progress(phase, message)
        else:
            logger.debug("[%s] %s", phase.name, message)


# ---------------------------------------------------------------------------
# Helper: resource_code → NodeType
# ---------------------------------------------------------------------------

def _resource_to_node_type(resource_code: str) -> NodeType:
    """
    Determina el tipo de nodo del metadominio para un resource_code.
    Por defecto Actor — la mayoría de registros de datos públicos
    son sobre organizaciones o personas.
    Configurable vía WatchProfile si se necesita Process o Asset.
    """
    _map = {
        "bdns_concesiones": NodeType.PROCESS,   # una concesión es un proceso
        "rer_entidades":    NodeType.ACTOR,
        "boe_sumarios":     NodeType.PROCESS,
        "catastro":         NodeType.ASSET,
    }
    return _map.get(resource_code, NodeType.ACTOR)


# ---------------------------------------------------------------------------
# Factory — crea proyecto e investigación desde cero
# ---------------------------------------------------------------------------

async def create_investigation(
    project_id: str,
    project_name: str,
    investigation_id: str,
    investigation_name: str,
    query_description: str,
    db_path: str = "data/grants_surveyor.db",
    odm_resource_codes: list[str] | None = None,
) -> tuple[Project, Investigation]:
    """
    Crea o recupera un proyecto e investigación listos para usar.

    odm_resource_codes: lista de resource_codes de ODM a suscribir.
    Si no se pasa, la investigación arranca sin fuentes — el analista
    las configura desde el Workbench o vía SourceDiscovery.
    """
    store = GraphStore(db_path)
    store.initialize()

    project = store.get_project(project_id)
    if not project:
        project = Project(id=project_id, name=project_name)
        store.upsert_project(project)

    profile = WatchProfile(
        id=str(uuid.uuid4()),
        investigation_id=investigation_id,
        version="1.0.0",
        name=investigation_name,
        query_description=query_description,
    )

    subscriptions = [
        ODMSubscription(
            investigation_id=investigation_id,
            resource_id="",          # se rellena cuando ODM confirme el UUID
            resource_code=code,
            resource_name=code,
            auto_upgrade=UpgradePolicy.MINOR,
        )
        for code in (odm_resource_codes or [])
    ]

    inv = store.get_investigation(investigation_id)
    if not inv:
        inv = Investigation(
            id=investigation_id,
            project_id=project_id,
            name=investigation_name,
            query_description=query_description,
            watch_profile=profile,
            odm_subscriptions=subscriptions,
            status=InvestigationStatus.ACTIVE,
        )
        store.upsert_investigation(inv)
        if investigation_id not in project.investigation_ids:
            project.investigation_ids.append(investigation_id)
            store.upsert_project(project)
        logger.info("Investigación '%s' creada.", investigation_name)
    else:
        logger.info("Investigación '%s' ya existe.", investigation_name)

    return project, inv
