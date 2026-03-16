"""
graph/store.py — Persistencia del grafo de conocimiento.

Una base de datos SQLite por proyecto. Todas las investigaciones
del proyecto comparten los nodos (Actor, Process, Asset, Source)
pero las relaciones, evidencias y findings llevan investigation_id
para mantener el aislamiento semántico.

Diseño:
  - Nodos: tabla genérica 'nodes' con tipo, nombre canónico e identifiers
  - Atributos: tabla 'node_attributes' con clave/valor/fuente/confianza
  - Relaciones: tabla 'relations' genérica con tipo y atributos JSON
  - Findings + Evidence: privados por investigation_id
  - ODM: subscriptions y eventos recibidos
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Generator

from ..models import (
    Actor,
    AgentExport,
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
    InvestigationTask,
    NodeType,
    ODMSubscription,
    Process,
    Project,
    Relation,
    RelationType,
    Source,
    UpgradePolicy,
    WatchProfile,
)


def _now() -> str:
    return datetime.utcnow().isoformat()


def _new_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Store principal
# ---------------------------------------------------------------------------

class GraphStore:
    """
    Interfaz con la base de datos SQLite del proyecto.

    Uso:
        store = GraphStore("data/grants_surveyor.db")
        store.initialize()
        with store.connection() as conn:
            ...
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Conexión
    # ------------------------------------------------------------------

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def initialize(self) -> None:
        """Crea todas las tablas si no existen. Idempotente."""
        with self.connection() as conn:
            conn.executescript(SCHEMA_SQL)

    # ------------------------------------------------------------------
    # Projects
    # ------------------------------------------------------------------

    def upsert_project(self, project: Project) -> None:
        with self.connection() as conn:
            conn.execute("""
                INSERT INTO projects (id, name, description, investigation_ids, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,
                    description=excluded.description,
                    investigation_ids=excluded.investigation_ids,
                    updated_at=excluded.updated_at
            """, (
                project.id, project.name, project.description,
                json.dumps(project.investigation_ids),
                project.created_at.isoformat(), _now(),
            ))

    def get_project(self, project_id: str) -> Project | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM projects WHERE id = ?", (project_id,)
            ).fetchone()
        if not row:
            return None
        return Project(
            id=row["id"], name=row["name"],
            description=row["description"] or "",
            investigation_ids=json.loads(row["investigation_ids"] or "[]"),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def list_projects(self) -> list[Project]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT id FROM projects ORDER BY created_at DESC"
            ).fetchall()
        return [p for r in rows if (p := self.get_project(r["id"]))]

    # ------------------------------------------------------------------
    # Investigations
    # ------------------------------------------------------------------

    def upsert_investigation(self, inv: Investigation) -> None:
        with self.connection() as conn:
            conn.execute("""
                INSERT INTO investigations
                    (id, project_id, name, description, query_description,
                     watch_profile_json, odm_subscriptions_json,
                     cycles_json, metrics_json, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name=excluded.name,
                    description=excluded.description,
                    query_description=excluded.query_description,
                    watch_profile_json=excluded.watch_profile_json,
                    odm_subscriptions_json=excluded.odm_subscriptions_json,
                    cycles_json=excluded.cycles_json,
                    metrics_json=excluded.metrics_json,
                    status=excluded.status,
                    updated_at=excluded.updated_at
            """, (
                inv.id, inv.project_id, inv.name, inv.description,
                inv.query_description,
                inv.watch_profile.model_dump_json(),
                json.dumps([s.model_dump(mode="json") for s in inv.odm_subscriptions]),
                json.dumps([c.model_dump(mode="json") for c in inv.cycles]),
                inv.metrics.model_dump_json(),
                inv.status.value,
                inv.created_at.isoformat(), _now(),
            ))

    def get_investigation(self, investigation_id: str) -> Investigation | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM investigations WHERE id = ?", (investigation_id,)
            ).fetchone()
        if not row:
            return None
        return Investigation(
            id=row["id"], project_id=row["project_id"],
            name=row["name"], description=row["description"] or "",
            query_description=row["query_description"],
            watch_profile=WatchProfile.model_validate_json(row["watch_profile_json"]),
            odm_subscriptions=[
                ODMSubscription.model_validate(s)
                for s in json.loads(row["odm_subscriptions_json"] or "[]")
            ],
            cycles=[
                CycleSummary.model_validate(c)
                for c in json.loads(row["cycles_json"] or "[]")
            ],
            metrics=InvestigationMetrics.model_validate_json(row["metrics_json"]),
            status=InvestigationStatus(row["status"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def list_investigations(self, project_id: str) -> list[Investigation]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT id FROM investigations WHERE project_id = ? ORDER BY created_at DESC",
                (project_id,)
            ).fetchall()
        return [i for r in rows if (i := self.get_investigation(r["id"]))]

    # ------------------------------------------------------------------
    # Nodes (Actor, Process, Asset, Source)
    # ------------------------------------------------------------------

    def upsert_node(self, node: Actor | Process | Asset | Source) -> None:
        with self.connection() as conn:
            conn.execute("""
                INSERT INTO nodes
                    (id, project_id, node_type, canonical_name,
                     identifiers_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    canonical_name=excluded.canonical_name,
                    identifiers_json=excluded.identifiers_json,
                    updated_at=excluded.updated_at
            """, (
                node.id, node.project_id, node.node_type.value,
                node.canonical_name,
                json.dumps(node.identifiers),
                node.created_at.isoformat(), _now(),
            ))
            # Upsert attributes
            for attr in node.attributes:
                conn.execute("""
                    INSERT INTO node_attributes
                        (id, node_id, key, value_json, source_id,
                         confidence, odm_dataset_version, observed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(node_id, key, source_id) DO UPDATE SET
                        value_json=excluded.value_json,
                        confidence=excluded.confidence,
                        observed_at=excluded.observed_at
                """, (
                    _new_id(), node.id, attr.key,
                    json.dumps(attr.value),
                    attr.source_id, attr.confidence,
                    attr.odm_dataset_version,
                    attr.observed_at.isoformat(),
                ))

    def get_node(self, node_id: str) -> Actor | Process | Asset | Source | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM nodes WHERE id = ?", (node_id,)
            ).fetchone()
            if not row:
                return None
            attr_rows = conn.execute(
                "SELECT * FROM node_attributes WHERE node_id = ?", (node_id,)
            ).fetchall()

        attributes = [
            Attribute(
                key=r["key"],
                value=json.loads(r["value_json"]),
                source_id=r["source_id"],
                confidence=r["confidence"],
                odm_dataset_version=r["odm_dataset_version"],
                observed_at=datetime.fromisoformat(r["observed_at"]),
            )
            for r in attr_rows
        ]

        base = dict(
            id=row["id"], project_id=row["project_id"],
            canonical_name=row["canonical_name"],
            identifiers=json.loads(row["identifiers_json"] or "{}"),
            attributes=attributes,
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

        node_type = NodeType(row["node_type"])
        if node_type == NodeType.ACTOR:
            return Actor(**base)
        elif node_type == NodeType.PROCESS:
            return Process(**base)
        elif node_type == NodeType.ASSET:
            return Asset(**base)
        elif node_type == NodeType.SOURCE:
            with self.connection() as conn:
                src_row = conn.execute(
                    "SELECT * FROM source_extras WHERE node_id = ?", (node_id,)
                ).fetchone()
            extras: dict[str, Any] = {}
            if src_row:
                extras = {
                    "url": src_row["url"],
                    "odm_dataset_id": src_row["odm_dataset_id"],
                    "odm_resource_code": src_row["odm_resource_code"],
                    "odm_version": src_row["odm_version"],
                }
            return Source(**base, **extras)
        return None

    def find_node_by_identifier(
        self, project_id: str, key: str, value: str
    ) -> Actor | Process | Asset | Source | None:
        """Busca un nodo por un identificador conocido (ej: nif=R1234567A)."""
        with self.connection() as conn:
            row = conn.execute("""
                SELECT id FROM nodes
                WHERE project_id = ?
                  AND json_extract(identifiers_json, '$.' || ?) = ?
            """, (project_id, key, value)).fetchone()
        return self.get_node(row["id"]) if row else None

    def search_nodes_by_name(
        self,
        name: str,
        project_id: str,
        node_type: NodeType | None = None,
        limit: int = 10,
    ) -> list[tuple[Actor | Process | Asset | Source, float]]:
        """Búsqueda por nombre canónico con score simple."""
        normalized = name.upper()
        query = """
            SELECT id,
                CASE
                    WHEN UPPER(canonical_name) = ? THEN 1.0
                    WHEN UPPER(canonical_name) LIKE ? THEN 0.8
                    WHEN UPPER(canonical_name) LIKE ? THEN 0.6
                    ELSE 0.3
                END as score
            FROM nodes
            WHERE project_id = ?
              AND UPPER(canonical_name) LIKE ?
        """
        params: list[Any] = [
            normalized, f"{normalized}%", f"%{normalized}%",
            project_id, f"%{normalized}%"
        ]
        if node_type:
            query += " AND node_type = ?"
            params.append(node_type.value)
        query += " ORDER BY score DESC LIMIT ?"
        params.append(limit)

        with self.connection() as conn:
            rows = conn.execute(query, params).fetchall()

        results = []
        for row in rows:
            node = self.get_node(row["id"])
            if node:
                results.append((node, float(row["score"])))
        return results

    def upsert_source(self, source: Source) -> None:
        """Upsert de Source con sus campos extra."""
        self.upsert_node(source)
        with self.connection() as conn:
            conn.execute("""
                INSERT INTO source_extras
                    (node_id, url, odm_dataset_id, odm_resource_code,
                     odm_version, retrieved_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    url=excluded.url,
                    odm_dataset_id=excluded.odm_dataset_id,
                    odm_resource_code=excluded.odm_resource_code,
                    odm_version=excluded.odm_version,
                    retrieved_at=excluded.retrieved_at
            """, (
                source.id, source.url, source.odm_dataset_id,
                source.odm_resource_code, source.odm_version,
                source.retrieved_at.isoformat(),
            ))

    def add_attribute(self, node_id: str, attr: Attribute) -> None:
        """Añade o actualiza un atributo de un nodo."""
        with self.connection() as conn:
            conn.execute("""
                INSERT INTO node_attributes
                    (id, node_id, key, value_json, source_id,
                     confidence, odm_dataset_version, observed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id, key, source_id) DO UPDATE SET
                    value_json=excluded.value_json,
                    confidence=excluded.confidence,
                    observed_at=excluded.observed_at
            """, (
                _new_id(), node_id, attr.key,
                json.dumps(attr.value),
                attr.source_id, attr.confidence,
                attr.odm_dataset_version,
                attr.observed_at.isoformat(),
            ))

    # ------------------------------------------------------------------
    # Relations
    # ------------------------------------------------------------------

    def upsert_relation(self, rel: Relation) -> None:
        with self.connection() as conn:
            conn.execute("""
                INSERT INTO relations
                    (id, project_id, investigation_id, relation_type,
                     source_node_id, target_node_id, attributes_json,
                     source_id, confidence, confirmed_by_human,
                     author, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    attributes_json=excluded.attributes_json,
                    confidence=excluded.confidence,
                    confirmed_by_human=excluded.confirmed_by_human
            """, (
                rel.id, rel.project_id, rel.investigation_id,
                rel.relation_type.value,
                rel.source_node_id, rel.target_node_id,
                json.dumps(rel.attributes),
                rel.source_id, rel.confidence,
                int(rel.confirmed_by_human), rel.author,
                rel.created_at.isoformat(),
            ))

    def get_relations(
        self,
        node_id: str,
        relation_type: RelationType | None = None,
        investigation_id: str | None = None,
        direction: str = "both",  # "out" | "in" | "both"
    ) -> list[Relation]:
        """Devuelve relaciones de un nodo, filtrando por tipo y dirección."""
        conditions = []
        params: list[Any] = []

        if direction == "out":
            conditions.append("source_node_id = ?")
            params.append(node_id)
        elif direction == "in":
            conditions.append("target_node_id = ?")
            params.append(node_id)
        else:
            conditions.append("(source_node_id = ? OR target_node_id = ?)")
            params.extend([node_id, node_id])

        if relation_type:
            conditions.append("relation_type = ?")
            params.append(relation_type.value)
        if investigation_id:
            conditions.append("investigation_id = ?")
            params.append(investigation_id)

        query = "SELECT * FROM relations WHERE " + " AND ".join(conditions)

        with self.connection() as conn:
            rows = conn.execute(query, params).fetchall()

        return [
            Relation(
                id=r["id"], project_id=r["project_id"],
                investigation_id=r["investigation_id"],
                relation_type=RelationType(r["relation_type"]),
                source_node_id=r["source_node_id"],
                target_node_id=r["target_node_id"],
                attributes=json.loads(r["attributes_json"] or "{}"),
                source_id=r["source_id"],
                confidence=r["confidence"],
                confirmed_by_human=bool(r["confirmed_by_human"]),
                author=r["author"],
                created_at=datetime.fromisoformat(r["created_at"]),
            )
            for r in rows
        ]

    def get_same_as_candidates(
        self, node_id: str, min_confidence: float = 0.7
    ) -> list[tuple[str, float, bool]]:
        """
        Devuelve candidatos de resolución de identidad para un nodo.
        Retorna lista de (target_node_id, confidence, confirmed_by_human).
        """
        with self.connection() as conn:
            rows = conn.execute("""
                SELECT target_node_id, confidence, confirmed_by_human
                FROM relations
                WHERE source_node_id = ?
                  AND relation_type = ?
                  AND confidence >= ?
                ORDER BY confidence DESC
            """, (node_id, RelationType.SAME_AS.value, min_confidence)).fetchall()
        return [(r["target_node_id"], r["confidence"], bool(r["confirmed_by_human"]))
                for r in rows]

    # ------------------------------------------------------------------
    # Findings
    # ------------------------------------------------------------------

    def upsert_finding(self, finding: Finding) -> None:
        with self.connection() as conn:
            conn.execute("""
                INSERT INTO findings
                    (id, investigation_id, subject_node_id, subject_node_type,
                     subject_canonical_name, related_node_ids_json,
                     evidences_json, confidence_aggregated, max_level,
                     narrative, status, requires_human_review, publishable,
                     confirmed_by, confirmed_at, analyst_notes,
                     watch_profile_version, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    evidences_json=excluded.evidences_json,
                    confidence_aggregated=excluded.confidence_aggregated,
                    max_level=excluded.max_level,
                    narrative=excluded.narrative,
                    status=excluded.status,
                    publishable=excluded.publishable,
                    confirmed_by=excluded.confirmed_by,
                    confirmed_at=excluded.confirmed_at,
                    analyst_notes=excluded.analyst_notes,
                    updated_at=excluded.updated_at
            """, (
                finding.id, finding.investigation_id,
                finding.subject_node_id, finding.subject_node_type,
                finding.subject_canonical_name,
                json.dumps(finding.related_node_ids),
                json.dumps([e.model_dump(mode="json") for e in finding.evidences]),
                finding.confidence_aggregated, finding.max_level.value,
                finding.narrative, finding.status.value,
                int(finding.requires_human_review), int(finding.publishable),
                finding.confirmed_by,
                finding.confirmed_at.isoformat() if finding.confirmed_at else None,
                finding.analyst_notes, finding.watch_profile_version,
                finding.created_at.isoformat(), _now(),
            ))

    def get_finding(self, finding_id: str) -> Finding | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM findings WHERE id = ?", (finding_id,)
            ).fetchone()
        if not row:
            return None
        return Finding(
            id=row["id"], investigation_id=row["investigation_id"],
            subject_node_id=row["subject_node_id"],
            subject_node_type=row["subject_node_type"],
            subject_canonical_name=row["subject_canonical_name"],
            related_node_ids=json.loads(row["related_node_ids_json"] or "[]"),
            evidences=[
                Evidence.model_validate(e)
                for e in json.loads(row["evidences_json"] or "[]")
            ],
            confidence_aggregated=row["confidence_aggregated"],
            max_level=EvidenceLevel(row["max_level"]),
            narrative=row["narrative"] or "",
            status=FindingStatus(row["status"]),
            requires_human_review=bool(row["requires_human_review"]),
            publishable=bool(row["publishable"]),
            confirmed_by=row["confirmed_by"],
            confirmed_at=(
                datetime.fromisoformat(row["confirmed_at"])
                if row["confirmed_at"] else None
            ),
            analyst_notes=row["analyst_notes"],
            watch_profile_version=row["watch_profile_version"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def list_findings(
        self,
        investigation_id: str,
        status: FindingStatus | None = None,
        min_confidence: float = 0.0,
        subject_node_type: str | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> list[Finding]:
        query = "SELECT id FROM findings WHERE investigation_id = ?"
        params: list[Any] = [investigation_id]
        if status:
            query += " AND status = ?"
            params.append(status.value)
        if min_confidence > 0:
            query += " AND confidence_aggregated >= ?"
            params.append(min_confidence)
        if subject_node_type:
            query += " AND subject_node_type = ?"
            params.append(subject_node_type)
        query += " ORDER BY confidence_aggregated DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self.connection() as conn:
            rows = conn.execute(query, params).fetchall()
        return [f for r in rows if (f := self.get_finding(r["id"]))]

    def get_findings_for_node(
        self, node_id: str, investigation_id: str
    ) -> list[Finding]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT id FROM findings WHERE subject_node_id = ? AND investigation_id = ?",
                (node_id, investigation_id)
            ).fetchall()
        return [f for r in rows if (f := self.get_finding(r["id"]))]

    def confirm_finding(
        self, finding_id: str, confirmed_by: str, notes: str = ""
    ) -> None:
        with self.connection() as conn:
            conn.execute("""
                UPDATE findings
                SET status=?, confirmed_by=?, confirmed_at=?,
                    analyst_notes=?, updated_at=?
                WHERE id=?
            """, (
                FindingStatus.CONFIRMED.value, confirmed_by,
                _now(), notes, _now(), finding_id,
            ))

    def discard_finding(
        self, finding_id: str, confirmed_by: str, notes: str = ""
    ) -> None:
        with self.connection() as conn:
            conn.execute("""
                UPDATE findings
                SET status=?, confirmed_by=?, confirmed_at=?,
                    analyst_notes=?, publishable=0, updated_at=?
                WHERE id=?
            """, (
                FindingStatus.DISCARDED.value, confirmed_by,
                _now(), notes, _now(), finding_id,
            ))

    def mark_investigating(
        self, finding_id: str, analyst: str, notes: str = ""
    ) -> None:
        with self.connection() as conn:
            conn.execute("""
                UPDATE findings
                SET status=?, analyst_notes=?, updated_at=?
                WHERE id=?
            """, (FindingStatus.INVESTIGATING.value, notes, _now(), finding_id))

    # ------------------------------------------------------------------
    # ODM Webhook events log
    # ------------------------------------------------------------------

    def log_odm_event(
        self,
        investigation_id: str,
        resource_code: str,
        version: str,
        record_count: int | None,
        status: str,
        error: str | None = None,
    ) -> None:
        with self.connection() as conn:
            conn.execute("""
                INSERT INTO odm_events
                    (id, investigation_id, resource_code, version,
                     record_count, status, error, received_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                _new_id(), investigation_id, resource_code, version,
                record_count, status, error, _now(),
            ))

    def get_last_odm_version(
        self, investigation_id: str, resource_code: str
    ) -> str | None:
        with self.connection() as conn:
            row = conn.execute("""
                SELECT version FROM odm_events
                WHERE investigation_id = ? AND resource_code = ?
                  AND status = 'processed'
                ORDER BY received_at DESC LIMIT 1
            """, (investigation_id, resource_code)).fetchone()
        return row["version"] if row else None

    # ------------------------------------------------------------------
    # Alerts
    # ------------------------------------------------------------------

    def upsert_alert(self, alert: Alert) -> None:
        with self.connection() as conn:
            conn.execute("""
                INSERT INTO alerts
                    (id, investigation_id, subject_node_id, subject_name,
                     watch_resource_codes_json, active, triggers_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    active=excluded.active,
                    triggers_json=excluded.triggers_json
            """, (
                alert.id, alert.investigation_id,
                alert.subject_node_id, alert.subject_name,
                json.dumps(alert.watch_resource_codes),
                int(alert.active),
                json.dumps(alert.triggers),
                alert.created_at.isoformat(),
            ))

    def get_active_alerts(self, investigation_id: str) -> list[Alert]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM alerts WHERE investigation_id = ? AND active = 1",
                (investigation_id,)
            ).fetchall()
        return [
            Alert(
                id=r["id"], investigation_id=r["investigation_id"],
                subject_node_id=r["subject_node_id"],
                subject_name=r["subject_name"],
                watch_resource_codes=json.loads(r["watch_resource_codes_json"] or "[]"),
                active=bool(r["active"]),
                triggers=json.loads(r["triggers_json"] or "[]"),
                created_at=datetime.fromisoformat(r["created_at"]),
            )
            for r in rows
        ]

    def alert_exists_for_node(
        self, investigation_id: str, subject_node_id: str
    ) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM alerts WHERE investigation_id=? AND subject_node_id=? AND active=1",
                (investigation_id, subject_node_id)
            ).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------

    def upsert_task(self, task: InvestigationTask) -> None:
        with self.connection() as conn:
            conn.execute("""
                INSERT INTO investigation_tasks
                    (id, investigation_id, finding_id, subject_node_id,
                     subject_name, title, description, analyst_notes,
                     status, created_by, created_at, updated_at,
                     resolved_at, resolution, resolution_notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    analyst_notes=excluded.analyst_notes,
                    status=excluded.status,
                    resolved_at=excluded.resolved_at,
                    resolution=excluded.resolution,
                    resolution_notes=excluded.resolution_notes,
                    updated_at=excluded.updated_at
            """, (
                task.id, task.investigation_id, task.finding_id,
                task.subject_node_id, task.subject_name,
                task.title, task.description, task.analyst_notes,
                task.status, task.created_by,
                task.created_at.isoformat(), _now(),
                task.resolved_at.isoformat() if task.resolved_at else None,
                task.resolution, task.resolution_notes,
            ))

    def list_tasks(
        self, investigation_id: str, status: str | None = None
    ) -> list[InvestigationTask]:
        query = "SELECT * FROM investigation_tasks WHERE investigation_id = ?"
        params: list[Any] = [investigation_id]
        if status:
            query += " AND status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC"
        with self.connection() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            InvestigationTask(
                id=r["id"], investigation_id=r["investigation_id"],
                finding_id=r["finding_id"], subject_node_id=r["subject_node_id"],
                subject_name=r["subject_name"], title=r["title"],
                description=r["description"] or "",
                analyst_notes=r["analyst_notes"] or "",
                status=r["status"], created_by=r["created_by"],
                created_at=datetime.fromisoformat(r["created_at"]),
                updated_at=datetime.fromisoformat(r["updated_at"]),
                resolved_at=(
                    datetime.fromisoformat(r["resolved_at"])
                    if r["resolved_at"] else None
                ),
                resolution=r["resolution"],
                resolution_notes=r["resolution_notes"] or "",
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Stats del ciclo
    # ------------------------------------------------------------------

    def get_cycle_stats(self, investigation_id: str) -> dict[str, Any]:
        with self.connection() as conn:
            total = conn.execute(
                "SELECT COUNT(*) as n FROM findings WHERE investigation_id=?",
                (investigation_id,)
            ).fetchone()["n"]
            by_status = conn.execute("""
                SELECT status, COUNT(*) as n FROM findings
                WHERE investigation_id=? GROUP BY status
            """, (investigation_id,)).fetchall()
            nodes = conn.execute("""
                SELECT COUNT(DISTINCT subject_node_id) as n FROM findings
                WHERE investigation_id=?
            """, (investigation_id,)).fetchone()["n"]

        status_counts = {r["status"]: r["n"] for r in by_status}
        reviewed = total - status_counts.get("pending", 0)
        return {
            "total_findings": total,
            "pending": status_counts.get("pending", 0),
            "confirmed": status_counts.get("confirmed", 0),
            "discarded": status_counts.get("discarded", 0),
            "investigating": status_counts.get("investigating", 0),
            "reviewed": reviewed,
            "coverage": reviewed / total if total > 0 else 0.0,
            "unique_nodes": nodes,
        }

    # ------------------------------------------------------------------
    # Grafo — queries cruzadas para el motor de reglas
    # ------------------------------------------------------------------

    def node_has_confirmed_connection(
        self, node_id: str, investigation_id: str
    ) -> bool:
        """
        ¿Existe algún nodo relacionado con este que esté confirmado
        en cualquier investigación del proyecto?
        """
        with self.connection() as conn:
            row = conn.execute("""
                SELECT 1
                FROM relations r
                JOIN findings f ON (
                    f.subject_node_id = r.target_node_id OR
                    f.subject_node_id = r.source_node_id
                )
                WHERE (r.source_node_id = ? OR r.target_node_id = ?)
                  AND f.status = 'confirmed'
                  AND f.subject_node_id != ?
                LIMIT 1
            """, (node_id, node_id, node_id)).fetchone()
        return row is not None

    def get_confirmed_nodes_for_related(
        self, node_id: str
    ) -> list[tuple[str, str]]:
        """
        Nodos confirmados relacionados con este nodo.
        Devuelve lista de (node_id, investigation_id).
        """
        with self.connection() as conn:
            rows = conn.execute("""
                SELECT DISTINCT f.subject_node_id, f.investigation_id
                FROM findings f
                JOIN relations r ON (
                    f.subject_node_id = r.source_node_id OR
                    f.subject_node_id = r.target_node_id
                )
                WHERE (r.source_node_id = ? OR r.target_node_id = ?)
                  AND f.status = 'confirmed'
                  AND f.subject_node_id != ?
            """, (node_id, node_id, node_id)).fetchall()
        return [(r["subject_node_id"], r["investigation_id"]) for r in rows]


# ---------------------------------------------------------------------------
# Schema SQL
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS projects (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    description         TEXT,
    investigation_ids   TEXT DEFAULT '[]',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS investigations (
    id                      TEXT PRIMARY KEY,
    project_id              TEXT NOT NULL REFERENCES projects(id),
    name                    TEXT NOT NULL,
    description             TEXT,
    query_description       TEXT NOT NULL,
    watch_profile_json      TEXT NOT NULL,
    odm_subscriptions_json  TEXT DEFAULT '[]',
    cycles_json             TEXT DEFAULT '[]',
    metrics_json            TEXT NOT NULL,
    status                  TEXT NOT NULL DEFAULT 'active',
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
    id                  TEXT PRIMARY KEY,
    project_id          TEXT NOT NULL REFERENCES projects(id),
    node_type           TEXT NOT NULL,
    canonical_name      TEXT NOT NULL,
    identifiers_json    TEXT DEFAULT '{}',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_nodes_project ON nodes(project_id);
CREATE INDEX IF NOT EXISTS idx_nodes_type ON nodes(project_id, node_type);
CREATE INDEX IF NOT EXISTS idx_nodes_name ON nodes(project_id, canonical_name);

CREATE TABLE IF NOT EXISTS node_attributes (
    id                  TEXT NOT NULL,
    node_id             TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    key                 TEXT NOT NULL,
    value_json          TEXT NOT NULL,
    source_id           TEXT NOT NULL,
    confidence          REAL NOT NULL DEFAULT 1.0,
    odm_dataset_version TEXT,
    observed_at         TEXT NOT NULL,
    UNIQUE(node_id, key, source_id)
);
CREATE INDEX IF NOT EXISTS idx_attrs_node ON node_attributes(node_id);
CREATE INDEX IF NOT EXISTS idx_attrs_key ON node_attributes(node_id, key);

CREATE TABLE IF NOT EXISTS source_extras (
    node_id             TEXT PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
    url                 TEXT,
    odm_dataset_id      TEXT,
    odm_resource_code   TEXT,
    odm_version         TEXT,
    retrieved_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS relations (
    id                  TEXT PRIMARY KEY,
    project_id          TEXT NOT NULL REFERENCES projects(id),
    investigation_id    TEXT NOT NULL REFERENCES investigations(id),
    relation_type       TEXT NOT NULL,
    source_node_id      TEXT NOT NULL REFERENCES nodes(id),
    target_node_id      TEXT NOT NULL REFERENCES nodes(id),
    attributes_json     TEXT DEFAULT '{}',
    source_id           TEXT NOT NULL,
    confidence          REAL NOT NULL DEFAULT 1.0,
    confirmed_by_human  INTEGER NOT NULL DEFAULT 0,
    author              TEXT NOT NULL DEFAULT 'agent',
    created_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rel_source ON relations(source_node_id);
CREATE INDEX IF NOT EXISTS idx_rel_target ON relations(target_node_id);
CREATE INDEX IF NOT EXISTS idx_rel_type ON relations(investigation_id, relation_type);
CREATE INDEX IF NOT EXISTS idx_rel_investigation ON relations(investigation_id);

CREATE TABLE IF NOT EXISTS findings (
    id                      TEXT PRIMARY KEY,
    investigation_id        TEXT NOT NULL REFERENCES investigations(id),
    subject_node_id         TEXT NOT NULL REFERENCES nodes(id),
    subject_node_type       TEXT NOT NULL,
    subject_canonical_name  TEXT NOT NULL,
    related_node_ids_json   TEXT DEFAULT '[]',
    evidences_json          TEXT DEFAULT '[]',
    confidence_aggregated   REAL NOT NULL DEFAULT 0.0,
    max_level               INTEGER NOT NULL DEFAULT 5,
    narrative               TEXT DEFAULT '',
    status                  TEXT NOT NULL DEFAULT 'pending',
    requires_human_review   INTEGER NOT NULL DEFAULT 1,
    publishable             INTEGER NOT NULL DEFAULT 0,
    confirmed_by            TEXT,
    confirmed_at            TEXT,
    analyst_notes           TEXT,
    watch_profile_version   TEXT,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_findings_investigation ON findings(investigation_id);
CREATE INDEX IF NOT EXISTS idx_findings_node ON findings(subject_node_id);
CREATE INDEX IF NOT EXISTS idx_findings_status ON findings(investigation_id, status);
CREATE INDEX IF NOT EXISTS idx_findings_confidence ON findings(investigation_id, confidence_aggregated DESC);

CREATE TABLE IF NOT EXISTS odm_events (
    id                  TEXT PRIMARY KEY,
    investigation_id    TEXT NOT NULL REFERENCES investigations(id),
    resource_code       TEXT NOT NULL,
    version             TEXT NOT NULL,
    record_count        INTEGER,
    status              TEXT NOT NULL,
    error               TEXT,
    received_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_odm_events_inv ON odm_events(investigation_id, resource_code);

CREATE TABLE IF NOT EXISTS alerts (
    id                      TEXT PRIMARY KEY,
    investigation_id        TEXT NOT NULL REFERENCES investigations(id),
    subject_node_id         TEXT NOT NULL REFERENCES nodes(id),
    subject_name            TEXT NOT NULL,
    watch_resource_codes_json TEXT DEFAULT '[]',
    active                  INTEGER NOT NULL DEFAULT 1,
    triggers_json           TEXT DEFAULT '[]',
    created_at              TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alerts_investigation ON alerts(investigation_id);

CREATE TABLE IF NOT EXISTS investigation_tasks (
    id                  TEXT PRIMARY KEY,
    investigation_id    TEXT NOT NULL REFERENCES investigations(id),
    finding_id          TEXT NOT NULL REFERENCES findings(id),
    subject_node_id     TEXT NOT NULL REFERENCES nodes(id),
    subject_name        TEXT NOT NULL,
    title               TEXT NOT NULL,
    description         TEXT,
    analyst_notes       TEXT,
    status              TEXT NOT NULL DEFAULT 'open',
    created_by          TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    resolved_at         TEXT,
    resolution          TEXT,
    resolution_notes    TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_investigation ON investigation_tasks(investigation_id);
"""
