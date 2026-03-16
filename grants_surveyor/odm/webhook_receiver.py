"""
odm/webhook_receiver.py — Receptor de notificaciones webhook de OpenDataManager.

OpenDataManager envía un POST firmado con HMAC-SHA256 a este endpoint
cada vez que hay un Dataset nuevo disponible para una suscripción activa.

El receiver:
  1. Verifica la firma HMAC del webhook
  2. Parsea el ODMWebhookEvent
  3. Detecta si es un major upgrade (requiere revisión humana)
  4. Lanza el pipeline de evaluación de forma asíncrona
  5. Responde 200 inmediatamente (no bloquea a ODM)

Configuración vía .env:
  ODM_WEBHOOK_SECRET=<secreto compartido con ODM>
  ODM_WEBHOOK_PORT=8050
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from ..models import ODMWebhookEvent
from ..graph.store import GraphStore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Estado global del receiver
# ---------------------------------------------------------------------------

class ReceiverState:
    """Estado compartido entre el webhook receiver y el pipeline."""

    def __init__(self) -> None:
        self.store: GraphStore | None = None
        self.pipeline_handler: "PipelineHandler | None" = None
        self.webhook_secret: str = os.getenv("ODM_WEBHOOK_SECRET", "")
        self._event_queue: asyncio.Queue[ODMWebhookEvent] = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None

    def configure(
        self,
        store: GraphStore,
        pipeline_handler: "PipelineHandler",
    ) -> None:
        self.store = store
        self.pipeline_handler = pipeline_handler

    async def start_worker(self) -> None:
        """Arranca el worker que procesa eventos de la cola."""
        self._worker_task = asyncio.create_task(self._process_queue())
        logger.info("ODM webhook worker arrancado.")

    async def stop_worker(self) -> None:
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass

    async def enqueue(self, event: ODMWebhookEvent) -> None:
        await self._event_queue.put(event)

    async def _process_queue(self) -> None:
        """Worker que consume eventos uno a uno para no saturar el pipeline."""
        while True:
            try:
                event = await self._event_queue.get()
                if self.pipeline_handler:
                    await self.pipeline_handler.handle_odm_event(event)
                self._event_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error procesando evento ODM: %s", e)


# Instancia global — se configura al arrancar la app
state = ReceiverState()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    await state.start_worker()
    yield
    await state.stop_worker()


app = FastAPI(
    title="GrantsSurveyor — ODM Webhook Receiver",
    description="Receptor de notificaciones de OpenDataManager",
    version="2.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Verificación HMAC
# ---------------------------------------------------------------------------

def _verify_hmac(
    payload: bytes,
    signature_header: str | None,
    secret: str,
) -> bool:
    """
    Verifica la firma HMAC-SHA256 del webhook.
    ODM envía la firma en el header X-ODM-Signature como 'sha256=<hex>'.
    """
    if not signature_header:
        return False
    if not secret:
        logger.warning("ODM_WEBHOOK_SECRET no configurado — verificación desactivada.")
        return True

    expected_prefix = "sha256="
    if not signature_header.startswith(expected_prefix):
        return False

    received_sig = signature_header[len(expected_prefix):]
    expected_sig = hmac.new(
        secret.encode(), payload, hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(received_sig, expected_sig)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/webhook/odm")
async def receive_odm_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_odm_signature: str | None = Header(default=None),
) -> JSONResponse:
    """
    Endpoint principal. Recibe notificaciones de OpenDataManager.

    Responde 200 inmediatamente y procesa en background
    para no bloquear el scheduler de ODM.
    """
    payload = await request.body()

    # 1. Verificar firma HMAC
    if not _verify_hmac(payload, x_odm_signature, state.webhook_secret):
        logger.warning(
            "Webhook rechazado: firma HMAC inválida. "
            "Comprueba que ODM_WEBHOOK_SECRET coincide en ambos sistemas."
        )
        raise HTTPException(status_code=401, detail="Invalid signature")

    # 2. Parsear evento
    try:
        data = await request.json()
        event = ODMWebhookEvent.model_validate(data)
    except Exception as e:
        logger.error("Error parseando webhook de ODM: %s", e)
        raise HTTPException(status_code=400, detail=f"Invalid payload: {e}")

    logger.info(
        "Webhook ODM recibido: resource=%s version=%s records=%s",
        event.resource_code, event.version, event.record_count
    )

    # 3. Registrar en store si está configurado
    if state.store:
        # Log en todas las investigaciones suscritas a este resource
        _log_event_for_subscribed_investigations(event)

    # 4. Encolar para procesamiento asíncrono
    await state.enqueue(event)

    # 5. Responder inmediatamente
    return JSONResponse(
        status_code=200,
        content={
            "status": "accepted",
            "resource": event.resource_code,
            "version": event.version,
            "queued_at": datetime.utcnow().isoformat(),
        }
    )


@app.get("/health")
async def health() -> JSONResponse:
    """Health check — ODM puede usarlo para verificar que el receiver está vivo."""
    return JSONResponse({
        "status": "ok",
        "queue_size": state._event_queue.qsize(),
        "worker_active": (
            state._worker_task is not None
            and not state._worker_task.done()
        ),
    })


@app.get("/status")
async def status() -> JSONResponse:
    """Estado del receiver con información de configuración."""
    return JSONResponse({
        "status": "running",
        "webhook_secret_configured": bool(state.webhook_secret),
        "pipeline_configured": state.pipeline_handler is not None,
        "store_configured": state.store is not None,
        "queue_size": state._event_queue.qsize(),
    })


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _log_event_for_subscribed_investigations(event: ODMWebhookEvent) -> None:
    """Registra el evento en todas las investigaciones que tienen esta fuente."""
    if not state.store:
        return
    try:
        projects = state.store.list_projects()
        for project in projects:
            for inv in state.store.list_investigations(project.id):
                sub = inv.get_subscription(event.resource_code)
                if sub and sub.active:
                    status = "major_upgrade_pending" if event.is_major_upgrade else "received"
                    state.store.log_odm_event(
                        investigation_id=inv.id,
                        resource_code=event.resource_code,
                        version=event.version,
                        record_count=event.record_count,
                        status=status,
                    )
    except Exception as e:
        logger.error("Error registrando evento ODM en store: %s", e)


# ---------------------------------------------------------------------------
# PipelineHandler — interfaz que conecta el receiver con el pipeline
# ---------------------------------------------------------------------------

class PipelineHandler:
    """
    Interfaz entre el webhook receiver y el pipeline de evaluación.

    El Runner implementa esta interfaz y se registra en state.configure().
    Esto desacopla el receiver del pipeline — el receiver no importa Runner
    directamente, evitando dependencias circulares.
    """

    async def handle_odm_event(self, event: ODMWebhookEvent) -> None:
        """
        Procesa un evento ODM.

        Implementaciones:
          - Verificar si alguna investigación está suscrita a este resource
          - Si es major upgrade: notificar al analista y NO procesar
          - Si es minor/patch: lanzar el pipeline de evaluación
        """
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Lanzador del servidor
# ---------------------------------------------------------------------------

def run_receiver(
    store: GraphStore,
    pipeline_handler: PipelineHandler,
    host: str = "0.0.0.0",
    port: int | None = None,
) -> None:
    """
    Arranca el webhook receiver en un proceso o thread separado.

    Uso desde el notebook o el runner:
        from grants_surveyor.odm.webhook_receiver import run_receiver
        run_receiver(store=store, pipeline_handler=runner)
    """
    import uvicorn

    server_port = port or int(os.getenv("ODM_WEBHOOK_PORT", "8050"))
    state.configure(store=store, pipeline_handler=pipeline_handler)

    logger.info(
        "Arrancando ODM webhook receiver en %s:%d", host, server_port
    )
    uvicorn.run(app, host=host, port=server_port, log_level="info")


def run_receiver_background(
    store: GraphStore,
    pipeline_handler: PipelineHandler,
    host: str = "0.0.0.0",
    port: int | None = None,
) -> None:
    """
    Arranca el receiver en un thread de background.
    Útil para Jupyter donde el event loop principal ya está ocupado.
    """
    import threading
    import uvicorn

    server_port = port or int(os.getenv("ODM_WEBHOOK_PORT", "8050"))
    state.configure(store=store, pipeline_handler=pipeline_handler)

    config = uvicorn.Config(
        app, host=host, port=server_port,
        log_level="info", loop="asyncio",
    )
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    logger.info(
        "ODM webhook receiver arrancado en background en %s:%d",
        host, server_port
    )
