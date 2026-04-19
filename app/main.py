"""
API mediador entre clientes y el proveedor de notificaciones (reto backend-python-test).

Flujo: POST /v1/requests (registro, estado queued) → POST .../process (entrega asíncrona)
→ GET ... (estado actual). El proveedor simulado escucha en 127.0.0.1:3001 (misma red
Docker que la app cuando se usa network_mode: service:provider).
"""
import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, Literal, Optional

import httpx
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

logger = logging.getLogger(__name__)

API_KEY = "test-dev-2026"
NOTIFY_PATH = "/v1/notify"
PROVIDER_BASE = "http://127.0.0.1:3001"

StatusLiteral = Literal["queued", "processing", "sent", "failed"]
TypeLiteral = Literal["email", "sms", "push"]

# id -> {to, message, type, status}
_store: Dict[str, Dict[str, Any]] = {}
_store_lock = asyncio.Lock()
# Limita llamadas concurrentes al proveedor (umbral ~50 en el simulador).
_provider_sem = asyncio.Semaphore(45)
_http_client: Optional[httpx.AsyncClient] = None


class NotificationCreate(BaseModel):
    """
    Cuerpo de POST /v1/requests.
    """

    to: str
    message: str
    type: TypeLiteral


class NotificationCreateResponse(BaseModel):
    """
    Respuesta 201 de creación.
    """

    id: str


class NotificationStatusResponse(BaseModel):
    """
    Respuesta de GET /v1/requests/{id}.
    """

    id: str
    status: StatusLiteral


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Cliente HTTP compartido para el proveedor (pool de conexiones).
    """
    global _http_client
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=50)
    _http_client = httpx.AsyncClient(
        base_url=PROVIDER_BASE,
        timeout=httpx.Timeout(60.0),
        limits=limits,
    )
    yield
    await _http_client.aclose()
    _http_client = None


app = FastAPI(
    title="Notification Service (Technical Test)",
    lifespan=lifespan,
)


@retry(
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError)),
    stop=stop_after_attempt(18),
    wait=wait_exponential_jitter(initial=0.15, max=10),
    reraise=True,
)
async def _post_notify(payload: Dict[str, Any]
) -> httpx.Response:
    """
    POST /v1/notify al proveedor; reintenta ante 429, 500 y errores de red.
    """
    assert _http_client is not None
    response = await _http_client.post(
        NOTIFY_PATH,
        json=payload,
        headers={
            "X-API-Key": API_KEY,
            "Content-Type": "application/json",
        },
    )
    if response.status_code in (429, 500):
        response.raise_for_status()
    if response.status_code != 200:
        response.raise_for_status()
    return response


async def _deliver(req_id: str, payload: Dict[str, Any]
) -> None:
    """
    Ejecuta la entrega en segundo plano y actualiza el estado a sent o failed.
    """
    try:
        async with _provider_sem:
            await _post_notify(payload)
    except Exception as exc:
        logger.warning("Delivery failed for %s: %s", req_id, exc)
        async with _store_lock:
            rec = _store.get(req_id)
            if rec is not None and rec["status"] == "processing":
                rec["status"] = "failed"
        return

    async with _store_lock:
        rec = _store.get(req_id)
        if rec is not None:
            rec["status"] = "sent"


@app.post(
    "/v1/requests",
    response_model=NotificationCreateResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_request(body: NotificationCreate
) -> NotificationCreateResponse:
    """Registra la notificación (queued); no contacta al proveedor."""
    req_id = str(uuid.uuid4())
    async with _store_lock:
        _store[req_id] = {
            "to": body.to,
            "message": body.message,
            "type": body.type,
            "status": "queued",
        }
    return NotificationCreateResponse(id=req_id)


@app.post(
    "/v1/requests/{req_id}/process",
    status_code=status.HTTP_202_ACCEPTED,
)
async def process_request(req_id: str
) -> None:
    """
    Pasa a processing y lanza la entrega al proveedor (idempotente si sent/processing).
    """
    async with _store_lock:
        rec = _store.get(req_id)
        if rec is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Request not found",
            )
        if rec["status"] == "sent":
            return None
        if rec["status"] == "processing":
            return None
        rec["status"] = "processing"
        payload = {
            "to": rec["to"],
            "message": rec["message"],
            "type": rec["type"],
        }

    asyncio.create_task(_deliver(req_id, payload))
    return None


@app.get(
    "/v1/requests/{req_id}",
    response_model=NotificationStatusResponse,
)
async def get_request(req_id: str
) -> NotificationStatusResponse:
    """
    Devuelve el estado actual de la solicitud.
    """
    async with _store_lock:
        rec = _store.get(req_id)
        if rec is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Request not found",
            )
        return NotificationStatusResponse(id=req_id, status=rec["status"])
