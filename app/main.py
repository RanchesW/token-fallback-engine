import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Literal

import httpx
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError, model_validator

from . import engine, payments
from .config import load_settings
from .db import create_pool
from .dispatch import Background, Dispatcher
from .signing import verify
from .watchdog import Watchdog

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format=f"%(asctime)s pid={os.getpid()} %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("app")


class GenerateIn(BaseModel):
    user_id: int
    model_slug: str
    prompt: str = Field(min_length=1)
    ref_images: list[str] = Field(default_factory=list)


class ProviderEvent(BaseModel):
    external_id: str
    status: Literal["completed", "failed"]
    result_url: str | None = None
    error_type: str | None = None
    client_ref: str | None = None

    @model_validator(mode="after")
    def _completed_has_url(self):
        if self.status == "completed" and not self.result_url:
            raise ValueError("completed event without result_url")
        return self


class PaymentEvent(BaseModel):
    payment_id: str = Field(min_length=1)
    user_id: int
    tokens: int = Field(gt=0)
    status: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    pool = await create_pool(settings.database_url, settings.db_pool_size)
    http = httpx.AsyncClient()
    background = Background()
    dispatcher = Dispatcher(pool, http, settings)
    watchdog = asyncio.create_task(Watchdog(pool, settings, dispatcher, background).run(), name="watchdog")

    app.state.settings = settings
    app.state.pool = pool
    app.state.background = background
    app.state.dispatcher = dispatcher
    try:
        yield
    finally:
        watchdog.cancel()
        await background.close(timeout=settings.submit_timeout)
        await http.aclose()
        await pool.close()


app = FastAPI(title="token-fallback-engine", lifespan=lifespan)


@app.exception_handler(engine.DomainError)
async def domain_error(_: Request, exc: engine.DomainError) -> JSONResponse:
    return JSONResponse({"error": exc.code, "detail": exc.message}, status_code=exc.status_code)


async def _signed_body(request: Request) -> bytes:
    body = await request.body()
    if not verify(body, request.headers.get("X-Signature"), request.app.state.settings.webhook_secret):
        raise HTTPException(401, "bad signature")
    return body


def _dispatch_later(request: Request, attempt_id: uuid.UUID | None) -> None:
    if attempt_id is not None:
        request.app.state.background.spawn(
            request.app.state.dispatcher.dispatch(attempt_id), name=f"dispatch-{attempt_id}"
        )


@app.get("/health")
async def health(request: Request):
    async with request.app.state.pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    return {"status": "ok", "provider_timeout": request.app.state.settings.provider_timeout}


@app.post("/generate", status_code=202)
async def generate(
    body: GenerateIn, request: Request, idempotency_key: str | None = Header(default=None)
):
    async with request.app.state.pool.acquire() as conn:
        created = await engine.create_task(
            conn,
            request.app.state.settings,
            user_id=body.user_id,
            model_slug=body.model_slug,
            prompt=body.prompt,
            ref_images=body.ref_images,
            idempotency_key=idempotency_key,
        )
    if created.attempt_id is None:
        return JSONResponse({"task_id": str(created.task_id)}, status_code=200)
    # отвечаем клиенту сразу, отправка идёт в фоне и переживёт падение воркера
    _dispatch_later(request, created.attempt_id)
    return {"task_id": str(created.task_id)}


@app.post("/webhooks/providers/{provider}")
async def provider_webhook(provider: str, request: Request):
    body = await _signed_body(request)
    if provider not in request.app.state.settings.provider_urls:
        raise HTTPException(404, f"unknown provider {provider!r}")
    try:
        event = ProviderEvent.model_validate_json(body)
    except ValidationError as exc:
        raise HTTPException(422, exc.errors(include_url=False, include_context=False)) from None

    async with request.app.state.pool.acquire() as conn:
        attempt_id = await engine.find_attempt(conn, provider, event.external_id, event.client_ref)
        if attempt_id is None:
            raise HTTPException(404, f"unknown external_id {event.external_id!r}")
        if event.status == "completed":
            settled = await engine.settle_attempt(
                conn, request.app.state.settings, attempt_id,
                result_url=event.result_url, external_id=event.external_id,
            )
        else:
            settled = await engine.settle_attempt(
                conn, request.app.state.settings, attempt_id,
                error_type=engine.normalize_error(event.error_type), external_id=event.external_id,
            )
    _dispatch_later(request, settled.next_attempt)
    return {"result": settled.outcome}


@app.post("/webhooks/payments")
async def payment_webhook(request: Request):
    body = await _signed_body(request)
    try:
        event = PaymentEvent.model_validate_json(body)
    except ValidationError as exc:
        raise HTTPException(422, exc.errors(include_url=False, include_context=False)) from None
    async with request.app.state.pool.acquire() as conn:
        result = await payments.apply_payment(
            conn, payment_id=event.payment_id, user_id=event.user_id, tokens=event.tokens, status=event.status
        )
    return {"result": result}


@app.get("/tasks/{task_id}")
async def get_task(task_id: uuid.UUID, request: Request):
    async with request.app.state.pool.acquire() as conn:
        task = await conn.fetchrow(
            "SELECT t.*, m.slug FROM tasks t JOIN models m ON m.id = t.model_id WHERE t.id = $1", task_id
        )
        if task is None:
            raise HTTPException(404, "task not found")
        attempts = await conn.fetch("SELECT * FROM attempts WHERE task_id = $1 ORDER BY seq", task_id)
        refunded = await conn.fetchval("SELECT EXISTS (SELECT 1 FROM ledger WHERE op_key = $1)", f"refund:{task_id}")

    plan = task["plan"]
    history = [
        {
            "seq": a["seq"],
            "provider": a["provider"],
            "priority": plan[a["leg_index"]]["priority"],
            "status": a["status"],
            "error_type": a["error_type"],
            "late_completed": a["late"],
            "external_id": a["external_id"],
            "started_at": a["created_at"],
            "submitted_at": a["submitted_at"],
            "finished_at": a["finished_at"],
        }
        for a in attempts
    ]
    current = next((h for h in history if h["status"] in engine.ACTIVE), None)
    if current is None and task["status"] == "completed":
        current = next((h for h in history if h["status"] == "completed"), None)
    if current is None and history:
        current = history[-1]

    return {
        "task_id": task["id"],
        "user_id": task["user_id"],
        "model": task["slug"],
        "status": task["status"],
        "price": task["price"],
        "result_url": task["result_url"],
        "failure_reason": task["failure_reason"],
        "refunded": refunded,
        "current_leg": current and {k: current[k] for k in ("seq", "provider", "priority", "status")},
        "attempts": history,
        "created_at": task["created_at"],
        "updated_at": task["updated_at"],
        "finished_at": task["finished_at"],
    }


@app.get("/users/{user_id}/balance")
async def get_balance(user_id: int, request: Request, limit: int = 20):
    async with request.app.state.pool.acquire() as conn:
        balance = await conn.fetchval("SELECT balance FROM users WHERE id = $1", user_id)
        if balance is None:
            raise HTTPException(404, "user not found")
        rows = await conn.fetch(
            "SELECT id, delta, kind, task_id, payment_id, balance_after, created_at "
            "FROM ledger WHERE user_id = $1 ORDER BY id DESC LIMIT $2",
            user_id, min(max(limit, 1), 200),
        )
    return {"user_id": user_id, "balance": balance, "ledger": [dict(row) for row in rows]}
