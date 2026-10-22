"""фейковый провайдер генерации. одно приложение, запускается два раза с PROVIDER_NAME=alpha / beta.

поведение меняется на лету через /_admin/config, чтоб тесты могли его специально ломать.
состояние в памяти, так что запускать в один процесс
"""
import asyncio
import json
import logging
import os
import random
import uuid
from typing import Literal

import httpx
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.signing import sign

NAME = os.environ.get("PROVIDER_NAME", "alpha")
SECRET = os.environ.get("WEBHOOK_SECRET", "dev-secret")

logging.basicConfig(level="INFO", format=f"%(asctime)s {NAME} %(levelname)s: %(message)s")
log = logging.getLogger(NAME)


class Behaviour(BaseModel):
    # через сколько секунд после POST /tasks придёт вебхук
    delay_min: float = Field(2.0, ge=0)
    delay_max: float = Field(20.0, ge=0)
    # доля задач которые закончатся failed, и с какой ошибкой
    fail_rate: float = Field(0.0, ge=0, le=1)
    fail_type: Literal["technical", "moderation", "payload", "random"] = "technical"
    # доля POST /tasks которые падают сразу
    sync_error_rate: float = Field(0.0, ge=0, le=1)
    # "503" - сразу отвечаем 503, "timeout" - задачу принимаем, но зависаем перед ответом
    sync_error: Literal["503", "timeout"] = "503"
    sync_hang_seconds: float = Field(30.0, ge=0)
    # каждый вебхук шлём столько раз, все сразу или по очереди
    duplicates: int = Field(1, ge=1, le=100)
    duplicates_concurrent: bool = True
    # доля задач у которых вебхук не придёт вообще
    drop_rate: float = Field(0.0, ge=0, le=1)
    # доля задач которые закончатся completed через late_delay секунд (позже чем нас ждут)
    late_rate: float = Field(0.0, ge=0, le=1)
    late_delay: float = Field(90.0, ge=0)


class TaskIn(BaseModel):
    model: str
    prompt: str
    ref_images: list[str] = Field(default_factory=list)
    callback_url: str
    client_ref: str | None = None


app = FastAPI(title=f"fake provider {NAME}")
behaviour = Behaviour()
jobs: dict[str, dict] = {}
by_key: dict[str, str] = {}
_background: set[asyncio.Task] = set()
_http: httpx.AsyncClient | None = None


def _client() -> httpx.AsyncClient:
    global _http
    if _http is None:
        _http = httpx.AsyncClient(timeout=10)
    return _http


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _background.add(task)
    task.add_done_callback(_background.discard)


def _plan_outcome(b: Behaviour, external_id: str, client_ref: str | None) -> tuple[float, dict | None]:
    """сразу решаем что и когда задача ответит. None = не ответит никогда"""
    base = {"external_id": external_id, "client_ref": client_ref}
    if random.random() < b.drop_rate:
        return 0, None
    if random.random() < b.late_rate:
        return b.late_delay, {**base, "status": "completed", "result_url": f"https://cdn.{NAME}.example/{external_id}.bin"}
    delay = random.uniform(b.delay_min, max(b.delay_min, b.delay_max))
    if random.random() < b.fail_rate:
        error = random.choice(["technical", "moderation", "payload"]) if b.fail_type == "random" else b.fail_type
        return delay, {**base, "status": "failed", "error_type": error}
    return delay, {**base, "status": "completed", "result_url": f"https://cdn.{NAME}.example/{external_id}.bin"}


async def _post_once(job: dict, body: bytes) -> None:
    headers = {"Content-Type": "application/json", "X-Signature": sign(body, SECRET)}
    # ретраим как настоящий провайдер: на сетевые ошибки и 5xx, на 4xx нет
    for attempt in range(5):
        try:
            response = await _client().post(job["callback_url"], content=body, headers=headers)
            job["deliveries"].append(response.status_code)
            if response.status_code < 500:
                return
        except httpx.HTTPError as exc:
            job["deliveries"].append(type(exc).__name__)
        await asyncio.sleep(0.5 * 2 ** attempt)


async def _run_job(external_id: str, delay: float, payload: dict | None, copies: int, concurrent: bool) -> None:
    job = jobs[external_id]
    if payload is None:
        job["state"] = "dropped"
        return
    await asyncio.sleep(delay)
    body = json.dumps(payload).encode()
    job["state"] = "sending"
    if concurrent:
        await asyncio.gather(*(_post_once(job, body) for _ in range(copies)))
    else:
        for _ in range(copies):
            await _post_once(job, body)
    job["state"] = "sent"
    log.info("job %s reported %s x%d -> %s", external_id, payload["status"], copies, job["deliveries"])


@app.post("/tasks", status_code=202)
async def create_task(body: TaskIn, idempotency_key: str | None = Header(default=None)):
    key = idempotency_key or body.client_ref
    if key and key in by_key:
        return {"external_id": by_key[key]}

    b = behaviour.model_copy()
    sync_fail = random.random() < b.sync_error_rate
    if sync_fail and b.sync_error == "503":
        return JSONResponse({"error": "temporarily unavailable"}, status_code=503)

    external_id = f"{NAME}-{uuid.uuid4().hex[:16]}"
    delay, payload = _plan_outcome(b, external_id, body.client_ref)
    jobs[external_id] = {
        "external_id": external_id,
        "client_ref": body.client_ref,
        "callback_url": body.callback_url,
        "model": body.model,
        "planned": payload,
        "delay": delay,
        "state": "pending",
        "deliveries": [],
    }
    if key:
        by_key[key] = external_id
    _spawn(_run_job(external_id, delay, payload, b.duplicates, b.duplicates_concurrent))

    if sync_fail:
        # задача принята и ответит, но клиент отвалится по таймауту и id не увидит
        await asyncio.sleep(b.sync_hang_seconds)
    return {"external_id": external_id}


@app.get("/health")
async def health():
    return {"status": "ok", "provider": NAME}


@app.get("/_admin/config")
async def get_config():
    return behaviour


@app.put("/_admin/config")
async def put_config(changes: dict):
    global behaviour
    try:
        behaviour = Behaviour(**{**behaviour.model_dump(), **changes})
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    return behaviour


@app.post("/_admin/reset")
async def reset():
    global behaviour
    behaviour = Behaviour()
    return behaviour


@app.get("/_admin/jobs/{external_id}")
async def get_job(external_id: str):
    if external_id not in jobs:
        raise HTTPException(404, "no such job")
    return jobs[external_id]
