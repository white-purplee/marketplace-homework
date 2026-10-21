import json
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import yaml
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException

from auth import ApiError
from generated.routes import router
from service import create_admin


@asynccontextmanager
async def lifespan(app):
    create_admin()
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(router)
contract = yaml.safe_load(Path(__file__).with_name('openapi.yaml').read_text())
app.openapi = lambda: contract


def error_response(code, status, message, details=None):
    body = {'error_code': code, 'message': message}
    if details is not None:
        body['details'] = details
    return JSONResponse(body, status_code=status)


@app.exception_handler(ApiError)
async def api_error(request, error):
    return error_response(error.code, error.status, error.message, error.details)


@app.exception_handler(RequestValidationError)
async def validation_error(request, error):
    details = {}
    for item in error.errors():
        field = '.'.join(map(str, item['loc']))
        details.setdefault(field, []).append(item['msg'])
    return error_response('VALIDATION_ERROR', 400, 'Некорректные данные', details)


@app.exception_handler(psycopg.DataError)
async def data_error(request, error):
    return error_response('VALIDATION_ERROR', 400, 'Значение не помещается в поле БД', {'data': 'Проверьте числовые значения'})


@app.exception_handler(HTTPException)
async def http_error(request, error):
    return error_response('HTTP_ERROR', error.status_code, str(error.detail))


def mask(value):
    if isinstance(value, dict):
        return {k: '***' if any(word in k.lower() for word in ('password','token','secret')) else mask(v) for k, v in value.items()}
    if isinstance(value, list):
        return [mask(item) for item in value]
    return value


@app.middleware('http')
async def log_request(request: Request, call_next):
    started = time.perf_counter()
    request_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc).isoformat()
    body = None
    if request.method in ('POST','PUT','DELETE','PATCH'):
        raw = await request.body()
        try:
            body = mask(json.loads(raw)) if raw else None
        except (ValueError, UnicodeDecodeError):
            body = '[invalid JSON]'
    try:
        response = await call_next(request)
    except Exception:
        response = error_response('INTERNAL_ERROR', 500, 'Внутренняя ошибка сервера')
    response.headers['X-Request-Id'] = request_id
    record = {
        'request_id': request_id, 'method': request.method, 'endpoint': request.url.path,
        'status_code': response.status_code,
        'duration_ms': round((time.perf_counter()-started)*1000, 2),
        'user_id': getattr(request.state, 'user_id', None), 'timestamp': timestamp,
    }
    if request.method in ('POST','PUT','DELETE','PATCH'):
        record['request_body'] = body
    print(json.dumps(record, ensure_ascii=False), flush=True)
    return response
