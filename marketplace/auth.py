import hashlib
import hmac
import os
from datetime import datetime, timedelta, timezone

import jwt
from decimal import Decimal
from pydantic import BaseModel, ConfigDict, field_validator
from typing import get_args


class ApiModel(BaseModel):
    model_config = ConfigDict(json_encoders={Decimal: float})

    @field_validator('*', mode='before')
    @classmethod
    def check_scalar_types(cls, value, info):
        annotation = cls.model_fields[info.field_name].annotation
        types = get_args(annotation) or (annotation,)
        if value is None:
            return value  # Обязательность и nullable проверяет сгенерированная модель.
        if int in types and type(value) is not int:
            raise ValueError('Ожидается целое число, не строка или boolean')
        if Decimal in types and (type(value) not in (int, float, Decimal)):
            raise ValueError('Ожидается число')
        if datetime in types and type(value) not in (str, datetime):
            raise ValueError('Ожидается дата и время в формате ISO 8601')
        if bool in types and type(value) is not bool:
            raise ValueError('Ожидается boolean')
        return value

from fastapi import Request

SECRET = os.environ['JWT_SECRET']


class ApiError(Exception):
    def __init__(self, code, status, message, details=None):
        self.code, self.status, self.message, self.details = code, status, message, details


def password_hash(password, salt=None):
    salt = salt or os.urandom(16).hex()
    digest = hashlib.pbkdf2_hmac('sha256', password.encode(), bytes.fromhex(salt), 260000).hex()
    return salt + ':' + digest


def password_matches(password, stored):
    return hmac.compare_digest(password_hash(password, stored.split(':')[0]), stored)


def tokens(user):
    now = datetime.now(timezone.utc)
    claims = {'sub': str(user['id']), 'role': user['role'], 'iat': now}
    return {
        'access_token': jwt.encode({**claims, 'type': 'access', 'exp': now + timedelta(minutes=20)}, SECRET, algorithm='HS256'),
        'refresh_token': jwt.encode({**claims, 'type': 'refresh', 'exp': now + timedelta(days=14)}, SECRET, algorithm='HS256'),
        'token_type': 'bearer',
    }


def decode_token(token, kind):
    try:
        claims = jwt.decode(token, SECRET, algorithms=['HS256'], options={'require': ['exp','iat','sub','role','type']})
        if claims['type'] != kind or claims['role'] not in ('USER','SELLER','ADMIN'):
            raise jwt.InvalidTokenError()
        return {'id': int(claims['sub']), 'role': claims['role']}
    except (jwt.InvalidTokenError, ValueError, TypeError) as error:
        code = 'REFRESH_TOKEN_INVALID' if kind == 'refresh' else 'TOKEN_EXPIRED' if isinstance(error, jwt.ExpiredSignatureError) else 'TOKEN_INVALID'
        raise ApiError(code, 401, 'Недействительный или просроченный токен') from error


def current_user(request: Request):
    scheme, _, token = request.headers.get('Authorization', '').partition(' ')
    if scheme.lower() != 'bearer' or not token:
        raise ApiError('TOKEN_INVALID', 401, 'Нужен Bearer access token')
    user = decode_token(token, 'access')
    request.state.user_id = user['id']
    return user
