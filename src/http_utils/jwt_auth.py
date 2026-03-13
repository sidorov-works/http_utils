# http_utils/jwt_auth.py

"""
JWT-based аутентификация для межсервисного взаимодействия.
Использует HS256 для подписи токенов с коротким сроком жизни.

В данном модуле - функции необходимы на стороне клиента
"""

from jose import jwt
import time
from typing import Optional, Dict, Any, List

import logging
logger = logging.getLogger(__name__)

def create_jwt_token(
        secret_key: str,
        token_expire_seconds: int = 30,
        algorithm: str = "HS256",
        service_name: str = "unknown",
        extra_payload: Optional[Dict[str, Any]] = None
    ) -> str:
    """
    Создает JWT токен для межсервисной аутентификации.
    
    Args:
        secret_key: Секретный ключ для подписи токена. Должен быть известен и клиенту, и серверу.
        token_expire_seconds: Время жизни токена в секундах. По умолчанию 30 секунд.
        algorithm: Алгоритм подписи JWT. По умолчанию "HS256".
        service_name: Идентификатор сервиса-отправителя. Будет добавлен в поля iss и service.
        extra_payload: Дополнительные данные для включения в токен. 
                      Например: {"request_id": "123", "user_id": 42}. По умолчанию None.
    
    Returns:
        str: JWT токен в формате строки
    
    Example:
        >>> token = create_jwt_token(
        ...     secret_key="my-secret",
        ...     service_name="encoder-client",
        ...     extra_payload={"request_id": "550e8400-e29b-41d4"}
        ... )
    
    Пример payload созданного токена:
        {
            "iss": "encoder-client",          # отправитель
            "iat": 1700000000,                 # время выпуска
            "exp": 1700000030,                 # истекает через 30 сек
            "service": "encoder-client",       # дублируем для удобства
            "request_id": "550e8400-e29b-41d4" # из extra_payload
        }
    
    Note:
        - Токен создается с коротким временем жизни (по умолчанию 30с) для безопасности
        - Всегда используйте HTTPS в production для защиты токена при передаче
        - Алгоритм HS256 требует, чтобы секретный ключ был известен обеим сторонам
    """
    current_time = int(time.time())
    
    # Базовый payload
    payload = {
        "iss": service_name,                          # кто выпустил токен
        "iat": current_time,                          # когда выпущен
        "exp": current_time + token_expire_seconds,   # когда истекает
        "service": service_name,                      # имя сервиса
    }
    
    # Добавляем дополнительные данные, если есть
    if extra_payload:
        payload.update(extra_payload)
    
    # Создаем подписанный токен
    token = jwt.encode(payload, secret_key, algorithm=algorithm)
    
    logger.debug(f"Created JWT token for {service_name}, expires in {token_expire_seconds}s")
    return token