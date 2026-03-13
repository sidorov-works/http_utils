# http_utils/signed.py

"""
Функции для добавления в класс RetryableHTTPClient
возможности автоматической подписи (JWT токена).

ВНИМАНИЕ: функции в данном модуле работают ТОЛЬКО для RetryableHTTPClient, 
поскольку используют конкретные названия его методов.
"""

from typing import Optional, Dict, Any

from .jwt_auth import create_jwt_token
from .http_client import RetryableHTTPClient

from enum import Enum

import logging
logger = logging.getLogger(__name__)


class AuthType(Enum):
    """Типы аутентификации для межсервисного взаимодействия"""
    JWT_AUTH = "jwt"               # JWT токен в Bearer-схеме
    SECRET_HEADER_AUTH = "secret_header"  # Простой секретный ключ в заголовке


def create_signed_client(
    base_client: RetryableHTTPClient,
    secret: str,
    service_name: str = "encoder-client",
    auth_type: AuthType = AuthType.JWT_AUTH,
    jwt_algorithm: Optional[str] = "HS256",
    jwt_token_expire: Optional[int] = 30,
    jwt_extra_payload: Optional[Dict[str, Any]] = None,
    auth_header_name: str = "Authorization",
    auth_header_scheme: str = "Bearer"
) -> RetryableHTTPClient:
    """
    Оборачивает RetryableHTTPClient для автоматического добавления подписи в запросы.

    В зависимости от auth_type может добавлять:
    - JWT токен (для AuthType.JWT_AUTH) - создается на каждый запрос с коротким сроком жизни
    - Простой секретный ключ (для AuthType.SECRET_HEADER_AUTH) - добавляется как есть

    Args:
        base_client: Исходный экземпляр RetryableHTTPClient, методы которого будут обернуты
        secret: Секретный ключ. Для JWT_AUTH - ключ подписи токена, 
                для SECRET_HEADER_AUTH - значение, подставляемое в заголовок
        service_name: Идентификатор сервиса-отправителя. Используется в JWT токене 
                     как iss и service. По умолчанию "encoder-client"
        auth_type: Тип аутентификации из перечисления AuthType. 
                  По умолчанию AuthType.JWT_AUTH
        jwt_algorithm: Алгоритм подписи JWT токена (например, "HS256", "RS256").
                      Если передано None, используется значение по умолчанию "HS256".
                      Применяется только при auth_type=JWT_AUTH
        jwt_token_expire: Время жизни JWT токена в секундах. Токен создается заново 
                         на каждый запрос. Если передано None, используется 30 секунд.
                         Применяется только при auth_type=JWT_AUTH
        jwt_extra_payload: Дополнительные данные для включения в payload JWT токена.
                          Например, можно передать {"request_id": "uuid", "role": "admin"}.
                          Применяется только при auth_type=JWT_AUTH
        auth_header_name: Имя HTTP заголовка, в который будет помещена подпись.
                         По умолчанию "Authorization" (стандартный заголовок аутентификации)
        auth_header_scheme: Схема аутентификации для заголовка. Добавляется перед подписью
                           через пробел. Например, "Bearer", "Basic", "Token". 
                           Если передать None или пустую строку, схема не добавляется.
                           По умолчанию "Bearer"

    Returns:
        RetryableHTTPClient: Тот же экземпляр клиента, но с обернутыми методами:
            - post_with_retry
            - put_with_retry
            - delete_with_retry
            - get_with_retry
            Все эти методы будут автоматически добавлять подпись в заголовки запроса.

    Raises:
        ValueError: Если secret пустой или service_name пустой
        Exception: Любые исключения от create_jwt_token при создании подписи

    Примеры использования:

        1. Базовая JWT-аутентификация:
        >>> client = RetryableHTTPClient()
        >>> signed_client = create_signed_client(
        ...     client,
        ...     secret="my-secret-key",
        ...     service_name="rag-worker"
        ... )
        >>> response = await signed_client.post_with_retry("http://encoder/info")
        # Заголовок: Authorization: Bearer <jwt-токен>

        2. JWT с дополнительными данными:
        >>> signed_client = create_signed_client(
        ...     client,
        ...     secret="my-secret-key",
        ...     service_name="rag-worker",
        ...     jwt_extra_payload={"request_id": "123", "priority": "high"},
        ...     jwt_token_expire=60  # токен живет 60 секунд
        ... )

        3. Аутентификация через простой секретный заголовок:
        >>> signed_client = create_signed_client(
        ...     client,
        ...     secret="api-key-12345",
        ...     auth_type=AuthType.SECRET_HEADER_AUTH,
        ...     auth_header_name="X-API-Key",
        ...     auth_header_scheme=None  # без схемы, просто значение
        ... )
        >>> response = await signed_client.get_with_retry("http://api.example.com/data")
        # Заголовок: X-API-Key: api-key-12345

        4. Использование с уже существующими заголовками:
        >>> response = await signed_client.post_with_retry(
        ...     "http://example.com",
        ...     headers={"Content-Type": "application/json"},
        ...     json={"data": "value"}
        ... )
        # Итоговые заголовки: 
        #   Content-Type: application/json
        #   Authorization: Bearer <jwt-токен>

    Как работает:
        1. Сохраняет оригинальные методы клиента
        2. Для каждого HTTP метода создает асинхронную обертку
        3. В обертке вызывает add_auth_header, которая:
           - Создает подпись (JWT токен или использует secret как есть)
           - Добавляет подпись в заголовки с указанным именем и схемой
           - Сохраняет все существующие заголовки
        4. Вызывает оригинальный метод с обновленными заголовками

    Note:
        - JWT токен создается заново на каждый запрос для обеспечения короткого времени жизни
        - Если в kwargs уже есть headers, они сохраняются и дополняются заголовком аутентификации
        - Методы клиента изменяются "на месте" (in-place), возвращается тот же объект
        - Для отключения аутентификации нужно создать новый клиент или перезаписать методы вручную
    """
    
    # Функция для добавления токена в заголовки
    def add_auth_header(kwargs: Dict) -> Dict:
        """Добавляет JWT токен в заголовки запроса"""
        # Если какие-то headers уже были среди именованных аргументов, сохраняем их
        headers = dict(kwargs.get("headers", {}))
        
        # Если требуется авторизация с JWT-токеном, генерируем его
        if auth_type == AuthType.JWT_AUTH:
            sign = create_jwt_token(
                secret_key=secret,
                service_name=service_name,
                algorithm=jwt_algorithm,
                token_expire_seconds=jwt_token_expire,
                extra_payload=jwt_extra_payload
            )
        # Если же обычный secret header, то переданный secret сам и будет подписью
        elif auth_type == AuthType.SECRET_HEADER_AUTH:
            sign = secret
        # Если неизвестный вид аутентификации, 
        # логируем ошибку и ничего не добавляем в заголовки
        else:
            logger.warning(f"Unknown auth_type: {auth_type}")
            return kwargs
        
        # Записываем подпись в указанный заголовок с указанной схемой
        if auth_header_scheme:
            headers[auth_header_name] = f"{auth_header_scheme} {sign}"
        else:
            headers[auth_header_name] = sign
        
        # Обновляем заголовки
        kwargs["headers"] = headers # тут и наши, и те, что были изначально
        return kwargs
    
    # Сохраняем оригинальные методы
    original_post = base_client.post_with_retry
    original_put = base_client.put_with_retry
    original_delete = base_client.delete_with_retry
    original_get = base_client.get_with_retry
    
    # Оборачиваем POST
    async def signed_post(*args, **kwargs):
        kwargs = add_auth_header(kwargs)
        return await original_post(*args, **kwargs)
    
    # Оборачиваем PUT 
    async def signed_put(*args, **kwargs):
        kwargs = add_auth_header(kwargs)
        return await original_put(*args, **kwargs)
    
    # Оборачиваем DELETE 
    async def signed_delete(*args, **kwargs):
        kwargs = add_auth_header(kwargs)
        return await original_delete(*args, **kwargs)
    
    # Оборачиваем GET 
    async def signed_get(*args, **kwargs):
        kwargs = add_auth_header(kwargs)
        return await original_get(*args, **kwargs)
    
    # Заменяем методы
    base_client.post_with_retry = signed_post
    base_client.put_with_retry = signed_put
    base_client.delete_with_retry = signed_delete
    base_client.get_with_retry = signed_get
    
    logger.info(f"JWT signing enabled for client (service: {service_name})")
    return base_client