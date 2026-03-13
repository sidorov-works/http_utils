# HTTP Utils

Асинхронный HTTP-клиент с повторными попытками и автоматической подписью запросов для межсервисного взаимодействия.

## Возможности

- Автоматические повторные попытки с экспоненциальной задержкой и jitter
- Контроль общего времени выполнения всех попыток (total_timeout)
- Автоматическая подпись запросов:
  - JWT токены с коротким сроком жизни (HS256)
  - Простые секретные заголовки (X-API-Key и т.п.)
- HTTP/2 поддержка через httpx
- Пул соединений с настраиваемыми лимитами
- Гибкая настройка retry-логики
- Полная типизация и докстринги

## Установка

### Из git-репозитория

```bash
# Через SSH
pip install git+ssh://git@github.com/yourusername/retryable_http_client.git@v0.1.0

# Через HTTPS
pip install git+https://github.com/yourusername/retryable_http_client.git@v0.1.0
```

### Локальная установка для разработки

```bash
git clone https://github.com/yourusername/retryable_http_client.git
cd retryable_http_client
pip install -e .[dev]
```

### Как зависимость в другом проекте

В pyproject.toml:
```toml
[project]
dependencies = [
    "http-utils @ git+https://github.com/yourusername/retryable_http_client.git@v0.1.0"
]
```

В requirements.txt:
```
http-utils @ git+https://github.com/yourusername/retryable_http_client.git@v0.1.0
```

## Быстрый старт

### Базовое использование

```python
import asyncio
from http_utils import RetryableHTTPClient

async def main():
    # Создаем клиент с кастомными настройками
    client = RetryableHTTPClient(
        base_timeout=5.0,      # таймаут на один запрос
        max_retries=3,          # максимум повторных попыток
        base_delay=1.0,         # начальная задержка
        max_delay=10.0,         # максимальная задержка
        total_timeout=30.0      # общий таймаут на все попытки
    )
    
    # Используем как контекстный менеджер
    async with client:
        response = await client.get_with_retry("https://api.example.com/data")
        print(response.json())

asyncio.run(main())
```

### JWT-аутентификация

```python
from http_utils import RetryableHTTPClient, create_signed_client, AuthType

async def main():
    # Базовый клиент
    base_client = RetryableHTTPClient()
    
    # Оборачиваем для автоматической JWT-подписи
    signed_client = create_signed_client(
        base_client=base_client,
        secret="your-secret-key",           # секрет для подписи JWT
        service_name="my-service",           # имя сервиса-отправителя
        auth_type=AuthType.JWT_AUTH,         # используем JWT
        jwt_algorithm="HS256",                # алгоритм подписи
        jwt_token_expire=30,                   # токен живет 30 секунд
        jwt_extra_payload={"request_id": "123"} # доп. данные в токене
    )
    
    async with signed_client:
        # Заголовок Authorization: Bearer <JWT-токен> добавится автоматически
        response = await signed_client.post_with_retry(
            "https://api.example.com/data",
            json={"key": "value"}
        )

asyncio.run(main())
```

### Аутентификация через секретный заголовок

```python
signed_client = create_signed_client(
    base_client=base_client,
    secret="api-key-12345",                   # значение заголовка
    auth_type=AuthType.SECRET_HEADER_AUTH,    # используем простой заголовок
    auth_header_name="X-API-Key",              # кастомное имя заголовка
    auth_header_scheme=None                     # без схемы (просто значение)
)

# Заголовок: X-API-Key: api-key-12345
```

## Документация API

### RetryableHTTPClient

Основной класс клиента с повторными попытками.

**Параметры конструктора:**

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| base_timeout | float | 10 | Таймаут каждого отдельного запроса в секундах |
| max_retries | int | 3 | Максимальное количество повторных попыток |
| base_delay | float | 1.0 | Начальная задержка перед повторной попыткой в секундах |
| max_delay | float | 30.0 | Максимальная задержка между попытками в секундах |
| total_timeout | Optional[float] | None | Общий таймаут на все попытки в секундах |

**Методы:**

- `request_with_retry(method, url, headers=None, success_statuses={200}, **kwargs)` - базовый метод для любого HTTP метода
- `get_with_retry(url, headers=None, success_statuses={200}, **kwargs)` - GET запрос
- `post_with_retry(url, headers=None, success_statuses={200}, **kwargs)` - POST запрос
- `put_with_retry(url, headers=None, success_statuses={200}, **kwargs)` - PUT запрос
- `delete_with_retry(url, headers=None, success_statuses={200, 204}, **kwargs)` - DELETE запрос
- `close()` - явное закрытие клиента

### create_jwt_token

Создание JWT токена для межсервисной аутентификации.

```python
from http_utils import create_jwt_token

token = create_jwt_token(
    secret_key="secret",
    service_name="my-service",
    token_expire_seconds=30,
    algorithm="HS256",
    extra_payload={"request_id": "123"}
)
```

**Параметры:**

- `secret_key: str` - секретный ключ для подписи токена
- `service_name: str` - идентификатор сервиса-отправителя
- `token_expire_seconds: int` - время жизни токена в секундах (по умолчанию 30)
- `algorithm: str` - алгоритм подписи (по умолчанию "HS256")
- `extra_payload: Optional[Dict]` - дополнительные данные для включения в токен

### create_signed_client

Обертка для автоматической подписи запросов.

**Параметры:**

| Параметр | Тип | По умолчанию | Описание |
|----------|-----|--------------|----------|
| base_client | RetryableHTTPClient | обязательный | Исходный клиент |
| secret | str | обязательный | Секретный ключ |
| service_name | str | "encoder-client" | Имя сервиса |
| auth_type | AuthType | AuthType.JWT_AUTH | Тип аутентификации |
| jwt_algorithm | Optional[str] | "HS256" | Алгоритм JWT |
| jwt_token_expire | Optional[int] | 30 | Время жизни JWT |
| jwt_extra_payload | Optional[Dict] | None | Доп. данные для JWT |
| auth_header_name | str | "Authorization" | Имя заголовка |
| auth_header_scheme | str | "Bearer" | Схема аутентификации |

### AuthType

Перечисление типов аутентификации:

- `AuthType.JWT_AUTH` - JWT токен (Authorization: Bearer <token>)
- `AuthType.SECRET_HEADER_AUTH` - простой секретный заголовок

## Логирование

Библиотека использует стандартный модуль logging. Для включения логов:

```python
import logging
logging.basicConfig(level=logging.INFO)
```

## Разработка и тестирование

```bash
# Клонировать репозиторий
git clone https://github.com/yourusername/retryable_http_client.git
cd retryable_http_client

# Создать виртуальное окружение
python -m venv venv
source venv/bin/activate  # или venv\Scripts\activate на Windows

# Установить в режиме разработки с зависимостями для тестирования
pip install -e .[dev]

# Запустить тесты
pytest tests/
```

## Требования

- Python >= 3.9
- httpx >= 0.28.1
- python-jose >= 3.5.0

## Лицензия

MIT

## Авторы

Oleg Sidorov (sidorov.works@yandex.ru)