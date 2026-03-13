# http_utils/http_client.py

import httpx
import asyncio
import random
from typing import Dict, Set, Optional

import logging
logger = logging.getLogger(__name__)


class RetryableHTTPClient:
    """
    Универсальный HTTP-клиент с повторными попытками и экспоненциальной задержкой.
    
    Клиент использует httpx.AsyncClient под капотом и предоставляет методы
    для выполнения HTTP-запросов с автоматическими повторными попытками при сбоях.
    Поддерживает HTTP/2, пул соединений и настраиваемые параметры retry-логики.
    
    Особенности:
    - Экспоненциальная задержка с jitter для распределения нагрузки
    - Общий таймаут на выполнение всех попыток (total_timeout)
    - Фиксированный таймаут на каждый отдельный запрос (base_timeout)
    - Не повторяет клиентские ошибки (4xx), кроме 429 Too Many Requests
    - Поддержка контекстного менеджера для автоматического закрытия
    
    Пример:
        >>> async with RetryableHTTPClient(max_retries=3, total_timeout=30.0) as client:
        ...     response = await client.get_with_retry("https://api.example.com/data")
        ...     print(response.json())
    
    Attributes:
        base_timeout (float): Таймаут каждого отдельного запроса в секундах
        max_retries (int): Максимальное количество повторных попыток
        base_delay (float): Начальная задержка перед первой повторной попыткой (в секундах)
        max_delay (float): Максимальная задержка между повторными попытками (в секундах)
        total_timeout (float, optional): Общее максимальное время выполнения всех попыток
        _client (httpx.AsyncClient, optional): Внутренний HTTP-клиент
    """
    
    def __init__(
        self,
        base_timeout: float = 10,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        total_timeout: Optional[float] = None
    ):
        """
        Инициализация HTTP-клиента с повторными попытками.

        Args:
            base_timeout: Таймаут каждого отдельного запроса в секундах. 
                Это максимальное время ожидания ответа на один конкретный запрос.
                Если сервер не отвечает за это время, попытка считается неудачной.
                Должен быть > 0.
            max_retries: Максимальное количество повторных попыток при неудачных 
                запросах. Общее количество попыток = max_retries + 1 (первая попытка).
                Должно быть >= 0.
            base_delay: Начальная задержка перед первой повторной попыткой в секундах.
                Используется для расчета экспоненциальной задержки с jitter.
                Для attempt=2 задержка = base_delay, для attempt=3 = base_delay * 2 и т.д.
                Должна быть > 0.
            max_delay: Максимальная задержка между повторными попытками в секундах.
                Должна быть >= base_delay.
            total_timeout: Общее максимальное время выполнения всех попыток в секундах,
                включая время ожидания ответов и задержки между попытками.
                Если None - ограничение не применяется.
                Если указан, должен быть > 0.
        
        Raises:
            ValueError: При некорректных значениях параметров
        
        Note:
            Клиент инициализируется лениво - фактическое создание httpx.AsyncClient
            происходит только при первом запросе через метод _ensure_client().
        """
        # Валидация входных параметров
        if base_timeout <= 0:
            raise ValueError(f"base_timeout должен быть > 0, получено {base_timeout}")
        
        if max_retries < 0:
            raise ValueError(f"max_retries должен быть >= 0, получено {max_retries}")
        
        if base_delay <= 0:
            raise ValueError(f"base_delay должен быть > 0, получено {base_delay}")
        
        if max_delay < base_delay:
            raise ValueError(f"max_delay ({max_delay}) должен быть >= base_delay ({base_delay})")
        
        if total_timeout is not None and total_timeout <= 0:
            raise ValueError(f"total_timeout должен быть > 0, получено {total_timeout}")
        
        self.base_timeout = base_timeout
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.total_timeout = total_timeout
        self._client = None

    async def _ensure_client(self):
        """
        Обеспечивает инициализацию клиента при первом использовании.
        
        Создает httpx.AsyncClient с настроенными параметрами:
        - Таймаут на основе base_timeout (фиксированный для всех запросов)
        - Лимиты соединений (макс. 100 одновременных, 20 keepalive)
        - Включена поддержка HTTP/2
        
        Если клиент уже существует и не закрыт, повторная инициализация не происходит.
        """
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.base_timeout),
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
                http2=True
            )

    def _calculate_delay_with_jitter(self, attempt: int) -> float:
        """
        Рассчитывает задержку перед повторной попыткой с добавлением jitter.
        
        Используется экспоненциальная стратегия: delay = base_delay * 2^(attempt-2).
        К полученному значению применяется jitter в диапазоне ±50% для равномерного
        распределения нагрузки при параллельных запросах (избегание "thundering herd").
        
        Args:
            attempt: Номер текущей попытки (начинается с 1). Для attempt=1 задержка не применяется,
                метод вызывается только для attempt >= 2.
            
        Returns:
            float: Задержка в секундах с применением jitter, ограниченная max_delay
            
        Note:
            Для первой попытки (attempt=1) задержка не применяется, но метод
            может быть вызван для attempt > 1
        """
        # Экспоненциальная задержка: base_delay * 2^(attempt-2)
        # Для attempt=2: base_delay * 2^0 = base_delay
        # Для attempt=3: base_delay * 2^1 = base_delay * 2
        # Для attempt=4: base_delay * 2^2 = base_delay * 4 и т.д.
        exponential_delay = self.base_delay * (2 ** (attempt - 2))
        
        # Ограничиваем максимальной задержкой
        capped_delay = min(exponential_delay, self.max_delay)
        
        # Добавляем jitter в диапазоне [0.5, 1.5] от базового значения
        # Это распределяет нагрузку во времени при множестве параллельных запросов
        jitter_factor = 0.5 + random.random()  # случайное число от 0.5 до 1.5
        delay_with_jitter = capped_delay * jitter_factor
        
        # Логируем рассчитанную задержку для отладки
        logger.debug(f"Attempt {attempt}: base delay={exponential_delay:.2f}s, "
                    f"capped={capped_delay:.2f}s, with jitter={delay_with_jitter:.2f}s")
        
        return delay_with_jitter

    def _should_retry_status(self, status_code: int) -> bool:
        """
        Определяет, нужно ли повторять запрос при получении HTTP статуса.
        
        Правила повторных попыток:
        - Все серверные ошибки (5xx) - повторяем (проблема на стороне сервера)
        - 429 Too Many Requests - повторяем (клиент может повторить позже)
        - Остальные клиентские ошибки (4xx) - не повторяем (ошибка в запросе)
        - Успешные статусы (2xx) - обрабатываются отдельно, сюда не попадают
        
        Args:
            status_code: HTTP статус код ответа
            
        Returns:
            bool: True если запрос стоит повторить, False иначе
        """
        # Серверные ошибки - повторяем (проблема на стороне сервера)
        if 500 <= status_code < 600:
            return True
        
        # 429 Too Many Requests - повторяем с задержкой
        if status_code == 429:
            return True
        
        # Остальные клиентские ошибки (400-499, кроме 429) - не повторяем
        if 400 <= status_code < 500:
            return False
        
        # Другие статусы (информационные, редиректы) - не повторяем по умолчанию
        return False

    async def request_with_retry(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        success_statuses: Set[int] = {200},
        **kwargs
    ) -> httpx.Response:
        """
        Выполняет HTTP-запрос с повторными попытками и экспоненциальной задержкой.

        Args:
            method: HTTP метод (GET, POST, PUT, DELETE и т.д.)
            url: URL для запроса
            headers: HTTP заголовки. По умолчанию None.
            success_statuses: Набор HTTP статусов, которые считаются успешными. 
                По умолчанию {200}.
            **kwargs: Дополнительные параметры для передачи в httpx.AsyncClient.request
                (например, json, data, params, cookies и т.д.)

        Returns:
            httpx.Response: Объект ответа при успешном запросе

        Raises:
            httpx.HTTPStatusError: Если после всех попыток получен HTTP статус 
                не из success_statuses и статус не подлежит повторению
            httpx.RequestError: Если после всех попыток запрос не удался 
                из-за сетевых проблем
            TimeoutError: Если превышен общий таймаут total_timeout (если установлен)

        Note:
            - Таймаут каждого отдельного запроса фиксирован и равен base_timeout
            - Задержка между попытками: экспоненциальная с jitter
            - Не повторяются клиентские ошибки 4xx, кроме 429
            - Серверные ошибки 5xx и 429 повторяются
            - Общий таймаут total_timeout имеет приоритет над количеством попыток
        """
        await self._ensure_client()
        
        last_exception = None
        start_time = asyncio.get_event_loop().time()
        
        # Таймаут для каждого отдельного запроса фиксирован.
        # Увеличение таймаута с каждой попыткой было бы некорректно, так как
        # таймаут - это максимальное время ожидания ответа на конкретный запрос,
        # а не на всю последовательность попыток. Если сервер не отвечает за
        # base_timeout секунд в первой попытке, нет причин ждать дольше во второй.
        fixed_timeout = httpx.Timeout(self.base_timeout)
        
        # Общее количество попыток = начальный запрос + max_retries повторных
        # Таким образом, если max_retries=3, будет до 4 попыток всего
        max_attempts = self.max_retries + 1
        
        for attempt in range(1, max_attempts + 1):
            # Проверка общего таймаута перед каждой попыткой
            if self.total_timeout is not None:
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= self.total_timeout:
                    error_msg = (f"Общий таймаут {self.total_timeout}с превышен "
                                f"после {elapsed:.2f}с на попытке {attempt}")
                    logger.error(error_msg)
                    raise TimeoutError(error_msg)
            
            try:
                # Задержка между попытками (для attempt > 1)
                if attempt > 1:
                    delay = self._calculate_delay_with_jitter(attempt)
                    logger.debug(f"Попытка {attempt}/{max_attempts} после задержки {delay:.2f}с")
                    await asyncio.sleep(delay)
                    
                    # После задержки снова проверяем общий таймаут
                    if self.total_timeout is not None:
                        elapsed = asyncio.get_event_loop().time() - start_time
                        if elapsed >= self.total_timeout:
                            error_msg = (f"Общий таймаут {self.total_timeout}с превышен "
                                        f"после задержки на попытке {attempt}")
                            logger.error(error_msg)
                            raise TimeoutError(error_msg)
                
                logger.debug(f"Попытка {attempt}/{max_attempts}: {method} {url} "
                            f"(таймаут запроса {self.base_timeout}с)")
                
                response = await self._client.request(
                    method=method,
                    url=url,
                    headers=headers,
                    timeout=fixed_timeout,  # фиксированный таймаут для каждой попытки
                    **kwargs
                )
                
                # Проверяем, входит ли статус в список успешных
                if response.status_code in success_statuses:
                    logger.debug(f"Запрос успешен на попытке {attempt}: "
                                f"{method} {url} - {response.status_code}")
                    return response
                
                # Логируем неудачную попытку с деталями ответа
                logger.warning(
                    f"Попытка {attempt}/{max_attempts} не удалась: {method} {url} - "
                    f"Статус: {response.status_code}, Тело: {response.text[:200]}"
                )
                
                # Определяем, нужно ли повторять запрос для этого статуса
                if not self._should_retry_status(response.status_code):
                    # Для статусов, которые не подлежат повторению, сразу выбрасываем исключение
                    logger.error(f"Статус {response.status_code} не подлежит повторению")
                    response.raise_for_status()  # Выбросит HTTPStatusError
                
                # Если статус подлежит повторению, сохраняем исключение
                # для случая, когда попытки закончатся
                last_exception = httpx.HTTPStatusError(
                    f"HTTP ошибка {response.status_code}",
                    request=response.request,
                    response=response
                )
                
            except httpx.HTTPStatusError as e:
                # Эта ветка срабатывает при вызове raise_for_status() или если
                # исключение возникло в самом httpx
                if attempt < max_attempts and self._should_retry_status(e.response.status_code):
                    # Для повторяемых статусов (5xx, 429) продолжаем попытки
                    logger.warning(f"Повторяемая HTTP ошибка {e.response.status_code} "
                                  f"на попытке {attempt}")
                    last_exception = e
                    continue
                else:
                    # Для неповторяемых статусов или если попытки кончились - пробрасываем
                    logger.error(f"Неповторяемая HTTP ошибка {e.response.status_code} "
                                f"после {attempt} попыток")
                    raise
                    
            except httpx.RequestError as e:
                # Сетевые ошибки, таймауты, проблемы с DNS и т.д. - всегда повторяем
                last_exception = e
                if attempt < max_attempts:
                    logger.warning(
                        f"Сетевая ошибка на попытке {attempt}/{max_attempts}: "
                        f"{method} {url} - {str(e)}"
                    )
                    # Продолжаем цикл для следующей попытки
                else:
                    logger.error(f"Все попытки исчерпаны: {method} {url} - {str(e)}")
                    # Выходим из цикла, после будет raise
        
        # Если мы дошли до этой точки, значит все попытки исчерпаны
        # и последнее исключение должно быть поднято
        if last_exception:
            raise last_exception
        
        # Эта точка не должна достигаться, но на всякий случай
        raise RuntimeError(f"Неожиданное состояние: все попытки выполнены без результата")

    async def get_with_retry(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        success_statuses: Set[int] = {200},
        **kwargs
    ) -> httpx.Response:
        """
        Выполняет GET запрос с повторными попытками.
        
        Args:
            url: URL для GET запроса
            headers: HTTP заголовки
            success_statuses: Набор успешных HTTP статусов. По умолчанию {200}
            **kwargs: Дополнительные параметры для request_with_retry

        Returns:
            httpx.Response: Объект ответа

        See Also:
            request_with_retry: Базовый метод, выполняющий фактическую работу
        """
        return await self.request_with_retry(
            method="GET",
            url=url,
            headers=headers,
            success_statuses=success_statuses,
            **kwargs
        )

    async def post_with_retry(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        success_statuses: Set[int] = {200},
        **kwargs
    ) -> httpx.Response:
        """
        Выполняет POST запрос с повторными попытками.
        
        Args:
            url: URL для POST запроса
            headers: HTTP заголовки
            success_statuses: Набор успешных HTTP статусов. По умолчанию {200}
            **kwargs: Дополнительные параметры для request_with_retry
                (например, json, data для тела запроса)

        Returns:
            httpx.Response: Объект ответа

        Example:
            >>> response = await client.post_with_retry(
            ...     "https://api.example.com/users",
            ...     json={"name": "John", "email": "john@example.com"}
            ... )
        """
        return await self.request_with_retry(
            method="POST",
            url=url,
            headers=headers,
            success_statuses=success_statuses,
            **kwargs
        )

    async def put_with_retry(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        success_statuses: Set[int] = {200},
        **kwargs
    ) -> httpx.Response:
        """
        Выполняет PUT запрос с повторными попытками.
        
        Args:
            url: URL для PUT запроса
            headers: HTTP заголовки
            success_statuses: Набор успешных HTTP статусов. По умолчанию {200}
            **kwargs: Дополнительные параметры для request_with_retry

        Returns:
            httpx.Response: Объект ответа
        """
        return await self.request_with_retry(
            method="PUT",
            url=url,
            headers=headers,
            success_statuses=success_statuses,
            **kwargs
        )

    async def delete_with_retry(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        success_statuses: Set[int] = {200, 204},
        **kwargs
    ) -> httpx.Response:
        """
        Выполняет DELETE запрос с повторными попытками.
        
        Args:
            url: URL для DELETE запроса
            headers: HTTP заголовки
            success_statuses: Набор успешных HTTP статусов. 
                По умолчанию {200, 204} (OK и No Content)
            **kwargs: Дополнительные параметры для request_with_retry

        Returns:
            httpx.Response: Объект ответа
        """
        return await self.request_with_retry(
            method="DELETE",
            url=url,
            headers=headers,
            success_statuses=success_statuses,
            **kwargs
        )

    async def close(self):
        """
        Корректно закрывает HTTP-клиент и освобождает ресурсы.
        
        Закрывает внутренний httpx.AsyncClient и все открытые соединения.
        Безопасно вызывать multiple раз - повторные вызовы игнорируются.
        
        Всегда вызывайте этот метод после завершения работы с клиентом,
        или используйте клиент как контекстный менеджер.
        """
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self):
        """
        Вход в асинхронный контекстный менеджер.
        
        Обеспечивает автоматическую инициализацию клиента при входе в контекст.
        
        Returns:
            RetryableHTTPClient: Экземпляр клиента для использования в контексте
        """
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Выход из асинхронного контекстного менеджера.
        
        Обеспечивает автоматическое закрытие клиента при выходе из контекста,
        даже в случае исключения.
        
        Args:
            exc_type: Тип исключения, если было выброшено
            exc_val: Значение исключения
            exc_tb: Трассировка исключения
        """
        await self.close()