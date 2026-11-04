# Flight Booking: gRPC + Redis

Два Python-сервиса для бронирования авиабилетов. Проект содержит реализацию всех 10 пунктов задания: PostgreSQL, REST, gRPC, транзакции, API key, кеш, retry, Redis Sentinel и Circuit Breaker. Межсервисные сбои обрабатываются Saga с постоянным журналом и компенсацией.

## Быстрый запуск

Нужен Docker с Compose. В каталоге проекта:

```sh
docker compose up --build -d --wait
```

Для установленной отдельной версии Compose также доступен `docker-compose up --build`. Миграции применяются до запуска каждого приложения. `.env` не обязателен: в Compose есть локальные значения по умолчанию. Для изменения настроек скопируйте `.env.example` в `.env`.

- REST API: http://localhost:8000
- Swagger: http://localhost:8000/docs
- Готовность приложения: http://localhost:8000/health/ready
- Только REST-порт опубликован на localhost; БД, gRPC и Redis работают внутри сети Compose.

Полный короткий сценарий (нужен только Python, дополнительные пакеты на компьютере не требуются):

```sh
python scripts/demo.py
```

Он находит рейс, создаёт бронирование, повторяет создание с тем же ключом, отменяет его дважды и проверяет остаток мест. После сценария места возвращены, а отменённая запись остаётся в истории.

## Примеры API

Поиск: `GET /flights?origin=SVO&destination=LED`. Необязательный параметр `date=YYYY-MM-DD` обозначает календарную дату UTC. Поиск возвращает только SCHEDULED-рейсы.

Пример создания:

```http
POST /bookings
Content-Type: application/json
Idempotency-Key: 6ac14cc8-14f8-444c-b4a5-a3eb19ecb216

{
  "user_id": "student-1",
  "flight_id": "11111111-1111-4111-8111-111111111111",
  "passenger_name": "Ivan Ivanov",
  "passenger_email": "ivan@example.com",
  "seat_count": 2
}
```

Ответ 201 содержит `id`, данные пассажира, `total_price_minor`, `status`, время создания и изменения. Все суммы — целые копейки RUB. Начальная цена первого рейса — 500000 копеек. Его вылет при первой установке назначается на 30 дней вперёд; миграция не переносит дату при последующих перезапусках.

Остальные методы:

| Метод | Назначение |
|---|---|
| GET /flights/{id} | Рейс |
| GET /bookings/{id} | Бронирование |
| GET /bookings?user_id=student-1 | Бронирования пользователя |
| POST /bookings/{id}/cancel | Отмена и возврат мест |
| GET /operations/{id} | Результат/состояние операции после сбоя |

Повтор POST с тем же Idempotency-Key и тем же телом возвращает исходное бронирование. Другое тело с тем же ключом даёт 409. Повтор отмены безопасен. Ошибки имеют `{code, message, operation_id?}`: 422 — валидация, 404 — объект отсутствует, 409 — бизнес-конфликт, 503 — недоступность/открытый breaker, 504 — deadline. Если создание окончательно завершилось ошибкой, для новой попытки создания нужен новый ключ; старый ключ обозначает прежнюю операцию.

## Архитектура и данные

```mermaid
flowchart LR
    Client -->|REST| Booking[Booking Service]
    Booking --> BookingDB[(Booking PostgreSQL)]
    Booking -->|gRPC / API key| Flight[Flight Service]
    Flight --> FlightDB[(Flight PostgreSQL)]
    Flight -->|Sentinel discovery| Redis[(Redis primary + replica)]
```

Бизнес-данные: `bookings` в Booking DB; `flights` и `seat_reservations` в Flight DB. Связи между БД логические, по UUID. Внутри Flight DB есть физические FK. CHECK-ограничения не позволяют отрицательный остаток, нулевую цену и некорректные статусы/время/аэропорты. Номер рейса и дата вылета UTC уникальны.

Технические таблицы: `booking_operations` (журнал Saga), `reservation_guards` (защита от запоздавшего резервирования после компенсации), `cache_state` (версия кеша).

Подробный проект и ER-диаграмма: [docs/design.md](docs/design.md). Отдельная диаграмма: [docs/er.mmd](docs/er.mmd). Python-пакеты названы `booking_service` и `flight_service`; Dockerfile каждого приложения находится в корне, чтобы сборка могла включить общий контракт.

## Транзакции и восстановление

ReserveSeats блокирует guard и рейс (`SELECT FOR UPDATE`), проверяет места, уменьшает остаток, создаёт ACTIVE-резервацию и увеличивает версию кеша одной транзакцией. Повтор с тем же booking_id не списывает места повторно. Другие параметры того же booking_id отклоняются.

ReleaseReservation фиксирует запрет повторного резервирования, возвращает места и меняет статус на RELEASED одной транзакцией. Если резервации ещё нет, запрет всё равно сохраняется: опоздавший ReserveSeats её не создаст.

Создание в Booking Service: журнал → GetFlight → сохранение снимка цены → ReserveSeats → локальная транзакция вставки CONFIRMED и завершения журнала. Если исход резервирования неизвестен или запись бронирования не завершилась, операция компенсируется через ReleaseReservation. После рестарта восстановление консервативно компенсирует прерванное создание; окончательно сохранённое бронирование не компенсируется, поскольку его запись и SUCCEEDED журнала атомарны.

Отмена: журнал → release → локальная транзакция CANCELLED + завершение журнала. Фоновый обработчик продолжает незавершённые операции. Он координируется с REST через advisory lock на отдельном пуле соединений. Проверка состояния в транзакции не позволяет устаревшему обработчику подтвердить уже компенсируемую операцию.

Согласованность двух БД восстанавливается после возврата зависимостей. Saga не является общей ACID-транзакцией: до завершения восстановления возможны временные промежуточные состояния. При таймауте клиент использует operation_id или повторяет запрос с тем же ключом. Автоматическое истечение ACTIVE-резерваций не вводится, поскольку временное удержание не требуется условием; EXPIRED предусмотрен контрактом.

## Кеш и Redis Sentinel

Cache-Aside: `v{version}:flight:{id}` и `v{version}:search:{origin}:{destination}:{date-or-all}`, TTL по умолчанию 300 секунд. Версия и данные читаются из одного REPEATABLE READ READ ONLY снимка PostgreSQL. Мутации повышают версию в транзакции с данными. Запоздавшая запись кеша попадает под старую версию и не используется последующими читателями. Старые физические ключи удаляются по TTL. Один небольшой запрос за версией к PostgreSQL сохраняется даже при cache hit.

При недоступности Redis чтение идёт из PostgreSQL. Логи содержат `cache_hit`, `cache_miss`, `cache_error` и `cache_version_changed`. gRPC UpdateFlight меняет цену и инвалидирует кеш; стоимость уже созданного бронирования остаётся прежней.

Redis: один основной узел, реплика, три Sentinel, quorum 2. redis-py использует `Sentinel.master_for`, чтение кеша выполняется с текущего основного узла. Конфигурации Redis и Sentinel записаны на отдельные тома, чтобы изменения роли переживали перезапуск. Внутренняя сеть использует подсеть `172.30.87.0/24`; если она уже занята, нужно согласованно изменить адреса в Compose, скрипте Sentinel и проверке failover.

Проверка переключения и возврата старого основного узла в качестве реплики:

```sh
python scripts/verify_failover.py
```

Скрипт сначала заполняет кеш через работающий REST → gRPC сервис, затем временно останавливает текущий основной Redis и проверяет доступность API, продвижение реплики и запись кеша с TTL тем же работающим сервисом. В конце запускает остановленный узел обратно и проверяет его роль, состояние master_link_status=up и репликацию ключей. Успех выводится только после всех этапов. Скрипт рассчитан на отдельный учебный стек без параллельной нагрузки.

## Retry, deadline, Circuit Breaker

Клиентский interceptor выполняет не более трёх попыток только для UNAVAILABLE и DEADLINE_EXCEEDED; паузы — 100 и 200 мс. Одна попытка ограничена секундой, вся логическая gRPC-операция — четырьмя секундами. ReserveSeats и ReleaseReservation безопасны для повторов по booking_id. Дополнительный встроенный retry gRPC отключён.

Circuit Breaker окружает логическую операцию с retry. Пять последовательных технических ошибок открывают его на 15 секунд. В OPEN запрос немедленно получает 503. HALF_OPEN пропускает ровно один пробный вызов без retry. Успешная связь, включая бизнес-ответ, закрывает breaker; новая техническая ошибка снова открывает. Переходы видны как `circuit_transition`.

API key передаётся в metadata `x-api-key`; серверный interceptor проверяет каждый метод. Ключ отсутствует/неверен → UNAUTHENTICATED. Локальные значения предназначены для запуска задания; аутентификация пользователей REST и платежи в задание не входят.

## Проверки

Автономные проверки interceptor и обработки кеша:

```sh
python -m venv .venv
# Linux/macOS: . .venv/bin/activate
# PowerShell: .\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.lock
python scripts/generate_proto.py
python -m pytest -q
```

Отдельная проверка настоящего Redis/Sentinel без Docker и PostgreSQL (нужен локальный `redis-server`):

```sh
TEST_REDIS_SERVER=/path/to/redis-server python -m pytest -q tests/test_sentinel.py
```

Она поднимает основной узел, реплику и три Sentinel на свободных loopback-портах во временных каталогах, проверяет TTL, переключение с тем же экземпляром FlightCache, репликацию и повторный перезапуск прежнего primary. Проверка не использует Compose-сеть и не заменяет `scripts/verify_failover.py`.

Без TEST_* переменных интеграционные тесты явно пропускаются. Для всех тестов на PostgreSQL можно использовать уже поднятые контейнеры:

```sh
docker compose run --rm --no-deps -e TEST_FLIGHT_DATABASE_URL=postgresql+psycopg://flight:flight-local@flight-db:5432/flight_test -e TEST_BOOKING_DATABASE_URL=postgresql+psycopg://booking:booking-local@booking-db:5432/booking_test booking-service python scripts/run_integration.py
```

Скрипт создаёт отдельные базы `flight_test` и `booking_test`, применяет миграции и запускает pytest. Приложения используют только собственные БД; доступ к двум БД нужен исключительно интеграционному тестовому процессу. Имена тестовых БД обязательно заканчиваются на `_test`. Тесты добавляют изолированные записи и не очищают рабочие базы.

Покрыты: конкурентная покупка последнего места, конкурентные дубликаты reserve/release, потеря ответа после commit, компенсация перед опоздавшим reserve, ошибки записи бронирования, восстановление создания и отмены, конфликт Idempotency-Key, все методы аутентификации, REST → настоящий gRPC → PostgreSQL, гонка кеша, ограничения БД и состояния Circuit Breaker.

Актуально на 3 ноября 2026: здесь подтверждены автономные тесты и отдельная проверка Redis 7.0.15/Sentinel. Сборка Docker, полный Compose, Redis 7.4 из образа, интеграционные тесты PostgreSQL в этой среде и GitHub Actions пока не подтверждены. Подробности и прежние результаты разделены в [docs/verification.md](docs/verification.md).

Workflow [checks](.github/workflows/check.yml) запускает полный Compose-стек, REST-сценарий, интеграционные тесты и реальное переключение Sentinel. Он выполнится после размещения проекта в GitHub; локальное наличие файла workflow не означает успешный запуск CI.

## Соответствие заданию

| Пункт | Реализация |
|---|---|
| 1. gRPC-контракт | proto/flight/v1/flight.proto, кодогенерация |
| 2. ER и 3NF | docs/design.md, docs/er.mmd, ограничения миграций |
| 3. Два сервиса и PostgreSQL | booking_service, flight_service, Alembic, Compose |
| 4. Межсервисный flow | GetFlight → ReserveSeats → Booking; отмена через release |
| 5. Транзакции | SELECT FOR UPDATE, единая транзакция мест/резервации, Saga |
| 6. Аутентификация | API key из окружения, серверный interceptor |
| 7. Redis | Cache-Aside, TTL, версия, логи hit/miss |
| 8. Retry | Клиентский interceptor, три попытки, backoff, идемпотентность |
| 9. Отказоустойчивый Redis | Primary + replica + 3 Sentinel, Sentinel-aware клиент |
| 10. Circuit Breaker | CLOSED / OPEN / HALF_OPEN, конфигурация и логи |

Остановка с сохранением данных: `docker compose down`. Удаление томов не требуется для повторного запуска. Точный перечень реально выполненных проверок находится в [docs/verification.md](docs/verification.md).

Техническая основа: [gRPC Python](https://grpc.io/docs/languages/python/quickstart/), [redis-py Sentinel](https://redis.io/docs/latest/develop/clients/redis-py/connect/), [SQLAlchemy transactions](https://docs.sqlalchemy.org/en/20/core/connections.html).
