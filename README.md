# Data Orchestrator

Простой оркестратор переноса данных между источниками и приёмниками с поддержкой чанков.

## Возможности
- **Источники**: CSV, JSON, XML (загрузка файла), PostgreSQL (прямое подключение)
- **Приёмники**: Предпросмотр (первые строки), файлы CSV/JSON/XML, PostgreSQL
- **Перенос чанками**: укажите размер чанка (количество строк на шаг)
- 🎨 **Современный React интерфейс** с красивым дизайном
- 📁 **Drag & Drop** загрузка файлов
- ⚡ **Прогресс-бар** и индикаторы загрузки
- 📊 **Интерактивный предпросмотр** данных (ограничен 10 строками для производительности)
- 🔄 **Валидация** и обработка ошибок
- 📝 **Настройка разделителей** для CSV файлов (точка с запятой по умолчанию)
- 🗄️ **Подключение к PostgreSQL** с тестированием соединения
- 🔍 **Просмотр схемы таблиц** и выполнение SQL запросов
- 🏗️ **Гибкая архитектура** для добавления других СУБД
- 🧠 **Умное определение типов** - автоматический анализ данных и создание оптимальных типов столбцов PostgreSQL
- 🎯 **Модульная архитектура** - отдельные карточки для источника и приёмника
- 🔧 **Условные поля** - настройки появляются только когда нужны
- REST API на FastAPI.
- Docker и docker-compose для запуска.
- Тесты на pytest.
- 🚀 **Apache Airflow** для оркестрации рабочих процессов.

## Структура
- Backend (FastAPI): `backend/app`
  - Абстракции: `app/services/base.py` (`DataSource`, `DataSink`), поддержка чанков
  - Источники: `CSVSource`, `JSONSource`, `XMLSource`
  - Приёмники: `CSVFileSink`, `JSONFileSink`, `XMLFileSink`, `PreviewSink`
  - Airflow интеграция: `app/services/airflow_client.py`
  - API: `POST /upload` (предпросмотр), `POST /transfer` (перенос с выбором типов), `/airflow/*` (управление DAG)
- Frontend: React приложение с TypeScript и современным UI
  - Airflow Dashboard: `src/components/AirflowDashboard.tsx`
  - Airflow hooks: `src/hooks/useAirflow.ts`
- Airflow: `airflow/Dockerfile`, `airflow/dags/` (DAG файлы)
- Docker: `backend/Dockerfile`, `frontend/Dockerfile`, `docker-compose.yml`
- Тесты: `backend/tests`

## Запуск
```bash
docker compose up -d --build
```

- Фронтенд: http://localhost:3000
- Бэкенд: http://localhost:8000/health
- **Airflow**: http://localhost:8081

Прототип запущен по адресу http://158.160.24.225:3000/. Там же запущены ClickHouse и PostgreSQL. Если подробнее, то:
CONTAINER ID   IMAGE                          COMMAND                  CREATED          STATUS          PORTS                                                                                                NAMES
d6e5968f5250   postgres:15                    "docker-entrypoint.s…"   23 seconds ago   Up 15 seconds   0.0.0.0:5433->5432/tcp, [::]:5433->5432/tcp                                                          pg-api
aa2b9d135dc9   clickhouse/clickhouse-server   "/entrypoint.sh"         2 minutes ago    Up 15 seconds   0.0.0.0:8123->8123/tcp, [::]:8123->8123/tcp, 0.0.0.0:9000->9000/tcp, [::]:9000->9000/tcp, 9009/tcp   my-clickhouse
a316e42a7fa7   data_engineer-frontend         "/docker-entrypoint.…"   4 minutes ago    Up 4 minutes    80/tcp, 0.0.0.0:3000->3000/tcp, [::]:3000->3000/tcp                                                  data-orchestrator-frontend
051165888f8d   data_engineer-airflow          "/usr/bin/dumb-init …"   4 minutes ago    Up 4 minutes    0.0.0.0:8081->8080/tcp, [::]:8081->8080/tcp                                                          data-orchestrator-airflow
d6131f138680   data_engineer-backend          "uvicorn app.main:ap…"   4 minutes ago    Up 4 minutes    0.0.0.0:8000->8000/tcp, [::]:8000->8000/tcp                                                          data-orchestrator-backend
ddbbf361940a   postgres:15                    "docker-entrypoint.s…"   4 minutes ago    Up 4 minutes    0.0.0.0:5432->5432/tcp, [::]:5432->5432/tcp                                                          data-orchestrator-postgres

## Использование
### Веб-интерфейс
1. **Настройте источник данных**:
   - CSV/JSON/XML файл: перетащите файл или нажмите для выбора
   - PostgreSQL: заполните поля подключения (хост, порт, база, пользователь, пароль, таблица)
   - Настройте разделитель для CSV (по умолчанию ;)
2. **Настройте приёмник данных**:
   - Предпросмотр: покажет первые 10 строк
   - CSV/JSON/XML файл: сохранит результат
   - PostgreSQL: заполните поля подключения для записи данных
   - Настройте разделитель для CSV и размер чанка
3. **Нажмите «Запустить перенос»** и следите за прогрессом
4. **Просмотрите результат**: таблица для предпросмотра или автоматическое скачивание файла

    **Особенности интерфейса:**
    - 🎯 **Модульные карточки** - каждая настройка в своей карточке
    - 🔧 **Умные поля** - настройки CSV появляются только для CSV файлов
    - 📁 **Условная загрузка** - файл нужен только для файловых источников
    - 🗄️ **Отдельные поля БД** - удобный ввод параметров PostgreSQL
    - 🔄 **Двойные карточки** - отдельные настройки для источника и приёмника БД
    - 🧪 **Тестирование подключения** - проверка соединения с БД перед переносом
    - 🧠 **Умные типы данных** - автоматическое определение INTEGER, VARCHAR, DATE, BOOLEAN и других типов

### Airflow Dashboard
1. **Просмотр DAG**: список всех доступных рабочих процессов
2. **Управление DAG**: запуск, приостановка, возобновление
3. **Мониторинг**: просмотр статуса выполнения и истории запусков
4. **Переход в веб-интерфейс**: прямая ссылка на Airflow UI

**Учетные данные Airflow**:
- **URL**: http://localhost:8081
- **Логин**: `admin`
- **Пароль**: `uSv9mh8FRTuEYz7z`

### API
- **Предпросмотр файлов**:
  - `POST /upload` form-data: `file` (файл), `delimiter` (str, по умолчанию ";"). Ответ: `{ headers, rows }`
- **Перенос файлов**:
  - `POST /transfer` form-data: `source_type` (csv|json|xml), `sink_type` (preview|csv|json|xml), `chunk_size` (int), `delimiter` (str), `file` (файл)
  - Ответ: `{ result, out_path, headers }`
- **Работа с PostgreSQL**:
  - `POST /database/connect` - тестирование подключения
  - `POST /database/tables` - получение списка таблиц
  - `POST /database/schema` - получение схемы таблицы
  - `POST /database/query` - выполнение SQL запроса
  - `POST /transfer/to-database` - перенос данных в PostgreSQL
    - Параметры: `source_type`, `file`, `sink_connection_string`, `sink_table_name`, `sink_mode`, `chunk_size`, `delimiter`, `database_type`
- **Управление Airflow**:
  - `GET /airflow/dags` - список всех DAG
  - `GET /airflow/dags/{dag_id}/status` - статус конкретного DAG
  - `GET /airflow/dags/{dag_id}/runs` - список запусков DAG
  - `POST /airflow/dags/{dag_id}/trigger` - запуск DAG
  - `POST /airflow/dags/{dag_id}/pause` - приостановка DAG
  - `POST /airflow/dags/{dag_id}/unpause` - возобновление DAG
  - `GET /airflow/dags/{dag_id}/ui-url` - URL для перехода к DAG в веб-интерфейсе
  - `GET /airflow/ui-url` - URL веб-интерфейса Airflow

## Тесты
Запуск тестов локально:
```bash
docker compose exec backend pip install -r /app/requirements.txt
docker compose exec backend pytest -q
```

## Документация

### Airflow
- **Полная документация**: `AIRFLOW_DOCUMENTATION.md`
- **Быстрая справка**: `AIRFLOW_QUICK_REFERENCE.md`

### Основные учетные данные
- **Airflow веб-интерфейс**: http://localhost:8081 (admin / uSv9mh8FRTuEYz7z)
- **API пользователь**: api_user / api123

## Примечания
- Источники JSON: поддерживаются массивы объектов или `{"items": [...]}`; для массивов массивов заголовки будут `col1..colN`.
- Источники XML: по умолчанию ищется тег `item` (или берутся прямые дети корня), заголовки формируются из имён дочерних тегов.
- При сохранении файлов приёмники пишут в `/tmp` внутри контейнера бэкенда.
- Airflow DAG по умолчанию приостанавливаются при создании - используйте веб-интерфейс для их активации.


