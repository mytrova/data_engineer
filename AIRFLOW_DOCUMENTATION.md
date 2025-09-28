# Документация по Airflow

## Обзор

Apache Airflow интегрирован в проект Data Orchestrator для управления и мониторинга DAG (Directed Acyclic Graphs) - рабочих процессов обработки данных.

## Доступ к сервисам

### Веб-интерфейс Airflow
- **URL**: http://localhost:8081
- **Логин**: `admin`
- **Пароль**: `uSv9mh8FRTuEYz7z`

**Примечание**: Если ссылка в интерфейсе ведет на `http://airflow:8080/home`, это внутренний URL контейнера. Используйте http://localhost:8081 для доступа с хоста.

### API пользователь
- **Логин**: `api_user`
- **Пароль**: `api123`

### Основное приложение
- **URL**: http://localhost:3000
- **Airflow Dashboard**: встроен в основной интерфейс

### Backend API
- **URL**: http://localhost:8000
- **Airflow endpoints**: `/airflow/*`

## Запуск сервисов

```bash
# Запуск всех сервисов
docker compose up -d

# Проверка статуса
docker compose ps

# Просмотр логов
docker compose logs airflow
docker compose logs backend
```

## Структура DAG

### Тестовый DAG: `test_wait_dag`

**Описание**: Демонстрационный DAG с ожиданием 100 секунд

**Задачи**:
1. `wait_100_seconds` - ожидает 100 секунд
2. `print_completion` - выводит сообщение о завершении
3. `show_info` - показывает информацию о выполнении

**Расписание**: Ежедневно в 00:00

**Файл**: `airflow/dags/test_dag.py`

## Управление DAG

### Через веб-интерфейс

1. Откройте http://localhost:8081
2. Войдите с учетными данными `admin` / `uSv9mh8FRTuEYz7z`
3. Найдите DAG `test_wait_dag` в списке
4. Используйте кнопки:
   - ▶️ **Trigger** - запустить DAG
   - ⏸️ **Pause** - приостановить DAG
   - ▶️ **Unpause** - возобновить DAG
   - 📊 **Graph** - просмотреть граф задач
   - 📋 **Details** - детальная информация

### Через командную строку

```bash
# Просмотр списка DAG
docker exec data-orchestrator-airflow airflow dags list

# Запуск DAG
docker exec data-orchestrator-airflow airflow dags trigger test_wait_dag

# Приостановка DAG
docker exec data-orchestrator-airflow airflow dags pause test_wait_dag

# Возобновление DAG
docker exec data-orchestrator-airflow airflow dags unpause test_wait_dag

# Статус DAG run
docker exec data-orchestrator-airflow airflow dags state test_wait_dag <execution_date>

# Список задач
docker exec data-orchestrator-airflow airflow tasks list test_wait_dag

# Статус задач в DAG run
docker exec data-orchestrator-airflow airflow tasks states-for-dag-run test_wait_dag <dag_run_id>
```

### Через API

```bash
# Получение списка DAG
curl http://localhost:8000/airflow/dags

# Запуск DAG
curl -X POST http://localhost:8000/airflow/dags/test_wait_dag/trigger

# Статус DAG
curl http://localhost:8000/airflow/dags/test_wait_dag/status

# Список запусков DAG
curl http://localhost:8000/airflow/dags/test_wait_dag/runs

# URL веб-интерфейса
curl http://localhost:8000/airflow/ui-url
```

## Мониторинг

### Статусы DAG
- **Running** - выполняется
- **Success** - успешно завершен
- **Failed** - завершен с ошибкой
- **Queued** - в очереди на выполнение
- **Paused** - приостановлен

### Статусы задач
- **Success** - успешно выполнена
- **Failed** - завершена с ошибкой
- **Running** - выполняется
- **Up for retry** - ожидает повторного запуска
- **Up for reschedule** - ожидает переноса времени

## Создание новых DAG

### Шаги создания:

1. Создайте файл `.py` в директории `airflow/dags/`
2. Определите DAG с помощью `DAG()` объекта
3. Создайте задачи с помощью операторов (PythonOperator, BashOperator и др.)
4. Определите зависимости между задачами
5. Перезапустите Airflow или подождите автоматического обновления

### Пример структуры:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'your-name',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'your_dag_name',
    default_args=default_args,
    description='Описание вашего DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def your_task_function():
    # Ваш код здесь
    pass

task = PythonOperator(
    task_id='your_task_id',
    python_callable=your_task_function,
    dag=dag,
)
```

## Конфигурация

### Основные настройки в docker-compose.yml:

```yaml
environment:
  - AIRFLOW__CORE__EXECUTOR=LocalExecutor
  - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
  - AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
  - AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
  - AIRFLOW__CORE__LOAD_EXAMPLES=False
```

### Важные файлы:

- `airflow/Dockerfile` - образ Airflow
- `airflow/dags/` - директория с DAG файлами
- `docker-compose.yml` - конфигурация сервисов
- `backend/app/services/airflow_client.py` - клиент для работы с API

## Устранение неполадок

### DAG не появляется в списке:
1. Проверьте синтаксис Python файла
2. Убедитесь, что файл находится в `airflow/dags/`
3. Проверьте логи Airflow: `docker compose logs airflow`

### Ошибки аутентификации:
1. Убедитесь, что используете правильные учетные данные
2. Проверьте, что пользователь создан: `docker exec data-orchestrator-airflow airflow users list`

### Проблемы с базой данных:
1. Проверьте статус PostgreSQL: `docker compose ps`
2. Перезапустите базу данных: `docker compose restart postgres`

### Очистка и перезапуск:
```bash
# Остановка всех сервисов
docker compose down

# Очистка томов (ОСТОРОЖНО: удалит все данные!)
docker compose down -v

# Пересборка и запуск
docker compose up --build -d
```

## Безопасность

### Рекомендации:
1. Измените пароли по умолчанию в production
2. Используйте HTTPS для production окружения
3. Ограничьте доступ к веб-интерфейсу Airflow
4. Регулярно обновляйте зависимости

### Изменение паролей:
```bash
# Изменение пароля пользователя
docker exec data-orchestrator-airflow airflow users reset-password admin --password new_password

# Создание нового пользователя
docker exec data-orchestrator-airflow airflow users create \
  --username new_user \
  --firstname New \
  --lastname User \
  --role Admin \
  --email new@example.com \
  --password new_password
```

## Полезные ссылки

- [Документация Apache Airflow](https://airflow.apache.org/docs/)
- [Учебник по DAG](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)
- [Справочник операторов](https://airflow.apache.org/docs/apache-airflow/stable/operators/index.html)
- [REST API документация](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)

---

**Примечание**: Данная документация актуальна для текущей конфигурации проекта. При изменении настроек обновите соответствующие разделы.
