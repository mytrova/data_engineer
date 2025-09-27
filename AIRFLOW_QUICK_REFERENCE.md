# Airflow - Быстрая справка

## 🔑 Учетные данные

### Веб-интерфейс Airflow
- **URL**: http://localhost:8081
- **Логин**: `admin`
- **Пароль**: `uSv9mh8FRTuEYz7z`

### API пользователь
- **Логин**: `api_user`
- **Пароль**: `api123`

## 🚀 Быстрый старт

### 1. Запуск сервисов
```bash
docker compose up -d
```

### 2. Проверка статуса
```bash
docker compose ps
```

### 3. Открытие интерфейсов
- **Airflow UI**: http://localhost:8081
- **Основное приложение**: http://localhost:3000

## 📋 Основные команды

### Управление DAG
```bash
# Список DAG
docker exec data-orchestrator-airflow airflow dags list

# Запуск DAG
docker exec data-orchestrator-airflow airflow dags trigger test_wait_dag

# Приостановка DAG
docker exec data-orchestrator-airflow airflow dags pause test_wait_dag

# Возобновление DAG
docker exec data-orchestrator-airflow airflow dags unpause test_wait_dag
```

### Мониторинг
```bash
# Статус DAG
docker exec data-orchestrator-airflow airflow dags state test_wait_dag <execution_date>

# Список задач
docker exec data-orchestrator-airflow airflow tasks list test_wait_dag

# Статус задач
docker exec data-orchestrator-airflow airflow tasks states-for-dag-run test_wait_dag <dag_run_id>
```

### Логи
```bash
# Логи Airflow
docker compose logs airflow

# Логи Backend
docker compose logs backend
```

## 🔧 API Endpoints

```bash
# Список DAG
curl http://localhost:8000/airflow/dags

# Запуск DAG
curl -X POST http://localhost:8000/airflow/dags/test_wait_dag/trigger

# URL веб-интерфейса
curl http://localhost:8000/airflow/ui-url
```

## 📊 Статусы

### DAG статусы
- `running` - выполняется
- `success` - успешно завершен
- `failed` - завершен с ошибкой
- `paused` - приостановлен

### Задача статусы
- `success` - успешно выполнена
- `failed` - завершена с ошибкой
- `running` - выполняется

## 🆘 Устранение неполадок

### DAG не появляется
```bash
# Проверка синтаксиса
docker exec data-orchestrator-airflow python -m py_compile /opt/airflow/dags/test_dag.py

# Проверка файлов
docker exec data-orchestrator-airflow ls -la /opt/airflow/dags/
```

### Проблемы с аутентификацией
```bash
# Список пользователей
docker exec data-orchestrator-airflow airflow users list

# Создание нового пользователя
docker exec data-orchestrator-airflow airflow users create \
  --username new_user \
  --firstname New \
  --lastname User \
  --role Admin \
  --email new@example.com \
  --password new_password
```

### Перезапуск сервисов
```bash
# Перезапуск Airflow
docker compose restart airflow

# Перезапуск Backend
docker compose restart backend

# Полная перезагрузка
docker compose down && docker compose up -d
```

---

**💡 Совет**: Полная документация доступна в файле `AIRFLOW_DOCUMENTATION.md`
