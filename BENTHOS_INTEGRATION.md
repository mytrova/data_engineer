# Интеграция Benthos в систему переливки данных

## Обзор

Система переливки данных была переработана для использования **Benthos** вместо прямых SQL-запросов. Benthos - это мощный инструмент для обработки потоков данных, который обеспечивает надежную и масштабируемую переливку данных между различными источниками и приёмниками.

## Преимущества использования Benthos

1. **Универсальность**: Поддержка множества источников и приёмников данных
2. **Надежность**: Встроенные механизмы обработки ошибок и повторных попыток
3. **Масштабируемость**: Эффективная обработка больших объемов данных
4. **Гибкость**: Легкая настройка конфигураций для различных сценариев
5. **Мониторинг**: Встроенные метрики и логирование

## Поддерживаемые источники данных

### Базы данных
- **PostgreSQL**: Полная поддержка с SQL-запросами
- **ClickHouse**: Оптимизированная поддержка для аналитических запросов
- **SQLite**: Локальные файлы базы данных

### Потоковые системы
- **Kafka**: Поддержка топиков и consumer groups

### Файловые системы
- **CSV файлы**: С настраиваемыми разделителями
- **JSON файлы**: Структурированные данные
- **XML файлы**: Иерархические данные

## Поддерживаемые приёмники данных

### Базы данных
- **PostgreSQL**: С поддержкой batch-вставок
- **ClickHouse**: Оптимизированные вставки для аналитики
- **MySQL**: Совместимость с MySQL/MariaDB

### Потоковые системы
- **Kafka**: Публикация в топики с настраиваемыми ключами

### Файловые системы
- **CSV**: Экспорт с настраиваемыми разделителями
- **JSON**: Структурированный экспорт
- **XML**: Иерархический экспорт

## Архитектура DAG с Benthos

### Структура DAG
```
start_task >> benthos_transfer >> end_task
```

### Компоненты DAG
1. **start_task**: Инициализация процесса
2. **benthos_transfer**: Основная задача переливки данных
3. **end_task**: Завершение процесса

### Конфигурация Benthos
Каждый DAG создает временную конфигурацию Benthos в формате YAML:

```yaml
input:
  sql_select:
    driver: "postgresql"
    dsn: "postgresql://user:pass@host:port/db"
    table: "source_table"
    columns: ["*"]
    where: "1=1"
    batch_count: 1000

buffer:
  memory:
    batch_size: 1000

pipeline:
  processors:
    - mapping:
        root: "."
        mapping: "root = this"

output:
  sql_insert:
    driver: "clickhouse"
    dsn: "clickhouse://user:pass@host:port/db"
    table: "target_table"
    columns: ["*"]
    batch_size: 1000
```

## Типы переливки данных

### 1. Файл → База данных
- **Источник**: SQLite таблица
- **Приёмник**: PostgreSQL, ClickHouse, MySQL
- **Особенности**: Batch-обработка, оптимизированные вставки

### 2. База данных → База данных
- **Источник**: PostgreSQL, ClickHouse
- **Приёмник**: PostgreSQL, ClickHouse, MySQL
- **Особенности**: Прямая переливка между БД

### 3. Файл → Kafka
- **Источник**: SQLite таблица
- **Приёмник**: Kafka топик
- **Особенности**: JSON сериализация, batch-публикация

### 4. Kafka → База данных
- **Источник**: Kafka топик
- **Приёмник**: PostgreSQL, ClickHouse, MySQL
- **Особенности**: JSON десериализация, batch-вставки

### 5. База данных → Kafka
- **Источник**: PostgreSQL, ClickHouse
- **Приёмник**: Kafka топик
- **Особенности**: JSON сериализация, batch-публикация

### 6. Файл → Файл
- **Источник**: SQLite таблица
- **Приёмник**: CSV, JSON, XML файлы
- **Особенности**: Конвертация форматов

## Конфигурация Benthos

### Источники данных

#### SQLite
```yaml
input:
  sql_select:
    driver: "sqlite3"
    dsn: "file:/path/to/database.db"
    table: "table_name"
    columns: ["*"]
    where: "1=1"
    batch_count: 1000
```

#### PostgreSQL
```yaml
input:
  sql_select:
    driver: "postgresql"
    dsn: "postgresql://user:pass@host:port/db"
    table: "table_name"
    columns: ["*"]
    where: "1=1"
    batch_count: 1000
```

#### ClickHouse
```yaml
input:
  sql_select:
    driver: "clickhouse"
    dsn: "clickhouse://user:pass@host:port/db"
    table: "table_name"
    columns: ["*"]
    where: "1=1"
    batch_count: 1000
```

#### Kafka
```yaml
input:
  kafka:
    addresses: ["localhost:9092"]
    topics: ["topic_name"]
    consumer_group: "benthos-consumer"
```

### Приёмники данных

#### PostgreSQL
```yaml
output:
  sql_insert:
    driver: "postgresql"
    dsn: "postgresql://user:pass@host:port/db"
    table: "table_name"
    columns: ["*"]
    batch_size: 1000
```

#### ClickHouse
```yaml
output:
  sql_insert:
    driver: "clickhouse"
    dsn: "clickhouse://user:pass@host:port/db"
    table: "table_name"
    columns: ["*"]
    batch_size: 1000
```

#### Kafka
```yaml
output:
  kafka:
    addresses: ["localhost:9092"]
    topic: "topic_name"
    key: "{{.id}}"
    batch_size: 1000
```

#### Файлы
```yaml
output:
  file:
    path: "/path/to/output.csv"
    codec: "csv"
```

## Обработка данных

### Pipeline процессоры

#### Базовое маппирование
```yaml
pipeline:
  processors:
    - mapping:
        root: "."
        mapping: "root = this"
```

#### JSON сериализация (для Kafka)
```yaml
pipeline:
  processors:
    - mapping:
        root: "."
        mapping: "root = this"
    - json:
        operator: "to_json"
```

#### JSON десериализация (из Kafka)
```yaml
pipeline:
  processors:
    - json:
        operator: "from_json"
    - mapping:
        root: "."
        mapping: "root = this"
```

## Мониторинг и логирование

### Логирование
- Все операции Benthos логируются в Airflow
- Поддержка различных уровней логирования
- Детальная информация об ошибках

### Метрики
- Количество обработанных записей
- Время выполнения операций
- Статистика ошибок

### Обработка ошибок
- Автоматические повторные попытки
- Таймауты для длительных операций
- Graceful shutdown при ошибках

## Требования к системе

### Установка Benthos
```bash
# Установка Benthos
curl -Lsf https://sh.benthos.dev | bash

# Или через Docker
docker pull jeffail/benthos
```

### Зависимости Python
```python
# В requirements.txt
pyyaml>=6.0
```

### Переменные окружения
```bash
# Путь к Benthos (если не в PATH)
export BENTHOS_PATH=/usr/local/bin/benthos
```

## Примеры использования

### Создание DAG для переливки PostgreSQL → ClickHouse
```python
generator = AirflowDAGGenerator()
dag_content = generator.generate_data_transfer_dag(
    source_config={
        'type': 'postgresql',
        'host': 'localhost',
        'port': 5432,
        'database': 'source_db',
        'username': 'user',
        'password': 'pass',
        'table_name': 'users'
    },
    sink_config={
        'type': 'clickhouse',
        'host': 'localhost',
        'port': 8123,
        'database': 'target_db',
        'username': 'user',
        'password': 'pass',
        'table_name': 'users'
    },
    chunk_size=1000
)
```

### Создание DAG для переливки SQLite → Kafka
```python
generator = AirflowDAGGenerator()
dag_content = generator.generate_data_transfer_dag(
    file_id=123,
    source_table='file_data_123',
    sink_config={
        'type': 'kafka',
        'host': 'localhost:9092',
        'database': 'users_topic'
    },
    chunk_size=500
)
```

## Преимущества новой архитектуры

1. **Производительность**: Benthos оптимизирован для высокопроизводительной обработки данных
2. **Надежность**: Встроенные механизмы обработки ошибок и восстановления
3. **Масштабируемость**: Поддержка горизонтального масштабирования
4. **Гибкость**: Легкая настройка для различных сценариев использования
5. **Мониторинг**: Встроенные метрики и логирование
6. **Совместимость**: Поддержка широкого спектра источников и приёмников данных

## Миграция с предыдущей версии

Старые DAG'и с прямыми SQL-запросами будут продолжать работать, но новые DAG'и будут использовать Benthos. Это обеспечивает обратную совместимость и плавную миграцию.

## Заключение

Интеграция Benthos в систему переливки данных значительно улучшает производительность, надежность и гибкость системы. Новая архитектура обеспечивает поддержку широкого спектра источников и приёмников данных с минимальными изменениями в коде.
