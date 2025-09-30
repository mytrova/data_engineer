FROM apache/airflow:2.8.1-python3.11

# Установка дополнительных пакетов
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        postgresql-client \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Переключение обратно на пользователя airflow
USER airflow

# Копирование DAG файлов
COPY airflow/dags/ /opt/airflow/dags/

# Создание структуры директорий
RUN mkdir -p /opt/airflow/backend/app/services

# Копирование модулей backend
COPY backend/app/services/schema_mapper.py /opt/airflow/backend/app/services/schema_mapper.py
COPY airflow/backend/__init__.py /opt/airflow/backend/__init__.py
COPY airflow/backend/app/__init__.py /opt/airflow/backend/app/__init__.py
COPY airflow/backend/app/services/__init__.py /opt/airflow/backend/app/services/__init__.py

# Копирование и установка зависимостей
COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Установка переменных окружения
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
ENV AIRFLOW__WEBSERVER__SECRET_KEY=your-secret-key-here
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
ENV AIRFLOW__CORE__FERNET_KEY=your-fernet-key-here
ENV AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
ENV AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False

# Создание директории для логов
RUN mkdir -p /opt/airflow/logs

# Команда по умолчанию
CMD ["airflow", "standalone"]
