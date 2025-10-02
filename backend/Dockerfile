FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Обновляем pip и устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Настраиваем pip для более надежной установки
RUN pip install --upgrade pip setuptools wheel

COPY backend/requirements.txt /app/requirements.txt

# Устанавливаем зависимости поэтапно для избежания таймаутов
# Сначала устанавливаем основные зависимости
RUN pip install --no-cache-dir \
    --timeout 1000 \
    --retries 5 \
    --trusted-host pypi.org \
    --trusted-host pypi.python.org \
    --trusted-host files.pythonhosted.org \
    fastapi==0.115.2 uvicorn[standard]==0.30.6 python-multipart==0.0.9

# Устанавливаем остальные зависимости
RUN pip install --no-cache-dir \
    --timeout 1000 \
    --retries 5 \
    --trusted-host pypi.org \
    --trusted-host pypi.python.org \
    --trusted-host files.pythonhosted.org \
    pytest==8.3.3 httpx==0.27.2 requests==2.31.0 psycopg2-binary==2.9.9 sqlalchemy==1.4.53 alembic==1.13.1 docker==7.1.0 clickhouse-driver==0.2.6 clickhouse-sqlalchemy==0.2.0 kafka-python==2.0.2 confluent-kafka==2.3.0 pyyaml==6.0.1 aiohttp==3.9.1

COPY backend /app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]


