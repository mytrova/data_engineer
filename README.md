# Data Orchestrator

Простой оркестратор переноса данных между источниками и приёмниками с поддержкой чанков.

## Возможности
- Источники: CSV, JSON, XML (загрузка файла через веб).
- Приёмники: Предпросмотр (первые строки), а также сохранение в файлы CSV/JSON/XML.
- Перенос чанками: укажите размер чанка (количество строк на шаг).
- Простой веб-интерфейс для выбора источника, приёмника и размера чанка.
- REST API на FastAPI.
- Docker и docker-compose для запуска.
- Тесты на pytest.

## Структура
- Backend (FastAPI): `backend/app`
  - Абстракции: `app/services/base.py` (`DataSource`, `DataSink`), поддержка чанков
  - Источники: `CSVSource`, `JSONSource`, `XMLSource`
  - Приёмники: `CSVFileSink`, `JSONFileSink`, `XMLFileSink`, `PreviewSink`
  - API: `POST /upload` (предпросмотр), `POST /transfer` (перенос с выбором типов)
- Frontend: `frontend/index.html`, `frontend/style.css`
- Docker: `backend/Dockerfile`, `frontend/Dockerfile`, `docker-compose.yml`
- Тесты: `backend/tests`

## Запуск
```bash
cd /Users/marina/data
docker compose up -d --build
```

- Фронтенд: http://localhost:3000
- Бэкенд: http://localhost:8000/health

## Использование
### Веб-интерфейс
1. Выберите тип источника (CSV/JSON/XML).
2. Выберите тип приёмника (Предпросмотр/CSV/JSON/XML).
3. Укажите размер чанка (по умолчанию 10).
4. Выберите файл и нажмите «Запустить».
5. Для «Предпросмотра» появится таблица; для файлов – путь к сохранённому файлу внутри контейнера (`/tmp/output.*`).

### API
- Предпросмотр CSV (совместим и с JSON/XML — будет попытка распарсить):
  - `POST /upload` form-data: `file` (файл). Ответ: `{ headers, rows }`.
- Перенос с выбором типов:
  - `POST /transfer` form-data: `source_type` (csv|json|xml), `sink_type` (preview|csv|json|xml), `chunk_size` (int), `file` (файл)
  - Ответ: `{ result, out_path, headers }`

## Тесты
Запуск тестов локально:
```bash
cd /Users/marina/data
docker compose exec backend pip install -r /app/requirements.txt
docker compose exec backend pytest -q
```

## Примечания
- Источники JSON: поддерживаются массивы объектов или `{"items": [...]}`; для массивов массивов заголовки будут `col1..colN`.
- Источники XML: по умолчанию ищется тег `item` (или берутся прямые дети корня), заголовки формируются из имён дочерних тегов.
- При сохранении файлов приёмники пишут в `/tmp` внутри контейнера бэкенда.


