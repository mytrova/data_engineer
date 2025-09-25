# Data Orchestrator Frontend

Современный React фронтенд для Data Orchestrator с красивым интерфейсом.

## Технологии

- **React 18** с TypeScript
- **Vite** для быстрой сборки
- **Styled Components** для стилизации
- **React Dropzone** для загрузки файлов
- **Lucide React** для иконок

## Возможности

- 🎨 Современный и отзывчивый дизайн
- 📁 Drag & Drop загрузка файлов
- ⚡ Быстрая обработка с прогресс-баром
- 📊 Предпросмотр данных в таблице
- 💾 Автоматическое скачивание результатов
- 🔄 Валидация и обработка ошибок

## Разработка

```bash
# Установка зависимостей
npm install

# Запуск в режиме разработки
npm run dev

# Сборка для продакшена
npm run build

# Предпросмотр сборки
npm run preview

# Линтинг
npm run lint
```

## Структура проекта

```
src/
├── components/          # React компоненты
│   ├── DataTransferForm.tsx
│   ├── FileUpload.tsx
│   ├── TransferOptions.tsx
│   ├── DataPreview.tsx
│   ├── ProgressBar.tsx
│   └── Header.tsx
├── hooks/              # Пользовательские хуки
│   └── useDataTransfer.ts
├── styles/             # Глобальные стили
│   └── GlobalStyles.ts
├── App.tsx             # Главный компонент
├── main.tsx           # Точка входа
└── index.css          # Базовые стили
```

## Docker

Приложение автоматически собирается и запускается через Docker Compose.

```bash
# Сборка и запуск
docker compose up -d --build

# Фронтенд будет доступен на http://localhost:3000
```
