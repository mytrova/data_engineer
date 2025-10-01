/**
 * Утилиты для работы с API
 */

/**
 * Определяет базовый URL для API в зависимости от окружения
 */
export const getApiBase = (): string => {
  const hostname = window.location.hostname
  if (hostname === 'localhost' || hostname === '127.0.0.1') {
    return 'http://localhost:8000'
  }
  return window.location.origin.replace(':3000', ':8000')
}

/**
 * Создает полный URL для API эндпоинта
 */
export const getApiUrl = (endpoint: string): string => {
  const base = getApiBase()
  const cleanEndpoint = endpoint.startsWith('/') ? endpoint : `/${endpoint}`
  return `${base}${cleanEndpoint}`
}
