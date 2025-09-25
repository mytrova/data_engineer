import { useState, useEffect } from 'react'

interface TransferResult {
  result?: string
  out_path?: string
  headers?: string[]
  rows?: any[]
}

// Функции для работы с localStorage
const saveToStorage = (key: string, value: any) => {
  try {
    localStorage.setItem(key, JSON.stringify(value))
  } catch (error) {
    console.warn('Не удалось сохранить в localStorage:', error)
  }
}

const loadFromStorage = (key: string, defaultValue: any) => {
  try {
    const item = localStorage.getItem(key)
    return item ? JSON.parse(item) : defaultValue
  } catch (error) {
    console.warn('Не удалось загрузить из localStorage:', error)
    return defaultValue
  }
}

export const useDataTransfer = () => {
  const [file, setFile] = useState<File | null>(null)
  const [fileName, setFileName] = useState<string>('')
  const [sourceType, setSourceType] = useState('csv')
  const [sinkType, setSinkType] = useState('preview')
  const [chunkSize, setChunkSize] = useState(10)
  const [sourceDelimiter, setSourceDelimiter] = useState(';')
  const [sinkDelimiter, setSinkDelimiter] = useState(';')
  
  // Поля для источника БД
  const [sourceDbHost, setSourceDbHost] = useState('')
  const [sourceDbPort, setSourceDbPort] = useState('5432')
  const [sourceDbDatabase, setSourceDbDatabase] = useState('')
  const [sourceDbUsername, setSourceDbUsername] = useState('')
  const [sourceDbPassword, setSourceDbPassword] = useState('')
  const [sourceDbTableName, setSourceDbTableName] = useState('')
  
  // Поля для приёмника БД
  const [sinkDbHost, setSinkDbHost] = useState('')
  const [sinkDbPort, setSinkDbPort] = useState('5432')
  const [sinkDbDatabase, setSinkDbDatabase] = useState('')
  const [sinkDbUsername, setSinkDbUsername] = useState('')
  const [sinkDbPassword, setSinkDbPassword] = useState('')
  const [sinkDbTableName, setSinkDbTableName] = useState('')
  
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<TransferResult | null>(null)

  // Загружаем данные из localStorage при инициализации
  useEffect(() => {
    const savedData = loadFromStorage('dataTransferSettings', {})
    
    if (savedData.sourceType) setSourceType(savedData.sourceType)
    if (savedData.sinkType) setSinkType(savedData.sinkType)
    if (savedData.chunkSize) setChunkSize(savedData.chunkSize)
    if (savedData.sourceDelimiter) setSourceDelimiter(savedData.sourceDelimiter)
    if (savedData.sinkDelimiter) setSinkDelimiter(savedData.sinkDelimiter)
    if (savedData.fileName) setFileName(savedData.fileName)
    
    // Поля источника БД
    if (savedData.sourceDbHost) setSourceDbHost(savedData.sourceDbHost)
    if (savedData.sourceDbPort) setSourceDbPort(savedData.sourceDbPort)
    if (savedData.sourceDbDatabase) setSourceDbDatabase(savedData.sourceDbDatabase)
    if (savedData.sourceDbUsername) setSourceDbUsername(savedData.sourceDbUsername)
    if (savedData.sourceDbPassword) setSourceDbPassword(savedData.sourceDbPassword)
    if (savedData.sourceDbTableName) setSourceDbTableName(savedData.sourceDbTableName)
    
    // Поля приёмника БД
    if (savedData.sinkDbHost) setSinkDbHost(savedData.sinkDbHost)
    if (savedData.sinkDbPort) setSinkDbPort(savedData.sinkDbPort)
    if (savedData.sinkDbDatabase) setSinkDbDatabase(savedData.sinkDbDatabase)
    if (savedData.sinkDbUsername) setSinkDbUsername(savedData.sinkDbUsername)
    if (savedData.sinkDbPassword) setSinkDbPassword(savedData.sinkDbPassword)
    if (savedData.sinkDbTableName) setSinkDbTableName(savedData.sinkDbTableName)
  }, [])

  // Сохраняем данные в localStorage при изменении
  useEffect(() => {
    const settings = {
      sourceType,
      sinkType,
      chunkSize,
      sourceDelimiter,
      sinkDelimiter,
      fileName,
      sourceDbHost,
      sourceDbPort,
      sourceDbDatabase,
      sourceDbUsername,
      sourceDbPassword,
      sourceDbTableName,
      sinkDbHost,
      sinkDbPort,
      sinkDbDatabase,
      sinkDbUsername,
      sinkDbPassword,
      sinkDbTableName
    }
    saveToStorage('dataTransferSettings', settings)
  }, [
    sourceType, sinkType, chunkSize, sourceDelimiter, sinkDelimiter, fileName,
    sourceDbHost, sourceDbPort, sourceDbDatabase, sourceDbUsername, sourceDbPassword, sourceDbTableName,
    sinkDbHost, sinkDbPort, sinkDbDatabase, sinkDbUsername, sinkDbPassword, sinkDbTableName
  ])

  const getApiBase = () => {
    const hostname = window.location.hostname
    if (hostname === 'localhost' || hostname === '127.0.0.1') {
      return 'http://localhost:8000'
    }
    return window.location.origin.replace(':3000', ':8000')
  }

  const handleTransfer = async () => {
    // Проверяем, нужен ли файл для выбранного типа источника
    const needsFile = ['csv', 'json', 'xml'].includes(sourceType)
    if (needsFile && !file) {
      setError('Пожалуйста, выберите файл')
      return
    }

    // Проверяем, нужны ли параметры БД для приёмника
    if (sinkType === 'database') {
      if (!sinkDbHost || !sinkDbPort || !sinkDbDatabase || !sinkDbUsername || !sinkDbPassword || !sinkDbTableName) {
        setError('Пожалуйста, заполните все поля подключения к базе данных для приёмника')
        return
      }
    }

    setIsLoading(true)
    setError(null)
    setResult(null)

    try {
      let endpoint: string
      let formData: FormData

      if (sinkType === 'database') {
        // Используем специальный эндпоинт для переноса в БД
        endpoint = '/transfer/to-database'
        formData = new FormData()
        
        // Параметры источника
        formData.append('source_type', sourceType)
        if (file) {
          formData.append('file', file)
        }
        
        // Параметры приёмника БД
        formData.append('sink_connection_string', `postgresql://${sinkDbUsername}:${sinkDbPassword}@${sinkDbHost}:${sinkDbPort}/${sinkDbDatabase}`)
        formData.append('sink_table_name', sinkDbTableName)
        formData.append('sink_mode', 'append')
        formData.append('chunk_size', chunkSize.toString())
        formData.append('delimiter', sourceDelimiter)
        formData.append('database_type', 'postgresql')
        
        // Если источник тоже БД
        if (sourceType === 'database') {
          if (!sourceDbHost || !sourceDbPort || !sourceDbDatabase || !sourceDbUsername || !sourceDbPassword) {
            throw new Error('Пожалуйста, заполните все поля подключения к базе данных для источника')
          }
          formData.append('source_connection_string', `postgresql://${sourceDbUsername}:${sourceDbPassword}@${sourceDbHost}:${sourceDbPort}/${sourceDbDatabase}`)
          formData.append('source_table_name', sourceDbTableName)
        }
      } else {
        // Обычный перенос в файлы
        endpoint = sinkType === 'preview' ? '/upload' : '/transfer'
        formData = new FormData()
        
        if (file) {
          formData.append('file', file)
        }
        formData.append('source_type', sourceType)
        formData.append('sink_type', sinkType)
        formData.append('chunk_size', chunkSize.toString())
        
        // Используем соответствующий разделитель в зависимости от типа операции
        const delimiter = sinkType === 'preview' ? sourceDelimiter : sinkDelimiter
        formData.append('delimiter', delimiter)
      }

      const response = await fetch(`${getApiBase()}${endpoint}`, {
        method: 'POST',
        body: formData,
      })

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}))
        const errorMessage = errorData.error || `Ошибка сервера: ${response.status}`
        throw new Error(typeof errorMessage === 'string' ? errorMessage : JSON.stringify(errorMessage))
      }

      if (sinkType === 'preview' || sinkType === 'database') {
        const data = await response.json()
        setResult(data)
      } else {
        // Для файловых приёмников
        const blob = await response.blob()
        const url = window.URL.createObjectURL(blob)
        
        // Пытаемся получить имя файла из заголовков
        const contentDisposition = response.headers.get('Content-Disposition') || ''
        const match = contentDisposition.match(/filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/)
        let filename = 'export'
        if (match) {
          filename = decodeURIComponent(match[1] || match[2] || filename)
        } else {
          filename = `export.${sinkType}`
        }

        // Создаем ссылку для скачивания
        const link = document.createElement('a')
        link.href = url
        link.download = filename
        document.body.appendChild(link)
        link.click()
        document.body.removeChild(link)
        window.URL.revokeObjectURL(url)

        setResult({
          result: 'Файл успешно обработан и скачан',
          out_path: filename
        })
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Произошла неизвестная ошибка')
    } finally {
      setIsLoading(false)
    }
  }

  const reset = () => {
    setFile(null)
    setFileName('')
    setSourceType('csv')
    setSinkType('preview')
    setChunkSize(10)
    setSourceDelimiter(';')
    setSinkDelimiter(';')
    setSourceDbHost('')
    setSourceDbPort('5432')
    setSourceDbDatabase('')
    setSourceDbUsername('')
    setSourceDbPassword('')
    setSourceDbTableName('')
    setSinkDbHost('')
    setSinkDbPort('5432')
    setSinkDbDatabase('')
    setSinkDbUsername('')
    setSinkDbPassword('')
    setSinkDbTableName('')
    setIsLoading(false)
    setError(null)
    setResult(null)
    
    // Очищаем localStorage
    try {
      localStorage.removeItem('dataTransferSettings')
    } catch (error) {
      console.warn('Не удалось очистить localStorage:', error)
    }
  }

  return {
    file,
    fileName,
    sourceType,
    sinkType,
    chunkSize,
    sourceDelimiter,
    sinkDelimiter,
    sourceDbHost,
    sourceDbPort,
    sourceDbDatabase,
    sourceDbUsername,
    sourceDbPassword,
    sourceDbTableName,
    sinkDbHost,
    sinkDbPort,
    sinkDbDatabase,
    sinkDbUsername,
    sinkDbPassword,
    sinkDbTableName,
    isLoading,
    error,
    result,
    setFile,
    setFileName,
    setSourceType,
    setSinkType,
    setChunkSize,
    setSourceDelimiter,
    setSinkDelimiter,
    setSourceDbHost,
    setSourceDbPort,
    setSourceDbDatabase,
    setSourceDbUsername,
    setSourceDbPassword,
    setSourceDbTableName,
    setSinkDbHost,
    setSinkDbPort,
    setSinkDbDatabase,
    setSinkDbUsername,
    setSinkDbPassword,
    setSinkDbTableName,
    handleTransfer,
    reset
  }
}
