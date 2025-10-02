import { useState, useEffect } from 'react'

interface TransferResult {
  result?: string
  out_path?: string
  headers?: string[]
  rows?: any[]
  message?: string
  dag_id?: string
  status?: string
  dag_run?: any
  error?: string
  file_id?: number
  file_path?: string
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
  
  // Поля для источника PostgreSQL
  const [sourceDbHost, setSourceDbHost] = useState('')
  const [sourceDbPort, setSourceDbPort] = useState('5432')
  const [sourceDbDatabase, setSourceDbDatabase] = useState('')
  const [sourceDbUsername, setSourceDbUsername] = useState('')
  const [sourceDbPassword, setSourceDbPassword] = useState('')
  const [sourceDbTableName, setSourceDbTableName] = useState('')
  
  // Поля для источника ClickHouse
  const [sourceChHost, setSourceChHost] = useState('')
  const [sourceChPort, setSourceChPort] = useState('8123')
  const [sourceChDatabase, setSourceChDatabase] = useState('')
  const [sourceChUsername, setSourceChUsername] = useState('')
  const [sourceChPassword, setSourceChPassword] = useState('')
  const [sourceChTableName, setSourceChTableName] = useState('')
  
  // Поля для источника Kafka
  const [sourceKafkaBootstrapServers, setSourceKafkaBootstrapServers] = useState('')
  const [sourceKafkaTopic, setSourceKafkaTopic] = useState('')
  const [sourceKafkaGroupId, setSourceKafkaGroupId] = useState('')
  
  // Поля для приёмника PostgreSQL
  const [sinkDbHost, setSinkDbHost] = useState('')
  const [sinkDbPort, setSinkDbPort] = useState('5432')
  const [sinkDbDatabase, setSinkDbDatabase] = useState('')
  const [sinkDbUsername, setSinkDbUsername] = useState('')
  const [sinkDbPassword, setSinkDbPassword] = useState('')
  const [sinkDbTableName, setSinkDbTableName] = useState('')
  
  // Поля для приёмника ClickHouse
  const [sinkChHost, setSinkChHost] = useState('')
  const [sinkChPort, setSinkChPort] = useState('8123')
  const [sinkChDatabase, setSinkChDatabase] = useState('')
  const [sinkChUsername, setSinkChUsername] = useState('')
  const [sinkChPassword, setSinkChPassword] = useState('')
  const [sinkChTableName, setSinkChTableName] = useState('')
  
  // Поля для приёмника Kafka
  const [sinkKafkaBootstrapServers, setSinkKafkaBootstrapServers] = useState('')
  const [sinkKafkaTopic, setSinkKafkaTopic] = useState('')
  const [sinkKafkaKeyField, setSinkKafkaKeyField] = useState('')
  
  const [useAirflow, setUseAirflow] = useState(false)
  
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
    
    // Поля источника PostgreSQL
    if (savedData.sourceDbHost) setSourceDbHost(savedData.sourceDbHost)
    if (savedData.sourceDbPort) setSourceDbPort(savedData.sourceDbPort)
    if (savedData.sourceDbDatabase) setSourceDbDatabase(savedData.sourceDbDatabase)
    if (savedData.sourceDbUsername) setSourceDbUsername(savedData.sourceDbUsername)
    if (savedData.sourceDbPassword) setSourceDbPassword(savedData.sourceDbPassword)
    if (savedData.sourceDbTableName) setSourceDbTableName(savedData.sourceDbTableName)
    
    // Поля источника ClickHouse
    if (savedData.sourceChHost) setSourceChHost(savedData.sourceChHost)
    if (savedData.sourceChPort) setSourceChPort(savedData.sourceChPort)
    if (savedData.sourceChDatabase) setSourceChDatabase(savedData.sourceChDatabase)
    if (savedData.sourceChUsername) setSourceChUsername(savedData.sourceChUsername)
    if (savedData.sourceChPassword) setSourceChPassword(savedData.sourceChPassword)
    if (savedData.sourceChTableName) setSourceChTableName(savedData.sourceChTableName)
    
    // Поля источника Kafka
    if (savedData.sourceKafkaBootstrapServers) setSourceKafkaBootstrapServers(savedData.sourceKafkaBootstrapServers)
    if (savedData.sourceKafkaTopic) setSourceKafkaTopic(savedData.sourceKafkaTopic)
    if (savedData.sourceKafkaGroupId) setSourceKafkaGroupId(savedData.sourceKafkaGroupId)
    
    // Поля приёмника PostgreSQL
    if (savedData.sinkDbHost) setSinkDbHost(savedData.sinkDbHost)
    if (savedData.sinkDbPort) setSinkDbPort(savedData.sinkDbPort)
    if (savedData.sinkDbDatabase) setSinkDbDatabase(savedData.sinkDbDatabase)
    if (savedData.sinkDbUsername) setSinkDbUsername(savedData.sinkDbUsername)
    if (savedData.sinkDbPassword) setSinkDbPassword(savedData.sinkDbPassword)
    if (savedData.sinkDbTableName) setSinkDbTableName(savedData.sinkDbTableName)
    
    // Поля приёмника ClickHouse
    if (savedData.sinkChHost) setSinkChHost(savedData.sinkChHost)
    if (savedData.sinkChPort) setSinkChPort(savedData.sinkChPort)
    if (savedData.sinkChDatabase) setSinkChDatabase(savedData.sinkChDatabase)
    if (savedData.sinkChUsername) setSinkChUsername(savedData.sinkChUsername)
    if (savedData.sinkChPassword) setSinkChPassword(savedData.sinkChPassword)
    if (savedData.sinkChTableName) setSinkChTableName(savedData.sinkChTableName)
    
    // Поля приёмника Kafka
    if (savedData.sinkKafkaBootstrapServers) setSinkKafkaBootstrapServers(savedData.sinkKafkaBootstrapServers)
    if (savedData.sinkKafkaTopic) setSinkKafkaTopic(savedData.sinkKafkaTopic)
    if (savedData.sinkKafkaKeyField) setSinkKafkaKeyField(savedData.sinkKafkaKeyField)
  }, [])

  // Автоматически включаем Airflow если источник - база данных
  useEffect(() => {
    const isDatabaseSource = ['postgresql', 'clickhouse', 'kafka'].includes(sourceType)
    if (isDatabaseSource && !useAirflow) {
      setUseAirflow(true)
    }
  }, [sourceType, useAirflow])

  // Сохраняем данные в localStorage при изменении
  useEffect(() => {
    const settings = {
      sourceType,
      sinkType,
      chunkSize,
      sourceDelimiter,
      sinkDelimiter,
      fileName,
      // PostgreSQL источник
      sourceDbHost,
      sourceDbPort,
      sourceDbDatabase,
      sourceDbUsername,
      sourceDbPassword,
      sourceDbTableName,
      // ClickHouse источник
      sourceChHost,
      sourceChPort,
      sourceChDatabase,
      sourceChUsername,
      sourceChPassword,
      sourceChTableName,
      // Kafka источник
      sourceKafkaBootstrapServers,
      sourceKafkaTopic,
      sourceKafkaGroupId,
      // PostgreSQL приёмник
      sinkDbHost,
      sinkDbPort,
      sinkDbDatabase,
      sinkDbUsername,
      sinkDbPassword,
      sinkDbTableName,
      // ClickHouse приёмник
      sinkChHost,
      sinkChPort,
      sinkChDatabase,
      sinkChUsername,
      sinkChPassword,
      sinkChTableName,
      // Kafka приёмник
      sinkKafkaBootstrapServers,
      sinkKafkaTopic,
      sinkKafkaKeyField
    }
    saveToStorage('dataTransferSettings', settings)
  }, [
    sourceType, sinkType, chunkSize, sourceDelimiter, sinkDelimiter, fileName,
    sourceDbHost, sourceDbPort, sourceDbDatabase, sourceDbUsername, sourceDbPassword, sourceDbTableName,
    sourceChHost, sourceChPort, sourceChDatabase, sourceChUsername, sourceChPassword, sourceChTableName,
    sourceKafkaBootstrapServers, sourceKafkaTopic, sourceKafkaGroupId,
    sinkDbHost, sinkDbPort, sinkDbDatabase, sinkDbUsername, sinkDbPassword, sinkDbTableName,
    sinkChHost, sinkChPort, sinkChDatabase, sinkChUsername, sinkChPassword, sinkChTableName,
    sinkKafkaBootstrapServers, sinkKafkaTopic, sinkKafkaKeyField
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

    // Проверяем, является ли файл большим (более 20 МБ)
    if (needsFile && file && file.size > 20 * 1024 * 1024) {
      // Большой файл - используем специальный эндпоинт для прямой переливки
      setIsLoading(true)
      setError(null)
      setResult(null)

      try {
        // Проверяем, что приёмник - база данных (обязательно для больших файлов)
        if (sinkType !== 'database') {
          setError('Для больших файлов приёмник должен быть базой данных')
          setIsLoading(false)
          return
        }

        // Проверяем параметры подключения к БД
        if (!sinkDbHost || !sinkDbPort || !sinkDbDatabase || !sinkDbUsername || !sinkDbPassword || !sinkDbTableName) {
          setError('Пожалуйста, заполните все поля подключения к базе данных для приёмника')
          setIsLoading(false)
          return
        }

        const formData = new FormData()
        formData.append('file', file)
        
        // Конфигурация приёмника для прямой переливки
        const sinkConfig = {
          type: 'database',
          host: sinkDbHost,
          port: parseInt(sinkDbPort || '5432'),
          database: sinkDbDatabase,
          username: sinkDbUsername,
          password: sinkDbPassword,
          table_name: sinkDbTableName,
          db_type: 'postgresql'
        }
        
        formData.append('sink_config', JSON.stringify(sinkConfig))
        formData.append('chunk_size', chunkSize.toString())

        const response = await fetch(`${getApiBase()}/large-file/upload`, {
          method: 'POST',
          body: formData
        })

        if (!response.ok) {
          const errorData = await response.json().catch(() => ({}))
          throw new Error(errorData.error || `Ошибка сервера: ${response.status}`)
        }

        const data = await response.json()
        
        if (data.status === 'processing') {
          setResult({
            status: 'processing',
            message: data.message,
            file_path: data.file_path,
            result: 'Файл обрабатывается с прямой переливкой в базу данных. Проверьте статус в разделе "Управление большими файлами".'
          })
          
          // Очищаем источник данных после отправки на обработку
          setFile(null)
        } else {
          setResult(data)
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Произошла неизвестная ошибка')
      } finally {
        setIsLoading(false)
      }
      return
    }

    // Проверяем, нужны ли параметры БД для приёмника
    if (['postgresql', 'clickhouse', 'kafka'].includes(sinkType)) {
      if (sinkType === 'postgresql' && (!sinkDbHost || !sinkDbPort || !sinkDbDatabase || !sinkDbUsername || !sinkDbPassword || !sinkDbTableName)) {
        setError('Пожалуйста, заполните все поля подключения к PostgreSQL для приёмника')
        return
      }
      if (sinkType === 'clickhouse' && (!sinkChHost || !sinkChPort || !sinkChDatabase || !sinkChUsername || !sinkChPassword || !sinkChTableName)) {
        setError('Пожалуйста, заполните все поля подключения к ClickHouse для приёмника')
        return
      }
      if (sinkType === 'kafka' && (!sinkKafkaBootstrapServers || !sinkKafkaTopic)) {
        setError('Пожалуйста, заполните все поля подключения к Kafka для приёмника')
        return
      }
    }

    setIsLoading(true)
    setError(null)
    setResult(null)

    try {
      let endpoint: string
      let formData: FormData

      console.log('DEBUG: sinkType =', sinkType)
      console.log('DEBUG: useAirflow =', useAirflow)
      console.log('DEBUG: sourceType =', sourceType)

      if (['postgresql', 'clickhouse', 'kafka'].includes(sinkType)) {
        if (useAirflow) {
          let uploadedFileName = ''
          
          // Если источник - файл, сначала загружаем его
          if (!['postgresql', 'clickhouse', 'kafka'].includes(sourceType) && file) {
            const uploadFormData = new FormData()
            uploadFormData.append('file', file)
            
            const uploadResponse = await fetch('/api/airflow/upload-file', {
              method: 'POST',
              body: uploadFormData
            })
            
            const uploadData = await uploadResponse.json()
            if (uploadData.status === 'success') {
              uploadedFileName = uploadData.file_name
            } else if (uploadData.status === 'large_file') {
              // Большой файл - не создаем DAG, показываем сообщение
              setResult({
                status: 'large_file',
                message: uploadData.message,
                file_id: uploadData.file_id,
                file_path: uploadData.file_path,
                result: 'Файл сохранен и обрабатывается в фоновом режиме'
              })
              return
            } else {
              setResult({
                status: 'error',
                error: `Ошибка загрузки файла: ${uploadData.error}`
              })
              return
            }
          }
          
          // Создаем DAG для Airflow
          const sourceConfig = {
            type: ['postgresql', 'clickhouse', 'kafka'].includes(sourceType) ? sourceType : 'file',
            ...(['postgresql', 'clickhouse', 'kafka'].includes(sourceType) ? {
              host: sourceType === 'postgresql' ? sourceDbHost : 
                    sourceType === 'clickhouse' ? sourceChHost : 
                    sourceKafkaBootstrapServers,
              port: sourceType === 'postgresql' ? sourceDbPort : 
                    sourceType === 'clickhouse' ? sourceChPort : 
                    '9092',
              database: sourceType === 'postgresql' ? sourceDbDatabase : 
                       sourceType === 'clickhouse' ? sourceChDatabase : 
                       sourceKafkaTopic,
              username: sourceType === 'postgresql' ? sourceDbUsername : 
                       sourceType === 'clickhouse' ? sourceChUsername : 
                       '',
              password: sourceType === 'postgresql' ? sourceDbPassword : 
                       sourceType === 'clickhouse' ? sourceChPassword : 
                       '',
              table_name: sourceType === 'postgresql' ? sourceDbTableName : 
                         sourceType === 'clickhouse' ? sourceChTableName : 
                         sourceKafkaTopic
            } : {
              file_path: uploadedFileName ? `/opt/airflow/${uploadedFileName}` : '',
              file_type: sourceType,
              delimiter: sourceDelimiter
            })
          }
          
          const sinkConfig = {
            type: sinkType,
            host: sinkType === 'postgresql' ? sinkDbHost : 
                  sinkType === 'clickhouse' ? sinkChHost : 
                  sinkKafkaBootstrapServers,
            port: sinkType === 'postgresql' ? sinkDbPort : 
                  sinkType === 'clickhouse' ? sinkChPort : 
                  '9092',
            database: sinkType === 'postgresql' ? sinkDbDatabase : 
                     sinkType === 'clickhouse' ? sinkChDatabase : 
                     sinkKafkaTopic,
            username: sinkType === 'postgresql' ? sinkDbUsername : 
                     sinkType === 'clickhouse' ? sinkChUsername : 
                     '',
            password: sinkType === 'postgresql' ? sinkDbPassword : 
                     sinkType === 'clickhouse' ? sinkChPassword : 
                     '',
            table_name: sinkType === 'postgresql' ? sinkDbTableName : 
                       sinkType === 'clickhouse' ? sinkChTableName : 
                       sinkKafkaTopic
          }
          
          // Создаем DAG
          const dagResponse = await fetch(`${getApiBase()}/airflow/generate-dag`, {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              source_config: sourceConfig,
              sink_config: sinkConfig,
              chunk_size: chunkSize,
              total_rows: null // Пока не знаем общее количество строк
            })
          })
          
          if (!dagResponse.ok) {
            const errorData = await dagResponse.json().catch(() => ({}))
            throw new Error(errorData.error || 'Ошибка при создании DAG')
          }
          
          const dagData = await dagResponse.json()
          
          // Пытаемся запустить DAG сразу после создания
          const dagInfo = {
            message: `DAG ${dagData.dag_id} создан. Попытка запуска...`,
            dag_id: dagData.dag_id,
            status: 'creating',
            result: 'Создание DAG в фоновом режиме...'
          }
          
          setResult(dagInfo)
          
          // Сохраняем в localStorage для отслеживания в Airflow Dashboard
          const existingDAGs = JSON.parse(localStorage.getItem('creating_dags') || '{}')
          existingDAGs[dagData.dag_id] = dagInfo
          localStorage.setItem('creating_dags', JSON.stringify(existingDAGs))
          
                 // Функция для попытки запуска DAG с повторными попытками
                 const attemptTriggerDAG = async (attemptNumber = 1, maxAttempts = 50) => {
            try {
              // Сначала принудительно обновляем DAG'и в Airflow (каждые 3 попытки)
              if (attemptNumber % 3 === 1) {
                try {
                  await fetch(`${getApiBase()}/airflow/dags/reserialize`, { method: 'POST' })
                  console.log(`Попытка ${attemptNumber}: DAG'и обновлены в Airflow`)
                } catch (reserializeError) {
                  console.log(`Попытка ${attemptNumber}: Не удалось обновить DAG'и:`, reserializeError)
                }
              }
              
              // Проверяем, загрузился ли DAG в Airflow
              const dagStatusResponse = await fetch(`${getApiBase()}/airflow/dags/${dagData.dag_id}`)
              
              if (dagStatusResponse.ok) {
                // DAG загружен, пытаемся запустить
                const triggerResponse = await fetch(`${getApiBase()}/airflow/dags/${dagData.dag_id}/trigger`, {
                  method: 'POST'
                })
                
                if (triggerResponse.ok) {
                  const triggerData = await triggerResponse.json()
                  const updatedDagInfo = {
                    message: `DAG ${dagData.dag_id} запущен и выполняется в фоне`,
                    dag_id: dagData.dag_id,
                    status: 'running',
                    dag_run: triggerData.dag_run,
                    result: 'Переливка данных выполняется в фоновом режиме через Airflow'
                  }
                  setResult(updatedDagInfo)
                  
                  // Обновляем localStorage
                  const existingDAGs = JSON.parse(localStorage.getItem('creating_dags') || '{}')
                  existingDAGs[dagData.dag_id] = updatedDagInfo
                  localStorage.setItem('creating_dags', JSON.stringify(existingDAGs))
                  return // Успешно запущен
                }
              }
              
                     // Если не удалось запустить и есть еще попытки
                     if (attemptNumber < maxAttempts) {
                       const delay = Math.min(attemptNumber * 3, 60) * 1000 // Увеличиваем задержку: 3s, 6s, 9s, 12s, 15s... до 60s
                
                const updatedDagInfo = {
                  message: `DAG ${dagData.dag_id} создан. Попытка запуска ${attemptNumber}/${maxAttempts}...`,
                  dag_id: dagData.dag_id,
                  status: 'creating',
                  result: `Попытка запуска ${attemptNumber}/${maxAttempts}...`
                }
                setResult(updatedDagInfo)
                
                // Обновляем localStorage
                const existingDAGs = JSON.parse(localStorage.getItem('creating_dags') || '{}')
                existingDAGs[dagData.dag_id] = updatedDagInfo
                localStorage.setItem('creating_dags', JSON.stringify(existingDAGs))
                
                // Повторяем попытку через delay
                setTimeout(() => attemptTriggerDAG(attemptNumber + 1, maxAttempts), delay)
              } else {
                // Все попытки исчерпаны
                const updatedDagInfo = {
                  message: `DAG ${dagData.dag_id} создан, но не удалось запустить автоматически. Запустите вручную в Airflow UI.`,
                  dag_id: dagData.dag_id,
                  status: 'created',
                  result: 'DAG создан, но требует ручного запуска в Airflow UI'
                }
                setResult(updatedDagInfo)
                
                // Обновляем localStorage
                const existingDAGs = JSON.parse(localStorage.getItem('creating_dags') || '{}')
                existingDAGs[dagData.dag_id] = updatedDagInfo
                localStorage.setItem('creating_dags', JSON.stringify(existingDAGs))
              }
            } catch (error) {
                     // Ошибка при попытке запуска
                     if (attemptNumber < maxAttempts) {
                       const delay = Math.min(attemptNumber * 3, 60) * 1000
                setTimeout(() => attemptTriggerDAG(attemptNumber + 1, maxAttempts), delay)
              } else {
                const errorDagInfo = {
                  message: `DAG ${dagData.dag_id} создан, но произошла ошибка при запуске: ${error}`,
                  dag_id: dagData.dag_id,
                  status: 'error',
                  result: `Ошибка при запуске DAG: ${error}`
                }
                setResult(errorDagInfo)
                
                // Обновляем localStorage
                const existingDAGs = JSON.parse(localStorage.getItem('creating_dags') || '{}')
                existingDAGs[dagData.dag_id] = errorDagInfo
                localStorage.setItem('creating_dags', JSON.stringify(existingDAGs))
              }
            }
          }
          
                 // Запускаем первую попытку через 15 секунд
                 setTimeout(() => attemptTriggerDAG(1, 50), 15000)
          
          return // Выходим из функции, так как процесс асинхронный
        } else {
          // Обычная переливка в БД
          endpoint = '/transfer/to-database'
          formData = new FormData()
          
          // Параметры источника
          formData.append('source_type', sourceType)
          if (file) {
            formData.append('file', file)
          }
          
          // Параметры приёмника БД
          formData.append('sink_mode', 'append')
          formData.append('chunk_size', chunkSize.toString())
          formData.append('delimiter', sourceDelimiter)
          
          // Определяем тип приёмника и добавляем соответствующие параметры
          if (sinkType === 'postgresql') {
            formData.append('sink_connection_string', `postgresql://${sinkDbUsername}:${sinkDbPassword}@${sinkDbHost}:${sinkDbPort}/${sinkDbDatabase}`)
            formData.append('sink_table_name', sinkDbTableName)
            formData.append('database_type', 'postgresql')
          } else if (sinkType === 'clickhouse') {
            // Проверяем параметры ClickHouse
            if (!sinkChHost || !sinkChPort || !sinkChDatabase || !sinkChUsername || !sinkChPassword || !sinkChTableName) {
              setError('Пожалуйста, заполните все поля подключения к ClickHouse для приёмника')
              setIsLoading(false)
              return
            }
            
            console.log('ClickHouse parameters:', {
              host: sinkChHost,
              port: sinkChPort,
              database: sinkChDatabase,
              username: sinkChUsername,
              password: sinkChPassword,
              table: sinkChTableName
            })
            
            formData.append('sink_host', sinkChHost)
            formData.append('sink_port', sinkChPort)
            formData.append('sink_database', sinkChDatabase)
            formData.append('sink_username', sinkChUsername)
            formData.append('sink_password', sinkChPassword)
            formData.append('sink_table_name', sinkChTableName)
            formData.append('database_type', 'clickhouse')
          } else if (sinkType === 'kafka') {
            formData.append('sink_bootstrap_servers', sinkKafkaBootstrapServers)
            formData.append('sink_topic', sinkKafkaTopic)
            formData.append('sink_key_field', sinkKafkaKeyField)
            formData.append('database_type', 'kafka')
          }
          
          // Если источник тоже БД
          if (sourceType === 'postgresql') {
            if (!sourceDbHost || !sourceDbPort || !sourceDbDatabase || !sourceDbUsername || !sourceDbPassword) {
              throw new Error('Пожалуйста, заполните все поля подключения к базе данных для источника')
            }
            formData.append('source_connection_string', `postgresql://${sourceDbUsername}:${sourceDbPassword}@${sourceDbHost}:${sourceDbPort}/${sourceDbDatabase}`)
            formData.append('source_table_name', sourceDbTableName)
          } else if (sourceType === 'clickhouse') {
            if (!sourceChHost || !sourceChPort || !sourceChDatabase || !sourceChUsername || !sourceChPassword) {
              throw new Error('Пожалуйста, заполните все поля подключения к ClickHouse для источника')
            }
            formData.append('source_host', sourceChHost)
            formData.append('source_port', sourceChPort)
            formData.append('source_database', sourceChDatabase)
            formData.append('source_username', sourceChUsername)
            formData.append('source_password', sourceChPassword)
            formData.append('source_table_name', sourceChTableName)
          } else if (sourceType === 'kafka') {
            if (!sourceKafkaBootstrapServers || !sourceKafkaTopic) {
              throw new Error('Пожалуйста, заполните все поля подключения к Kafka для источника')
            }
            formData.append('source_bootstrap_servers', sourceKafkaBootstrapServers)
            formData.append('source_topic', sourceKafkaTopic)
            formData.append('source_group_id', sourceKafkaGroupId)
          }
        }
      } else {
        // Обычный перенос в файлы
        console.log('DEBUG: Используем обычный перенос в файлы')
        endpoint = sinkType === 'preview' ? '/upload' : '/transfer'
        console.log('DEBUG: endpoint =', endpoint)
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
        
        // Проверяем, является ли файл большим
        if (data.status === 'large_file') {
          setResult({
            status: 'large_file',
            message: data.message,
            file_id: data.file_id,
            file_path: data.file_path,
            result: 'Файл сохранен и обрабатывается в фоновом режиме'
          })
        } else {
          setResult(data)
        }
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
    // PostgreSQL источник
    setSourceDbHost('')
    setSourceDbPort('5432')
    setSourceDbDatabase('')
    setSourceDbUsername('')
    setSourceDbPassword('')
    setSourceDbTableName('')
    // ClickHouse источник
    setSourceChHost('')
    setSourceChPort('8123')
    setSourceChDatabase('')
    setSourceChUsername('')
    setSourceChPassword('')
    setSourceChTableName('')
    // Kafka источник
    setSourceKafkaBootstrapServers('')
    setSourceKafkaTopic('')
    setSourceKafkaGroupId('')
    // PostgreSQL приёмник
    setSinkDbHost('')
    setSinkDbPort('5432')
    setSinkDbDatabase('')
    setSinkDbUsername('')
    setSinkDbPassword('')
    setSinkDbTableName('')
    // ClickHouse приёмник
    setSinkChHost('')
    setSinkChPort('8123')
    setSinkChDatabase('')
    setSinkChUsername('')
    setSinkChPassword('')
    setSinkChTableName('')
    // Kafka приёмник
    setSinkKafkaBootstrapServers('')
    setSinkKafkaTopic('')
    setSinkKafkaKeyField('')
    setUseAirflow(false)
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
    // PostgreSQL источник
    sourceDbHost,
    sourceDbPort,
    sourceDbDatabase,
    sourceDbUsername,
    sourceDbPassword,
    sourceDbTableName,
    // ClickHouse источник
    sourceChHost,
    sourceChPort,
    sourceChDatabase,
    sourceChUsername,
    sourceChPassword,
    sourceChTableName,
    // Kafka источник
    sourceKafkaBootstrapServers,
    sourceKafkaTopic,
    sourceKafkaGroupId,
    // PostgreSQL приёмник
    sinkDbHost,
    sinkDbPort,
    sinkDbDatabase,
    sinkDbUsername,
    sinkDbPassword,
    sinkDbTableName,
    // ClickHouse приёмник
    sinkChHost,
    sinkChPort,
    sinkChDatabase,
    sinkChUsername,
    sinkChPassword,
    sinkChTableName,
    // Kafka приёмник
    sinkKafkaBootstrapServers,
    sinkKafkaTopic,
    sinkKafkaKeyField,
    useAirflow,
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
    // PostgreSQL источник
    setSourceDbHost,
    setSourceDbPort,
    setSourceDbDatabase,
    setSourceDbUsername,
    setSourceDbPassword,
    setSourceDbTableName,
    // ClickHouse источник
    setSourceChHost,
    setSourceChPort,
    setSourceChDatabase,
    setSourceChUsername,
    setSourceChPassword,
    setSourceChTableName,
    // Kafka источник
    setSourceKafkaBootstrapServers,
    setSourceKafkaTopic,
    setSourceKafkaGroupId,
    // PostgreSQL приёмник
    setSinkDbHost,
    setSinkDbPort,
    setSinkDbDatabase,
    setSinkDbUsername,
    setSinkDbPassword,
    setSinkDbTableName,
    // ClickHouse приёмник
    setSinkChHost,
    setSinkChPort,
    setSinkChDatabase,
    setSinkChUsername,
    setSinkChPassword,
    setSinkChTableName,
    // Kafka приёмник
    setSinkKafkaBootstrapServers,
    setSinkKafkaTopic,
    setSinkKafkaKeyField,
    setUseAirflow,
    handleTransfer,
    reset
  }
}
