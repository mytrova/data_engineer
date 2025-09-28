import React, { useState, useEffect } from 'react'
import styled from 'styled-components'
import { 
  FileText, 
  CheckCircle, 
  XCircle, 
  AlertCircle,
  Activity,
  RefreshCw,
  Pause,
  Play,
  Trash2
} from 'lucide-react'

interface ProcessingProcess {
  process_id: string
  status: string
  started_at: string
  completed_at?: string
  paused_at?: string
  resumed_at?: string
  stopped_at?: string
  rows_processed: number
  current_chunk: number
  total_chunks: number
  sink_config?: any
  error?: string
}


const ManagerContainer = styled.div`
  background: rgba(255, 255, 255, 0.1);
  backdrop-filter: blur(10px);
  border-radius: 16px;
  padding: 2rem;
  margin-bottom: 2rem;
  border: 1px solid rgba(255, 255, 255, 0.2);
`

const Header = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 2rem;
`

const Title = styled.h2`
  color: white;
  margin: 0;
  display: flex;
  align-items: center;
  gap: 0.5rem;
`

const RefreshButton = styled.button`
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  border: none;
  color: white;
  padding: 0.5rem 1rem;
  border-radius: 8px;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 0.5rem;
  transition: all 0.3s ease;

  &:hover {
    transform: translateY(-2px);
    box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
  }
`

const FileList = styled.div`
  display: grid;
  gap: 1rem;
`

const FileCard = styled.div`
  background: rgba(255, 255, 255, 0.05);
  border-radius: 12px;
  padding: 1.5rem;
  border: 1px solid rgba(255, 255, 255, 0.1);
  transition: all 0.3s ease;

  &:hover {
    background: rgba(255, 255, 255, 0.1);
    transform: translateY(-2px);
  }
`

const FileHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1rem;
`

const FileName = styled.h3`
  color: white;
  margin: 0;
  display: flex;
  align-items: center;
  gap: 0.5rem;
`


const StatusBadge = styled.span<{ status: string }>`
  padding: 0.25rem 0.75rem;
  border-radius: 20px;
  font-size: 0.8rem;
  font-weight: 500;
  display: flex;
  align-items: center;
  gap: 0.25rem;
  
  ${props => {
    switch (props.status) {
      case 'uploaded':
        return 'background: rgba(59, 130, 246, 0.2); color: #60a5fa; border: 1px solid rgba(59, 130, 246, 0.3);'
      case 'processing':
        return 'background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.3);'
      case 'completed':
        return 'background: rgba(34, 197, 94, 0.2); color: #4ade80; border: 1px solid rgba(34, 197, 94, 0.3);'
      case 'error':
        return 'background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.3);'
      default:
        return 'background: rgba(107, 114, 128, 0.2); color: #9ca3af; border: 1px solid rgba(107, 114, 128, 0.3);'
    }
  }}
`

const FileInfo = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
  margin-bottom: 1rem;
`

const InfoItem = styled.div`
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
`

const InfoLabel = styled.span`
  color: #a0a0a0;
  font-size: 0.8rem;
  text-transform: uppercase;
  letter-spacing: 0.5px;
`

const InfoValue = styled.span`
  color: white;
  font-weight: 500;
`

const ProgressBar = styled.div`
  width: 100%;
  height: 8px;
  background: rgba(255, 255, 255, 0.1);
  border-radius: 4px;
  overflow: hidden;
  margin: 0.5rem 0;
`

const ProgressFill = styled.div<{ progress: number }>`
  height: 100%;
  background: linear-gradient(90deg, #667eea, #764ba2);
  width: ${props => props.progress}%;
  transition: width 0.3s ease;
`


const LargeFileManager: React.FC = () => {
  const [processes, setProcesses] = useState<ProcessingProcess[]>([])
  const [loading, setLoading] = useState(true)

  const fetchProcesses = async () => {
    try {
      // Получаем базовый URL для API
      const getApiBase = () => {
        const hostname = window.location.hostname
        if (hostname === 'localhost' || hostname === '127.0.0.1') {
          return 'http://localhost:8000'
        }
        return window.location.origin.replace(':3000', ':8000')
      }
      
      const response = await fetch(`${getApiBase()}/large-file/status`)
      const data = await response.json()
      if (data.status === 'success') {
        // Преобразуем объект процессов в массив
        const processesArray = Object.entries(data.processes).map(([processId, processData]: [string, any]) => ({
          process_id: processId,
          ...processData
        }))
        setProcesses(processesArray)
      }
    } catch (error) {
      console.error('Ошибка при загрузке процессов:', error)
    } finally {
      setLoading(false)
    }
  }

  const pauseProcess = async (processId: string) => {
    try {
      const getApiBase = () => {
        const hostname = window.location.hostname
        if (hostname === 'localhost' || hostname === '127.0.0.1') {
          return 'http://localhost:8000'
        }
        return window.location.origin.replace(':3000', ':8000')
      }
      
      const response = await fetch(`${getApiBase()}/large-file/pause/${processId}`, {
        method: 'POST'
      })
      const data = await response.json()
      if (data.status === 'success') {
        await fetchProcesses() // Обновляем список
      } else {
        console.error('Ошибка при приостановке процесса:', data.error)
      }
    } catch (error) {
      console.error('Ошибка при приостановке процесса:', error)
    }
  }

  const resumeProcess = async (processId: string) => {
    try {
      const getApiBase = () => {
        const hostname = window.location.hostname
        if (hostname === 'localhost' || hostname === '127.0.0.1') {
          return 'http://localhost:8000'
        }
        return window.location.origin.replace(':3000', ':8000')
      }
      
      const response = await fetch(`${getApiBase()}/large-file/resume/${processId}`, {
        method: 'POST'
      })
      const data = await response.json()
      if (data.status === 'success') {
        await fetchProcesses() // Обновляем список
      } else {
        console.error('Ошибка при возобновлении процесса:', data.error)
      }
    } catch (error) {
      console.error('Ошибка при возобновлении процесса:', error)
    }
  }

  const deleteProcess = async (processId: string) => {
    if (!confirm('Вы уверены, что хотите удалить этот процесс? Данные в базе данных будут сохранены.')) {
      return
    }
    
    try {
      const getApiBase = () => {
        const hostname = window.location.hostname
        if (hostname === 'localhost' || hostname === '127.0.0.1') {
          return 'http://localhost:8000'
        }
        return window.location.origin.replace(':3000', ':8000')
      }
      
      const response = await fetch(`${getApiBase()}/large-file/delete/${processId}`, {
        method: 'DELETE'
      })
      const data = await response.json()
      if (data.status === 'success') {
        await fetchProcesses() // Обновляем список
      } else {
        console.error('Ошибка при удалении процесса:', data.error)
      }
    } catch (error) {
      console.error('Ошибка при удалении процесса:', error)
    }
  }

  const formatDate = (dateString: string): string => {
    return new Date(dateString).toLocaleString('ru-RU')
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'processing':
        return <Activity size={16} />
      case 'paused':
        return <Pause size={16} />
      case 'completed':
        return <CheckCircle size={16} />
      case 'error':
        return <XCircle size={16} />
      default:
        return <AlertCircle size={16} />
    }
  }

  useEffect(() => {
    fetchProcesses()
    
    // Автоматическое обновление каждые 5 секунд
    const interval = setInterval(fetchProcesses, 5000)
    
    return () => clearInterval(interval)
  }, [])

  if (loading) {
    return (
      <ManagerContainer>
        <div style={{ textAlign: 'center', color: 'white' }}>
          Загрузка процессов...
        </div>
      </ManagerContainer>
    )
  }

  return (
    <ManagerContainer>
      <Header>
        <Title>
          <FileText size={24} />
          Управление большими файлами
        </Title>
        <RefreshButton onClick={fetchProcesses}>
          <RefreshCw size={16} />
          Обновить
        </RefreshButton>
      </Header>

      <FileList>
        {processes.map((process) => {
          // Прогресс обработки
          let progress = 0
          if (process.status === 'completed') {
            progress = 100
          } else if (process.status === 'processing' && process.rows_processed > 0) {
            // Показываем прогресс, но не более 95% пока не завершено
            progress = Math.min((process.rows_processed / 10000) * 100, 95)
          } else if (process.status === 'processing') {
            // Если обработка началась, но строк еще нет, показываем 5%
            progress = 5
          }

          return (
            <FileCard key={process.process_id}>
              <FileHeader>
                <FileName>
                  {getStatusIcon(process.status)}
                  {process.process_id}
                </FileName>
                <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                  <StatusBadge status={process.status}>
                    {process.status === 'processing' && 'Обрабатывается'}
                    {process.status === 'paused' && 'Приостановлен'}
                    {process.status === 'completed' && 'Завершен'}
                    {process.status === 'error' && 'Ошибка'}
                  </StatusBadge>
                </div>
              </FileHeader>

              <FileInfo>
                <InfoItem>
                  <InfoLabel>ID процесса</InfoLabel>
                  <InfoValue>{process.process_id}</InfoValue>
                </InfoItem>
                <InfoItem>
                  <InfoLabel>Начало</InfoLabel>
                  <InfoValue>{formatDate(process.started_at)}</InfoValue>
                </InfoItem>
                <InfoItem>
                  <InfoLabel>Обработано строк</InfoLabel>
                  <InfoValue>{process.rows_processed.toLocaleString()}</InfoValue>
                </InfoItem>
                {process.completed_at && (
                  <InfoItem>
                    <InfoLabel>Завершен</InfoLabel>
                    <InfoValue>{formatDate(process.completed_at)}</InfoValue>
                  </InfoItem>
                )}
              </FileInfo>

              {process.status === 'processing' && (
                <div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.5rem' }}>
                    <span style={{ color: 'white', fontSize: '0.9rem' }}>Прогресс обработки</span>
                    <span style={{ color: '#a0a0a0', fontSize: '0.9rem' }}>
                      {process.rows_processed > 0 ? `${Math.round(progress)}% (${process.rows_processed.toLocaleString()} строк)` : 'Начало обработки...'}
                    </span>
                  </div>
                  <ProgressBar>
                    <ProgressFill progress={progress} />
                  </ProgressBar>
                </div>
              )}

              {process.error && (
                <div style={{ 
                  background: 'rgba(239, 68, 68, 0.1)', 
                  border: '1px solid rgba(239, 68, 68, 0.3)',
                  borderRadius: '8px',
                  padding: '1rem',
                  marginTop: '1rem'
                }}>
                  <div style={{ color: '#f87171', fontWeight: '500', marginBottom: '0.5rem' }}>
                    Ошибка обработки:
                  </div>
                  <div style={{ color: 'white', fontSize: '0.9rem' }}>
                    {process.error}
                  </div>
                </div>
              )}

              {process.sink_config && (
                <div style={{ 
                  background: 'rgba(59, 130, 246, 0.1)', 
                  border: '1px solid rgba(59, 130, 246, 0.3)',
                  borderRadius: '8px',
                  padding: '1rem',
                  marginTop: '1rem'
                }}>
                  <div style={{ color: '#60a5fa', fontWeight: '500', marginBottom: '0.5rem' }}>
                    База данных назначения:
                  </div>
                  <div style={{ color: 'white', fontSize: '0.9rem' }}>
                    {process.sink_config.host}:{process.sink_config.port}/{process.sink_config.database}.{process.sink_config.table_name}
                  </div>
                </div>
              )}

              {/* Кнопки управления */}
              <div style={{ 
                display: 'flex', 
                gap: '0.5rem', 
                marginTop: '1rem',
                justifyContent: 'flex-end'
              }}>
                {process.status === 'processing' && (
                  <button
                    onClick={() => pauseProcess(process.process_id)}
                    style={{
                      background: 'linear-gradient(135deg, #f59e0b 0%, #d97706 100%)',
                      border: 'none',
                      borderRadius: '8px',
                      padding: '0.5rem 1rem',
                      color: 'white',
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem',
                      fontSize: '0.9rem',
                      fontWeight: '500'
                    }}
                  >
                    <Pause size={16} />
                    Приостановить
                  </button>
                )}
                
                {process.status === 'paused' && (
                  <button
                    onClick={() => resumeProcess(process.process_id)}
                    style={{
                      background: 'linear-gradient(135deg, #10b981 0%, #059669 100%)',
                      border: 'none',
                      borderRadius: '8px',
                      padding: '0.5rem 1rem',
                      color: 'white',
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem',
                      fontSize: '0.9rem',
                      fontWeight: '500'
                    }}
                  >
                    <Play size={16} />
                    Возобновить
                  </button>
                )}
                
                {(process.status === 'completed' || process.status === 'error' || process.status === 'paused') && (
                  <button
                    onClick={() => deleteProcess(process.process_id)}
                    style={{
                      background: 'linear-gradient(135deg, #ef4444 0%, #dc2626 100%)',
                      border: 'none',
                      borderRadius: '8px',
                      padding: '0.5rem 1rem',
                      color: 'white',
                      cursor: 'pointer',
                      display: 'flex',
                      alignItems: 'center',
                      gap: '0.5rem',
                      fontSize: '0.9rem',
                      fontWeight: '500'
                    }}
                  >
                    <Trash2 size={16} />
                    Удалить
                  </button>
                )}
              </div>
            </FileCard>
          )
        })}
      </FileList>

      {processes.length === 0 && (
        <div style={{ 
          textAlign: 'center', 
          color: '#a0a0a0', 
          padding: '3rem',
          fontSize: '1.1rem'
        }}>
          Процессов обработки больших файлов пока нет
        </div>
      )}
    </ManagerContainer>
  )
}

export default LargeFileManager

