import React, { useState, useEffect } from 'react'
import styled from 'styled-components'
import { 
  ExternalLink, 
  RefreshCw, 
  Clock, 
  CheckCircle, 
  XCircle, 
  AlertCircle,
  Activity,
  Trash2
} from 'lucide-react'

interface DAG {
  dag_id: string
  description: string
  is_paused: boolean
  is_active: boolean
  has_task_concurrency_limits: boolean
  has_import_errors: boolean
  next_dagrun: string | null
  next_dagrun_data_interval_start: string | null
  next_dagrun_data_interval_end: string | null
}

interface DAGRun {
  dag_run_id: string
  dag_id: string
  execution_date: string
  start_date: string | null
  end_date: string | null
  state: string
  run_type: string
  conf: any
}

const DashboardContainer = styled.div`
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
  background: rgba(255, 255, 255, 0.1);
  border: 1px solid rgba(255, 255, 255, 0.2);
  color: white;
  padding: 0.5rem 1rem;
  border-radius: 8px;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 0.5rem;
  transition: all 0.2s ease;

  &:hover {
    background: rgba(255, 255, 255, 0.2);
  }

  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
`

const DAGList = styled.div`
  display: flex;
  flex-direction: column;
  gap: 1rem;
`

const DAGCard = styled.div`
  background: rgba(255, 255, 255, 0.05);
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 12px;
  padding: 1.5rem;
  transition: all 0.2s ease;

  &:hover {
    background: rgba(255, 255, 255, 0.08);
    border-color: rgba(255, 255, 255, 0.2);
  }
`

const DAGHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 1rem;
`

const DAGInfo = styled.div`
  flex: 1;
`

const DAGName = styled.h3`
  color: white;
  margin: 0 0 0.5rem 0;
  font-size: 1.1rem;
`

const DAGDescription = styled.p`
  color: rgba(255, 255, 255, 0.7);
  margin: 0;
  font-size: 0.9rem;
`

const DAGActions = styled.div`
  display: flex;
  gap: 0.5rem;
`

const ActionButton = styled.button<{ variant?: 'primary' | 'secondary' | 'danger' }>`
  background: ${props => {
    switch (props.variant) {
      case 'primary': return 'rgba(34, 197, 94, 0.2)'
      case 'danger': return 'rgba(239, 68, 68, 0.2)'
      default: return 'rgba(255, 255, 255, 0.1)'
    }
  }};
  border: 1px solid ${props => {
    switch (props.variant) {
      case 'primary': return 'rgba(34, 197, 94, 0.3)'
      case 'danger': return 'rgba(239, 68, 68, 0.3)'
      default: return 'rgba(255, 255, 255, 0.2)'
    }
  }};
  color: white;
  padding: 0.5rem;
  border-radius: 8px;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 0.25rem;
  transition: all 0.2s ease;
  font-size: 0.8rem;

  &:hover {
    background: ${props => {
      switch (props.variant) {
        case 'primary': return 'rgba(34, 197, 94, 0.3)'
        case 'danger': return 'rgba(239, 68, 68, 0.3)'
        default: return 'rgba(255, 255, 255, 0.2)'
      }
    }};
  }

  &:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
`

const StatusBadge = styled.span<{ status: string }>`
  display: inline-flex;
  align-items: center;
  gap: 0.25rem;
  padding: 0.25rem 0.5rem;
  border-radius: 6px;
  font-size: 0.75rem;
  font-weight: 500;
  margin-top: 0.5rem;
  background: ${props => {
    switch (props.status) {
      case 'active': return 'rgba(34, 197, 94, 0.2)'
      case 'paused': return 'rgba(156, 163, 175, 0.2)'
      case 'running': return 'rgba(59, 130, 246, 0.2)'
      case 'success': return 'rgba(34, 197, 94, 0.2)'
      case 'failed': return 'rgba(239, 68, 68, 0.2)'
      default: return 'rgba(156, 163, 175, 0.2)'
    }
  }};
  color: ${props => {
    switch (props.status) {
      case 'active': return '#22c55e'
      case 'paused': return '#9ca3af'
      case 'running': return '#3b82f6'
      case 'success': return '#22c55e'
      case 'failed': return '#ef4444'
      default: return '#9ca3af'
    }
  }};
  border: 1px solid ${props => {
    switch (props.status) {
      case 'active': return 'rgba(34, 197, 94, 0.3)'
      case 'paused': return 'rgba(156, 163, 175, 0.3)'
      case 'running': return 'rgba(59, 130, 246, 0.3)'
      case 'success': return 'rgba(34, 197, 94, 0.3)'
      case 'failed': return 'rgba(239, 68, 68, 0.3)'
      default: return 'rgba(156, 163, 175, 0.3)'
    }
  }};
`

const DAGRuns = styled.div`
  margin-top: 1rem;
`

const RunsHeader = styled.h4`
  color: white;
  margin: 0 0 0.5rem 0;
  font-size: 0.9rem;
  font-weight: 600;
`

const RunItem = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.5rem;
  background: rgba(255, 255, 255, 0.05);
  border-radius: 6px;
  margin-bottom: 0.25rem;
`

const RunInfo = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
`

const RunDate = styled.span`
  color: rgba(255, 255, 255, 0.7);
  font-size: 0.8rem;
`

const LoadingMessage = styled.div`
  color: rgba(255, 255, 255, 0.7);
  text-align: center;
  padding: 2rem;
`

const ErrorMessage = styled.div`
  color: #ef4444;
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid rgba(239, 68, 68, 0.2);
  border-radius: 8px;
  padding: 1rem;
  margin-bottom: 1rem;
`

export const AirflowDashboard: React.FC = () => {
  const [dags, setDAGs] = useState<DAG[]>([])
  const [dagRuns, setDAGRuns] = useState<Record<string, DAGRun[]>>({})
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [creatingDAGs, setCreatingDAGs] = useState<Record<string, any>>({})
  const [lastFetchTime, setLastFetchTime] = useState<number>(0)

  const fetchDAGs = async (forceRefresh = false) => {
    const now = Date.now()
    const cacheTimeout = 30000 // 30 секунд кэш
    
    // Если данные не устарели и не принудительное обновление, используем кэш
    if (!forceRefresh && now - lastFetchTime < cacheTimeout && dags.length > 0) {
      setLoading(false)
      return
    }
    
    try {
      setLoading(true)
      setError(null)
      const response = await fetch('/api/airflow/dags')
      const data = await response.json()
      
      if (data.status === 'success') {
        setDAGs(data.dags)
        setLastFetchTime(now)
        
        // Очищаем localStorage от несуществующих DAG'ов
        const existingDAGs = JSON.parse(localStorage.getItem('creating_dags') || '{}')
        const existingDAGIds = new Set(data.dags.map((dag: DAG) => dag.dag_id))
        
        const cleanedDAGs: Record<string, any> = {}
        Object.keys(existingDAGs).forEach(dagId => {
          if (existingDAGIds.has(dagId)) {
            cleanedDAGs[dagId] = existingDAGs[dagId]
          }
        })
        
        localStorage.setItem('creating_dags', JSON.stringify(cleanedDAGs))
        
        // Загружаем запуски только для активных DAG'ов и только если они еще не загружены
        const runsPromises = data.dags
          .filter((dag: DAG) => !dagRuns[dag.dag_id]) // Только если runs еще не загружены
          .slice(0, 3) // Ограничиваем до 3 DAG'ов одновременно для производительности
          .map(async (dag: DAG) => {
            try {
              const runsResponse = await fetch(`/api/airflow/dags/${dag.dag_id}/runs?limit=3`)
              const runsData = await runsResponse.json()
              return { dagId: dag.dag_id, runs: runsData.runs || [] }
            } catch (err) {
              console.warn(`Не удалось загрузить runs для DAG ${dag.dag_id}:`, err)
              return { dagId: dag.dag_id, runs: [] }
            }
          })
        
        if (runsPromises.length > 0) {
          const runsResults = await Promise.all(runsPromises)
          const newRunsMap: Record<string, DAGRun[]> = { ...dagRuns }
          runsResults.forEach(({ dagId, runs }) => {
            newRunsMap[dagId] = runs
          })
          setDAGRuns(newRunsMap)
        }
      } else {
        setError(data.error || 'Ошибка при загрузке DAG')
      }
    } catch (err) {
      setError('Ошибка подключения к серверу')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchDAGs()
    
    // Отслеживаем создание DAG'ов из localStorage
    const checkCreatingDAGs = () => {
      const stored = localStorage.getItem('creating_dags')
      if (stored) {
        try {
          const creatingDAGs = JSON.parse(stored)
          setCreatingDAGs(creatingDAGs)
        } catch (e) {
          console.warn('Ошибка при загрузке создающихся DAG\'ов:', e)
        }
      }
    }
    
    checkCreatingDAGs()
    
    // Проверяем каждые 10 секунд
    const interval = setInterval(checkCreatingDAGs, 10000)
    
    return () => clearInterval(interval)
  }, [])

  // Автоматически очищаем несуществующие DAG'и при загрузке
  useEffect(() => {
    if (dags.length > 0 && Object.keys(creatingDAGs).length > 0) {
      const existingDAGIds = new Set(dags.map((dag: DAG) => dag.dag_id))
      const hasNonExistentDAGs = Object.keys(creatingDAGs).some(dagId => !existingDAGIds.has(dagId))
      
      if (hasNonExistentDAGs) {
        console.log('Обнаружены несуществующие DAG\'и в localStorage, очищаем...')
        clearNonExistentDAGs()
      }
    }
  }, [dags, creatingDAGs])

  // Очищаем localStorage когда DAG появляется в обычном списке
  useEffect(() => {
    if (dags.length > 0 && Object.keys(creatingDAGs).length > 0) {
      const stored = localStorage.getItem('creating_dags')
      if (stored) {
        try {
          const creatingDAGsFromStorage = JSON.parse(stored)
          const updatedCreatingDAGs = { ...creatingDAGsFromStorage }
          let hasChanges = false
          
          // Удаляем DAG'и которые уже существуют в обычном списке
          dags.forEach(dag => {
            if (creatingDAGsFromStorage[dag.dag_id]) {
              delete updatedCreatingDAGs[dag.dag_id]
              hasChanges = true
            }
          })
          
          if (hasChanges) {
            localStorage.setItem('creating_dags', JSON.stringify(updatedCreatingDAGs))
            setCreatingDAGs(updatedCreatingDAGs)
          }
        } catch (e) {
          console.warn('Ошибка при очистке создающихся DAG\'ов:', e)
        }
      }
    }
  }, [dags, creatingDAGs])


  const openAirflowUI = async (dagId?: string) => {
    try {
      let url = ''
      if (dagId) {
        const response = await fetch(`/api/airflow/dags/${dagId}/ui-url`)
        const data = await response.json()
        url = data.ui_url
      } else {
        const response = await fetch('/api/airflow/ui-url')
        const data = await response.json()
        url = data.ui_url
      }
      
      window.open(url, '_blank')
    } catch (err) {
      setError('Ошибка при получении URL Airflow')
    }
  }

  const deleteDAG = async (dagId: string) => {
    if (!window.confirm(`Вы уверены, что хотите удалить DAG "${dagId}"?`)) {
      return
    }

    try {
      const response = await fetch(`/api/airflow/dags/${dagId}`, {
        method: 'DELETE'
      })
      const data = await response.json()
      
      if (data.status === 'success') {
        // Удаляем из localStorage если это создающийся DAG
        const existingDAGs = JSON.parse(localStorage.getItem('creating_dags') || '{}')
        if (existingDAGs[dagId]) {
          delete existingDAGs[dagId]
          localStorage.setItem('creating_dags', JSON.stringify(existingDAGs))
        }
        
        // Обновляем список DAG'ов после удаления
        await fetchDAGs(true)
      } else {
        setError(data.error || `Ошибка при удалении DAG ${dagId}`)
      }
    } catch (err) {
      setError(`Ошибка при удалении DAG ${dagId}`)
    }
  }

  const clearNonExistentDAGs = () => {
    // Очищаем localStorage от всех несуществующих DAG'ов
    const existingDAGs = JSON.parse(localStorage.getItem('creating_dags') || '{}')
    const existingDAGIds = new Set(dags.map((dag: DAG) => dag.dag_id))
    
    const cleanedDAGs: Record<string, any> = {}
    Object.keys(existingDAGs).forEach(dagId => {
      if (existingDAGIds.has(dagId)) {
        cleanedDAGs[dagId] = existingDAGs[dagId]
      }
    })
    
    localStorage.setItem('creating_dags', JSON.stringify(cleanedDAGs))
    
    // Обновляем список DAG'ов
    fetchDAGs(true)
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'success': return <CheckCircle size={14} />
      case 'failed': return <XCircle size={14} />
      case 'running': return <Activity size={14} />
      case 'paused': return <XCircle size={14} />
      default: return <Clock size={14} />
    }
  }

  const getDAGDisplayStatus = (dag: any) => {
    // Если есть статус последнего запуска, используем его
    if (dag.last_run_status) {
      return dag.last_run_status
    }
    // Иначе используем стандартную логику
    return dag.is_paused ? 'paused' : 'active'
  }

  const getDAGStatusIcon = (dag: any) => {
    const status = getDAGDisplayStatus(dag)
    switch (status) {
      case 'success': return <CheckCircle size={12} />
      case 'failed': return <XCircle size={12} />
      case 'running': return <Activity size={12} />
      case 'paused': return <XCircle size={12} />
      case 'active': return <CheckCircle size={12} />
      default: return <Clock size={12} />
    }
  }

  const getDAGStatusText = (dag: any) => {
    const status = getDAGDisplayStatus(dag)
    switch (status) {
      case 'success': return 'Выполнен'
      case 'failed': return 'Ошибка'
      case 'running': return 'Выполняется'
      case 'paused': return 'Приостановлен'
      case 'active': return 'Активен'
      default: return 'Неизвестно'
    }
  }

  if (loading) {
    return (
      <DashboardContainer>
        <LoadingMessage>
          <RefreshCw size={24} className="animate-spin" />
          <div>Загрузка DAG...</div>
        </LoadingMessage>
      </DashboardContainer>
    )
  }

  return (
    <DashboardContainer>
      <Header>
        <Title>
          <Activity size={24} />
          Airflow Dashboard
        </Title>
        <div style={{ display: 'flex', gap: '0.5rem' }}>
          <RefreshButton onClick={() => fetchDAGs(true)} disabled={loading}>
            <RefreshCw size={16} />
            Обновить
          </RefreshButton>
          <ActionButton onClick={() => openAirflowUI()}>
            <ExternalLink size={16} />
            Открыть Airflow UI
          </ActionButton>
        </div>
      </Header>

      {error && (
        <ErrorMessage>
          <AlertCircle size={16} />
          {error}
        </ErrorMessage>
      )}

      {/* Создающиеся DAG'и */}
      {Object.keys(creatingDAGs).length > 0 && (
        <div style={{ marginBottom: '2rem' }}>
          <h3 style={{ color: 'white', marginBottom: '1rem', fontSize: '1.1rem' }}>
            Создающиеся DAG'и
          </h3>
          {Object.entries(creatingDAGs)
            .filter(([dagId]) => !dags.some(dag => dag.dag_id === dagId))
            .map(([dagId, dagInfo]) => (
            <DAGCard key={dagId} style={{ border: '2px solid #3b82f6' }}>
              <DAGHeader>
                <DAGInfo>
                  <DAGName>{dagId}</DAGName>
                  <DAGDescription>{dagInfo.message || 'Создание DAG...'}</DAGDescription>
                  <StatusBadge status={dagInfo.status === 'creating' ? 'running' : dagInfo.status}>
                    <Activity size={12} />
                    {dagInfo.status === 'creating' ? 'Создается' : 
                     dagInfo.status === 'running' ? 'Выполняется' :
                     dagInfo.status === 'created' ? 'Создан' : 'Ошибка'}
                  </StatusBadge>
                </DAGInfo>
                <DAGActions>
                  <ActionButton onClick={() => openAirflowUI(dagId)}>
                    <ExternalLink size={14} />
                    Открыть в Airflow
                  </ActionButton>
                  <ActionButton 
                    onClick={() => deleteDAG(dagId)}
                    variant="danger"
                  >
                    <Trash2 size={14} />
                    Удалить
                  </ActionButton>
                </DAGActions>
              </DAGHeader>
            </DAGCard>
          ))}
        </div>
      )}

      <DAGList>
        {dags.map((dag) => (
          <DAGCard key={dag.dag_id}>
            <DAGHeader>
              <DAGInfo>
                <DAGName>{dag.dag_id}</DAGName>
                <DAGDescription>{dag.description || 'Описание отсутствует'}</DAGDescription>
                <StatusBadge status={getDAGDisplayStatus(dag)}>
                  {getDAGStatusIcon(dag)}
                  {getDAGStatusText(dag)}
                </StatusBadge>
              </DAGInfo>
              <DAGActions>
                <ActionButton onClick={() => openAirflowUI(dag.dag_id)}>
                  <ExternalLink size={14} />
                  Открыть в Airflow
                </ActionButton>
                <ActionButton 
                  onClick={() => deleteDAG(dag.dag_id)}
                  variant="danger"
                >
                  <Trash2 size={14} />
                  Удалить
                </ActionButton>
              </DAGActions>
            </DAGHeader>

            {dagRuns[dag.dag_id] && dagRuns[dag.dag_id].length > 0 && (
              <DAGRuns>
                <RunsHeader>Последние запуски:</RunsHeader>
                {dagRuns[dag.dag_id].map((run) => (
                  <RunItem key={run.dag_run_id}>
                    <RunInfo>
                      {getStatusIcon(run.state)}
                      <RunDate>
                        {new Date(run.execution_date).toLocaleString('ru-RU')}
                      </RunDate>
                    </RunInfo>
                    <StatusBadge status={run.state}>
                      {run.state}
                    </StatusBadge>
                  </RunItem>
                ))}
              </DAGRuns>
            )}
          </DAGCard>
        ))}
      </DAGList>

      {dags.length === 0 && !loading && (
        <LoadingMessage>
          DAG не найдены. Убедитесь, что Airflow запущен и DAG загружены.
        </LoadingMessage>
      )}
    </DashboardContainer>
  )
}
