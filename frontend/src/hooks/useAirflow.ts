import { useState, useCallback } from 'react'

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

interface UseAirflowReturn {
  dags: DAG[]
  dagRuns: Record<string, DAGRun[]>
  loading: boolean
  error: string | null
  fetchDAGs: () => Promise<void>
  getDAGRuns: (dagId: string, limit?: number) => Promise<DAGRun[]>
  getAirflowUIUrl: () => Promise<string>
  getDAGUIUrl: (dagId: string) => Promise<string>
  deleteDAG: (dagId: string) => Promise<boolean>
}

export const useAirflow = (): UseAirflowReturn => {
  const [dags, setDAGs] = useState<DAG[]>([])
  const [dagRuns, setDAGRuns] = useState<Record<string, DAGRun[]>>({})
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const fetchDAGs = useCallback(async () => {
    try {
      setLoading(true)
      setError(null)
      const response = await fetch('/api/airflow/dags')
      const data = await response.json()
      
      if (data.status === 'success') {
        setDAGs(data.dags)
        
        // Загружаем запуски для каждого DAG
        const runsPromises = data.dags.map(async (dag: DAG) => {
          const runsResponse = await fetch(`/api/airflow/dags/${dag.dag_id}/runs?limit=5`)
          const runsData = await runsResponse.json()
          return { dagId: dag.dag_id, runs: runsData.runs || [] }
        })
        
        const runsResults = await Promise.all(runsPromises)
        const runsMap: Record<string, DAGRun[]> = {}
        runsResults.forEach(({ dagId, runs }) => {
          runsMap[dagId] = runs
        })
        setDAGRuns(runsMap)
      } else {
        setError(data.error || 'Ошибка при загрузке DAG')
      }
    } catch (err) {
      setError('Ошибка подключения к серверу')
    } finally {
      setLoading(false)
    }
  }, [])


  const getDAGRuns = useCallback(async (dagId: string, limit: number = 10): Promise<DAGRun[]> => {
    try {
      const response = await fetch(`/api/airflow/dags/${dagId}/runs?limit=${limit}`)
      const data = await response.json()
      
      if (data.status === 'success') {
        return data.runs || []
      } else {
        setError(data.error || `Ошибка при получении запусков DAG ${dagId}`)
        return []
      }
    } catch (err) {
      setError('Ошибка при получении запусков DAG')
      return []
    }
  }, [])

  const getAirflowUIUrl = useCallback(async (): Promise<string> => {
    try {
      const response = await fetch('/api/airflow/ui-url')
      const data = await response.json()
      
      if (data.status === 'success') {
        return data.ui_url
      } else {
        throw new Error(data.error || 'Ошибка при получении URL Airflow')
      }
    } catch (err) {
      throw new Error('Ошибка при получении URL Airflow')
    }
  }, [])

  const getDAGUIUrl = useCallback(async (dagId: string): Promise<string> => {
    try {
      const response = await fetch(`/api/airflow/dags/${dagId}/ui-url`)
      const data = await response.json()
      
      if (data.status === 'success') {
        return data.ui_url
      } else {
        throw new Error(data.error || `Ошибка при получении URL DAG ${dagId}`)
      }
    } catch (err) {
      throw new Error(`Ошибка при получении URL DAG ${dagId}`)
    }
  }, [])

  const deleteDAG = useCallback(async (dagId: string): Promise<boolean> => {
    try {
      const response = await fetch(`/api/airflow/dags/${dagId}`, {
        method: 'DELETE'
      })
      const data = await response.json()
      
      if (data.status === 'success') {
        // Обновляем список DAG'ов после удаления
        await fetchDAGs()
        return true
      } else {
        setError(data.error || `Ошибка при удалении DAG ${dagId}`)
        return false
      }
    } catch (err) {
      setError(`Ошибка при удалении DAG ${dagId}`)
      return false
    }
  }, [fetchDAGs])

  return {
    dags,
    dagRuns,
    loading,
    error,
    fetchDAGs,
    getDAGRuns,
    getAirflowUIUrl,
    getDAGUIUrl,
    deleteDAG,
  }
}
