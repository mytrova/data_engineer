import React, { useState } from 'react'
import styled from 'styled-components'
import { Database, TestTube, Table, Eye } from 'lucide-react'
import { getApiUrl } from '../utils/api'

const DatabaseContainer = styled.div`
  background: #edf2f7;
  border-radius: 8px;
  padding: 1rem;
  margin-top: 1rem;
  border: 1px solid #cbd5e0;
`

const DatabaseHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.75rem;
  color: #4a5568;
  font-weight: 500;
  font-size: 0.9rem;
`

const Label = styled.label`
  display: block;
  margin-bottom: 0.5rem;
  color: #4a5568;
  font-weight: 500;
`

const Input = styled.input<{ disabled: boolean }>`
  width: 100%;
  padding: 0.75rem;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  background: white;
  color: #4a5568;
  font-size: 1rem;
  cursor: ${props => props.disabled ? 'not-allowed' : 'text'};
  opacity: ${props => props.disabled ? 0.6 : 1};
  margin-bottom: 1rem;

  &:focus {
    outline: none;
    border-color: #4299e1;
    box-shadow: 0 0 0 3px rgba(66, 153, 225, 0.1);
  }
`

const TextArea = styled.textarea<{ disabled: boolean }>`
  width: 100%;
  padding: 0.75rem;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  background: white;
  color: #4a5568;
  font-size: 1rem;
  cursor: ${props => props.disabled ? 'not-allowed' : 'text'};
  opacity: ${props => props.disabled ? 0.6 : 1};
  margin-bottom: 1rem;
  min-height: 80px;
  resize: vertical;

  &:focus {
    outline: none;
    border-color: #4299e1;
    box-shadow: 0 0 0 3px rgba(66, 153, 225, 0.1);
  }
`

const Button = styled.button<{ disabled: boolean; variant?: 'primary' | 'secondary' }>`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 1rem;
  border: none;
  border-radius: 8px;
  font-weight: 500;
  cursor: ${props => props.disabled ? 'not-allowed' : 'pointer'};
  transition: all 0.2s ease;
  opacity: ${props => props.disabled ? 0.6 : 1};
  margin-right: 0.5rem;
  margin-bottom: 0.5rem;
  
  ${props => props.variant === 'primary' ? `
    background: #4299e1;
    color: white;
    
    &:hover:not(:disabled) {
      background: #3182ce;
    }
  ` : `
    background: #e2e8f0;
    color: #4a5568;
    
    &:hover:not(:disabled) {
      background: #cbd5e0;
    }
  `}
`

const StatusMessage = styled.div<{ type: 'success' | 'error' | 'info' }>`
  padding: 0.75rem;
  border-radius: 8px;
  margin-top: 1rem;
  font-size: 0.9rem;
  
  ${props => props.type === 'success' ? `
    background: #f0fff4;
    border: 1px solid #9ae6b4;
    color: #22543d;
  ` : props.type === 'error' ? `
    background: #fed7d7;
    border: 1px solid #feb2b2;
    color: #c53030;
  ` : `
    background: #ebf8ff;
    border: 1px solid #90cdf4;
    color: #2c5282;
  `}
`

const TablesList = styled.div`
  margin-top: 1rem;
  max-height: 200px;
  overflow-y: auto;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  background: white;
`

const TableItem = styled.div`
  padding: 0.5rem 0.75rem;
  border-bottom: 1px solid #e2e8f0;
  cursor: pointer;
  transition: background-color 0.2s;
  
  &:hover {
    background: #f7fafc;
  }
  
  &:last-child {
    border-bottom: none;
  }
`

interface DatabaseConnectionFormProps {
  host: string
  port: string
  database: string
  username: string
  password: string
  tableName: string
  onHostChange: (host: string) => void
  onPortChange: (port: string) => void
  onDatabaseChange: (database: string) => void
  onUsernameChange: (username: string) => void
  onPasswordChange: (password: string) => void
  onTableNameChange: (tableName: string) => void
  disabled?: boolean
}

export const DatabaseConnectionForm: React.FC<DatabaseConnectionFormProps> = ({
  host,
  port,
  database,
  username,
  password,
  tableName,
  onHostChange,
  onPortChange,
  onDatabaseChange,
  onUsernameChange,
  onPasswordChange,
  onTableNameChange,
  disabled = false
}) => {
  const [isTesting, setIsTesting] = useState(false)
  const [testResult, setTestResult] = useState<{ type: 'success' | 'error' | 'info', message: string } | null>(null)
  const [tables, setTables] = useState<string[]>([])
  const [selectedTable, setSelectedTable] = useState<string>('')
  const [query, setQuery] = useState<string>('')

  // Генерируем строку подключения из отдельных полей
  const connectionString = `postgresql://${username}:${password}@${host}:${port}/${database}`

  const testConnection = async () => {
    if (!host || !port || !database || !username || !password) {
      setTestResult({ type: 'error', message: 'Заполните все обязательные поля' })
      return
    }

    setIsTesting(true)
    setTestResult(null)

    try {
      const formData = new FormData()
      formData.append('connection_string', connectionString)
      formData.append('database_type', 'postgresql')

      const response = await fetch(getApiUrl('/database/connect'), {
        method: 'POST',
        body: formData,
      })

      const result = await response.json()

      if (result.status === 'success' && result.connected) {
        setTestResult({ type: 'success', message: 'Подключение успешно!' })
        setTables(result.tables || [])
      } else {
        const errorMessage = result.error || 'Ошибка подключения'
        setTestResult({ type: 'error', message: typeof errorMessage === 'string' ? errorMessage : JSON.stringify(errorMessage) })
        setTables([])
      }
    } catch (error) {
      setTestResult({ type: 'error', message: 'Ошибка сети' })
      setTables([])
    } finally {
      setIsTesting(false)
    }
  }

  const getTableSchema = async (tableName: string) => {
    try {
      const formData = new FormData()
      formData.append('connection_string', connectionString)
      formData.append('table_name', tableName)
      formData.append('database_type', 'postgresql')

      const response = await fetch(getApiUrl('/database/schema'), {
        method: 'POST',
        body: formData,
      })

      const result = await response.json()
      
      if (result.status === 'success') {
        const schema = result.schema.map((col: any) => `${col.column_name} (${col.data_type})`).join(', ')
        setTestResult({ type: 'info', message: `Схема таблицы ${tableName}: ${schema}` })
      }
    } catch (error) {
      setTestResult({ type: 'error', message: 'Ошибка получения схемы' })
    }
  }

  return (
    <DatabaseContainer>
      <DatabaseHeader>
        <Database size={16} />
        Подключение к PostgreSQL
      </DatabaseHeader>
      
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
        <div>
          <Label htmlFor="db-host">Хост</Label>
          <Input
            id="db-host"
            type="text"
            placeholder="localhost"
            value={host}
            onChange={(e) => onHostChange(e.target.value)}
            disabled={disabled}
          />
        </div>
        <div>
          <Label htmlFor="db-port">Порт</Label>
          <Input
            id="db-port"
            type="text"
            placeholder="5432"
            value={port}
            onChange={(e) => onPortChange(e.target.value)}
            disabled={disabled}
          />
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
        <div>
          <Label htmlFor="db-username">Пользователь</Label>
          <Input
            id="db-username"
            type="text"
            placeholder="postgres"
            value={username}
            onChange={(e) => onUsernameChange(e.target.value)}
            disabled={disabled}
          />
        </div>
        <div>
          <Label htmlFor="db-password">Пароль</Label>
          <Input
            id="db-password"
            type="password"
            placeholder="password"
            value={password}
            onChange={(e) => onPasswordChange(e.target.value)}
            disabled={disabled}
          />
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
        <div>
          <Label htmlFor="db-database">База данных</Label>
          <Input
            id="db-database"
            type="text"
            placeholder="mydatabase"
            value={database}
            onChange={(e) => onDatabaseChange(e.target.value)}
            disabled={disabled}
          />
        </div>
        <div>
          <Label htmlFor="db-table">Таблица</Label>
          <Input
            id="db-table"
            type="text"
            placeholder="users"
            value={tableName}
            onChange={(e) => onTableNameChange(e.target.value)}
            disabled={disabled}
          />
        </div>
      </div>

      <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
        <Button
          variant="primary"
          onClick={testConnection}
          disabled={disabled || isTesting}
        >
          <TestTube size={16} />
          {isTesting ? 'Тестирование...' : 'Тестировать'}
        </Button>

        {tables.length > 0 && (
          <Button
            variant="secondary"
            onClick={() => setTestResult({ type: 'info', message: `Найдено таблиц: ${tables.length}` })}
            disabled={disabled}
          >
            <Table size={16} />
            Таблицы ({tables.length})
          </Button>
        )}
      </div>

      {testResult && (
        <StatusMessage type={testResult.type}>
          {testResult.message}
        </StatusMessage>
      )}

      {tables.length > 0 && (
        <TablesList>
          {tables.map((table) => (
            <TableItem
              key={table}
              onClick={() => {
                setSelectedTable(table)
                getTableSchema(table)
              }}
              style={{
                background: selectedTable === table ? '#e6fffa' : 'transparent'
              }}
            >
              <Eye size={14} style={{ marginRight: '0.5rem' }} />
              {table}
            </TableItem>
          ))}
        </TablesList>
      )}

      {selectedTable && (
        <div style={{ marginTop: '1rem' }}>
          <Label htmlFor="custom-query">SQL запрос (опционально)</Label>
          <TextArea
            id="custom-query"
            placeholder={`SELECT * FROM ${selectedTable} LIMIT 100`}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            disabled={disabled}
          />
        </div>
      )}
    </DatabaseContainer>
  )
}
