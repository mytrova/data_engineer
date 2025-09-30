import React from 'react'
import styled from 'styled-components'
import { Download, Settings } from 'lucide-react'
import { DatabaseConnectionForm } from './DatabaseConnectionForm'
import { ClickHouseConnectionForm } from './ClickHouseConnectionForm'
import { KafkaConnectionForm } from './KafkaConnectionForm'

const SinkContainer = styled.div`
  background: #f7fafc;
  border-radius: 12px;
  padding: 1.5rem;
  border: 1px solid #e2e8f0;
`

const SinkHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 1rem;
  color: #4a5568;
  font-weight: 600;
`

const Label = styled.label`
  display: block;
  margin-bottom: 0.5rem;
  color: #4a5568;
  font-weight: 500;
`

const Select = styled.select<{ disabled: boolean }>`
  width: 100%;
  padding: 0.75rem;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  background: white;
  color: #4a5568;
  font-size: 1rem;
  cursor: ${props => props.disabled ? 'not-allowed' : 'pointer'};
  opacity: ${props => props.disabled ? 0.6 : 1};
  margin-bottom: 1rem;

  &:focus {
    outline: none;
    border-color: #4299e1;
    box-shadow: 0 0 0 3px rgba(66, 153, 225, 0.1);
  }
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

const CSVSettings = styled.div`
  background: #edf2f7;
  border-radius: 8px;
  padding: 1rem;
  margin-top: 1rem;
  border: 1px solid #cbd5e0;
`

const CSVSettingsHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.75rem;
  color: #4a5568;
  font-weight: 500;
  font-size: 0.9rem;
`

const ProcessingSettings = styled.div`
  background: #e6fffa;
  border-radius: 8px;
  padding: 1rem;
  margin-top: 1rem;
  border: 1px solid #81e6d9;
`

const ProcessingSettingsHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.75rem;
  color: #234e52;
  font-weight: 500;
  font-size: 0.9rem;
`

const CheckboxContainer = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-top: 1rem;
  padding: 0.75rem;
  background: #f0f9ff;
  border: 1px solid #bae6fd;
  border-radius: 8px;
`

const Checkbox = styled.input`
  width: 16px;
  height: 16px;
  cursor: pointer;
`

const CheckboxLabel = styled.label`
  color: #0369a1;
  font-weight: 500;
  cursor: pointer;
  font-size: 0.9rem;
`

interface SinkSelectorProps {
  sinkType: string
  delimiter: string
  chunkSize: number
  dbHost: string
  dbPort: string
  dbDatabase: string
  dbUsername: string
  dbPassword: string
  dbTableName: string
  // ClickHouse
  chHost: string
  chPort: string
  chDatabase: string
  chUsername: string
  chPassword: string
  chTableName: string
  // Kafka
  kafkaBootstrapServers: string
  kafkaTopic: string
  kafkaKeyField: string
  useAirflow: boolean
  sourceType?: string  // Добавляем тип источника
  onSinkTypeChange: (type: string) => void
  onDelimiterChange: (delimiter: string) => void
  onChunkSizeChange: (size: number) => void
  onDbHostChange: (host: string) => void
  onDbPortChange: (port: string) => void
  onDbDatabaseChange: (database: string) => void
  onDbUsernameChange: (username: string) => void
  onDbPasswordChange: (password: string) => void
  onDbTableNameChange: (tableName: string) => void
  // ClickHouse
  onChHostChange: (host: string) => void
  onChPortChange: (port: string) => void
  onChDatabaseChange: (database: string) => void
  onChUsernameChange: (username: string) => void
  onChPasswordChange: (password: string) => void
  onChTableNameChange: (tableName: string) => void
  // Kafka
  onKafkaBootstrapServersChange: (servers: string) => void
  onKafkaTopicChange: (topic: string) => void
  onKafkaKeyFieldChange: (keyField: string) => void
  onUseAirflowChange: (use: boolean) => void
  disabled?: boolean
}

export const SinkSelector: React.FC<SinkSelectorProps> = ({
  sinkType,
  delimiter,
  chunkSize,
  dbHost,
  dbPort,
  dbDatabase,
  dbUsername,
  dbPassword,
  dbTableName,
  chHost,
  chPort,
  chDatabase,
  chUsername,
  chPassword,
  chTableName,
  kafkaBootstrapServers,
  kafkaTopic,
  kafkaKeyField,
  useAirflow,
  sourceType,
  onSinkTypeChange,
  onDelimiterChange,
  onChunkSizeChange,
  onDbHostChange,
  onDbPortChange,
  onDbDatabaseChange,
  onDbUsernameChange,
  onDbPasswordChange,
  onDbTableNameChange,
  onChHostChange,
  onChPortChange,
  onChDatabaseChange,
  onChUsernameChange,
  onChPasswordChange,
  onChTableNameChange,
  onKafkaBootstrapServersChange,
  onKafkaTopicChange,
  onKafkaKeyFieldChange,
  onUseAirflowChange,
  disabled = false
}) => {
  const needsDelimiter = sinkType.toLowerCase() === 'csv'
  const isPreview = sinkType.toLowerCase() === 'preview'
  
  // Показываем галочку Airflow только если источник и приёмник - базы данных
  const showAirflowOption = ['postgresql', 'clickhouse', 'kafka'].includes(sourceType || '') && 
                           ['postgresql', 'clickhouse', 'kafka'].includes(sinkType)

  return (
    <SinkContainer>
      <SinkHeader>
        <Download size={20} />
        Приёмник данных
      </SinkHeader>
      
      <Label htmlFor="sink-type">Тип приёмника</Label>
      <Select
        id="sink-type"
        value={sinkType}
        onChange={(e) => onSinkTypeChange(e.target.value)}
        disabled={disabled}
      >
        <option value="preview">Предпросмотр</option>
        <option value="csv">CSV файл</option>
        <option value="json">JSON файл</option>
        <option value="xml">XML файл</option>
        <option value="postgresql">PostgreSQL</option>
        <option value="clickhouse">ClickHouse</option>
        <option value="kafka">Kafka</option>
      </Select>

      {needsDelimiter && (
        <CSVSettings>
          <CSVSettingsHeader>
            <Settings size={16} />
            Настройки CSV
          </CSVSettingsHeader>
          
          <Label htmlFor="sink-delimiter">Разделитель</Label>
          <Select
            id="sink-delimiter"
            value={delimiter}
            onChange={(e) => onDelimiterChange(e.target.value)}
            disabled={disabled}
          >
            <option value=";">Точка с запятой (;)</option>
            <option value=",">Запятая (,)</option>
            <option value="\t">Табуляция (\t)</option>
            <option value="|">Вертикальная черта (|)</option>
            <option value=" ">Пробел ( )</option>
          </Select>
        </CSVSettings>
      )}

      {!isPreview && (
        <ProcessingSettings>
          <ProcessingSettingsHeader>
            <Settings size={16} />
            Настройки обработки
          </ProcessingSettingsHeader>
          
          <Label htmlFor="chunk-size">Размер чанка (строк)</Label>
          <Input
            id="chunk-size"
            type="number"
            min="1"
            value={chunkSize}
            onChange={(e) => onChunkSizeChange(parseInt(e.target.value) || 10)}
            disabled={disabled}
          />
        </ProcessingSettings>
      )}

      {sinkType === 'postgresql' && (
        <>
          <DatabaseConnectionForm
            host={dbHost}
            port={dbPort}
            database={dbDatabase}
            username={dbUsername}
            password={dbPassword}
            tableName={dbTableName}
            onHostChange={onDbHostChange}
            onPortChange={onDbPortChange}
            onDatabaseChange={onDbDatabaseChange}
            onUsernameChange={onDbUsernameChange}
            onPasswordChange={onDbPasswordChange}
            onTableNameChange={onDbTableNameChange}
            disabled={disabled}
          />
          
          {showAirflowOption && (
            <CheckboxContainer>
              <Checkbox
                type="checkbox"
                id="use-airflow"
                checked={useAirflow}
                onChange={(e) => onUseAirflowChange(e.target.checked)}
                disabled={disabled}
              />
              <CheckboxLabel htmlFor="use-airflow">
                Использовать Airflow для оркестрации переливки данных
              </CheckboxLabel>
            </CheckboxContainer>
          )}
        </>
      )}

      {sinkType === 'clickhouse' && (
        <>
          <ClickHouseConnectionForm
            host={chHost}
            port={chPort}
            database={chDatabase}
            username={chUsername}
            password={chPassword}
            tableName={chTableName}
            onHostChange={onChHostChange}
            onPortChange={onChPortChange}
            onDatabaseChange={onChDatabaseChange}
            onUsernameChange={onChUsernameChange}
            onPasswordChange={onChPasswordChange}
            onTableNameChange={onChTableNameChange}
            disabled={disabled}
          />
          
          {showAirflowOption && (
            <CheckboxContainer>
              <Checkbox
                type="checkbox"
                id="use-airflow"
                checked={useAirflow}
                onChange={(e) => onUseAirflowChange(e.target.checked)}
                disabled={disabled}
              />
              <CheckboxLabel htmlFor="use-airflow">
                Использовать Airflow для оркестрации переливки данных
              </CheckboxLabel>
            </CheckboxContainer>
          )}
        </>
      )}

      {sinkType === 'kafka' && (
        <>
          <KafkaConnectionForm
            bootstrapServers={kafkaBootstrapServers}
            topic={kafkaTopic}
            keyField={kafkaKeyField}
            onBootstrapServersChange={onKafkaBootstrapServersChange}
            onTopicChange={onKafkaTopicChange}
            onKeyFieldChange={onKafkaKeyFieldChange}
            disabled={disabled}
          />
          
          {showAirflowOption && (
            <CheckboxContainer>
              <Checkbox
                type="checkbox"
                id="use-airflow"
                checked={useAirflow}
                onChange={(e) => onUseAirflowChange(e.target.checked)}
                disabled={disabled}
              />
              <CheckboxLabel htmlFor="use-airflow">
                Использовать Airflow для оркестрации переливки данных
              </CheckboxLabel>
            </CheckboxContainer>
          )}
        </>
      )}
    </SinkContainer>
  )
}
