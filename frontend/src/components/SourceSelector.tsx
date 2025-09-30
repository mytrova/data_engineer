import React from 'react'
import styled from 'styled-components'
import { Database, Settings } from 'lucide-react'
import { FileUpload } from './FileUpload'
import { DatabaseConnectionForm } from './DatabaseConnectionForm'
import { ClickHouseConnectionForm } from './ClickHouseConnectionForm'
import { KafkaConnectionForm } from './KafkaConnectionForm'

const SourceContainer = styled.div`
  background: #f7fafc;
  border-radius: 12px;
  padding: 1.5rem;
  border: 1px solid #e2e8f0;
`

const SourceHeader = styled.div`
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

interface SourceSelectorProps {
  sourceType: string
  delimiter: string
  file: File | null
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
  kafkaGroupId: string
  onSourceTypeChange: (type: string) => void
  onDelimiterChange: (delimiter: string) => void
  onFileSelect: (file: File | null) => void
  onFileNameChange: (fileName: string) => void
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
  onKafkaGroupIdChange: (groupId: string) => void
  disabled?: boolean
}

export const SourceSelector: React.FC<SourceSelectorProps> = ({
  sourceType,
  delimiter,
  file,
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
  kafkaGroupId,
  onSourceTypeChange,
  onDelimiterChange,
  onFileSelect,
  onFileNameChange,
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
  onKafkaGroupIdChange,
  disabled = false
}) => {
  const needsFile = ['csv', 'json', 'xml'].includes(sourceType.toLowerCase())
  const needsDelimiter = sourceType.toLowerCase() === 'csv'

  return (
    <SourceContainer>
      <SourceHeader>
        <Database size={20} />
        Источник данных
      </SourceHeader>
      
      <Label htmlFor="source-type">Тип источника</Label>
      <Select
        id="source-type"
        value={sourceType}
        onChange={(e) => onSourceTypeChange(e.target.value)}
        disabled={disabled}
      >
        <option value="csv">CSV файл</option>
        <option value="json">JSON файл</option>
        <option value="xml">XML файл</option>
        <option value="postgresql">PostgreSQL</option>
        <option value="clickhouse">ClickHouse</option>
        <option value="kafka">Kafka</option>
      </Select>

          {needsFile && (
            <FileUpload
              file={file}
              onFileSelect={onFileSelect}
              onFileNameChange={onFileNameChange}
              disabled={disabled}
            />
          )}

      {needsDelimiter && (
        <CSVSettings>
          <CSVSettingsHeader>
            <Settings size={16} />
            Настройки CSV
          </CSVSettingsHeader>
          
          <Label htmlFor="source-delimiter">Разделитель</Label>
          <Select
            id="source-delimiter"
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

      {sourceType === 'postgresql' && (
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
      )}

      {sourceType === 'clickhouse' && (
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
      )}

      {sourceType === 'kafka' && (
        <KafkaConnectionForm
          bootstrapServers={kafkaBootstrapServers}
          topic={kafkaTopic}
          groupId={kafkaGroupId}
          onBootstrapServersChange={onKafkaBootstrapServersChange}
          onTopicChange={onKafkaTopicChange}
          onGroupIdChange={onKafkaGroupIdChange}
          disabled={disabled}
        />
      )}
    </SourceContainer>
  )
}
