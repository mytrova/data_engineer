import React, { useState } from 'react'
import styled from 'styled-components'
import { SourceSelector } from './SourceSelector'
import { SinkSelector } from './SinkSelector'
import { DataPreview } from './DataPreview'
import { ProgressBar } from './ProgressBar'
import DataAnalysisModal from './DataAnalysisModal'
import { useDataTransfer } from '../hooks/useDataTransfer'

const FormContainer = styled.div`
  background: rgba(255, 255, 255, 0.95);
  backdrop-filter: blur(10px);
  border-radius: 20px;
  padding: 2rem;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.1);
  border: 1px solid rgba(255, 255, 255, 0.2);
`

const Title = styled.h1`
  font-size: 2rem;
  font-weight: 700;
  color: #2d3748;
  margin-bottom: 0.5rem;
  text-align: center;
`

const Subtitle = styled.p`
  color: #718096;
  text-align: center;
  margin-bottom: 2rem;
  font-size: 1.1rem;
`

const FormGrid = styled.div`
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 2rem;
  margin-bottom: 2rem;

  @media (max-width: 768px) {
    grid-template-columns: 1fr;
  }
`


const ErrorContainer = styled.div`
  margin-top: 1rem;
  padding: 1rem;
  background: #fed7d7;
  border-radius: 10px;
  border-left: 4px solid #e53e3e;
  color: #c53030;
`

const CheckboxContainer = styled.div`
  display: flex;
  align-items: center;
  gap: 8px;
  margin: 16px 0;
  padding: 12px 16px;
  background: #f8f9fa;
  border-radius: 8px;
  border: 1px solid #e1e5e9;
`

const Checkbox = styled.input`
  width: 18px;
  height: 18px;
  cursor: pointer;
  accent-color: #667eea;
`

const CheckboxLabel = styled.label`
  font-size: 14px;
  color: #333;
  cursor: pointer;
  user-select: none;
  font-weight: 500;
`


export const DataTransferForm: React.FC = () => {
  const [showAnalysisModal, setShowAnalysisModal] = useState(false)
  const [analysisData, setAnalysisData] = useState<any>(null)
  const [enableLLMAnalysis, setEnableLLMAnalysis] = useState(true)
  
  const handleTransferWithAnalysis = async () => {
    if (enableLLMAnalysis) {
      try {
        // Собираем данные о схеме источника и приёмника
        const sourceSchema = await getSourceSchema()
        const sinkSchema = await getSinkSchema()
        
        setAnalysisData({
          sourceSchema,
          sinkSchema,
          sourceType,
          sinkType
        })
        setShowAnalysisModal(true)
      } catch (error) {
        console.error('Ошибка при получении схемы данных:', error)
        // Если не удалось получить схему, запускаем перенос без анализа
        handleTransfer()
      }
    } else {
      // Если анализ отключен, запускаем перенос сразу без получения схемы
      handleTransfer()
    }
  }
  
  const handleConfirmTransfer = () => {
    setShowAnalysisModal(false)
    handleTransfer()
  }
  
  const getSourceSchema = async () => {
    try {
      if (sourceType === 'clickhouse') {
        // Для ClickHouse источника
        const response = await fetch('/api/database/connect', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          body: new URLSearchParams({
            database_type: 'clickhouse',
            host: sourceChHost,
            port: sourceChPort,
            database: sourceChDatabase,
            username: sourceChUsername,
            password: sourceChPassword,
          })
        })
        
        const data = await response.json()
        return {
          type: sourceType,
          host: sourceChHost,
          database: sourceChDatabase,
          table: sourceChTableName,
          connected: data.connected,
          tables: data.tables || []
        }
      } else if (sourceType === 'postgresql') {
        // Для PostgreSQL источника
        const connectionString = `postgresql://${sourceDbUsername}:${sourceDbPassword}@${sourceDbHost}:${sourceDbPort}/${sourceDbDatabase}`
        const response = await fetch('/api/database/connect', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          body: new URLSearchParams({
            database_type: 'postgresql',
            connection_string: connectionString,
          })
        })
        
        const data = await response.json()
        return {
          type: sourceType,
          host: sourceDbHost,
          database: sourceDbDatabase,
          table: sourceDbTableName,
          connected: data.connected,
          tables: data.tables || []
        }
      } else {
        // Для файловых источников
        return {
          type: sourceType,
          filename: file?.name,
          delimiter: sourceDelimiter
        }
      }
    } catch (error) {
      console.error('Ошибка при получении схемы источника:', error)
      return {
        type: sourceType,
        error: 'Не удалось получить схему источника'
      }
    }
  }
  
  const getSinkSchema = async () => {
    try {
      if (sinkType === 'clickhouse') {
        // Для ClickHouse приёмника
        const response = await fetch('/api/database/connect', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          body: new URLSearchParams({
            database_type: 'clickhouse',
            host: sinkChHost,
            port: sinkChPort,
            database: sinkChDatabase,
            username: sinkChUsername,
            password: sinkChPassword,
          })
        })
        
        const data = await response.json()
        return {
          type: sinkType,
          host: sinkChHost,
          database: sinkChDatabase,
          table: sinkChTableName,
          connected: data.connected,
          tables: data.tables || []
        }
      } else if (sinkType === 'postgresql') {
        // Для PostgreSQL приёмника
        const connectionString = `postgresql://${sinkDbUsername}:${sinkDbPassword}@${sinkDbHost}:${sinkDbPort}/${sinkDbDatabase}`
        const response = await fetch('/api/database/connect', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          body: new URLSearchParams({
            database_type: 'postgresql',
            connection_string: connectionString,
          })
        })
        
        const data = await response.json()
        return {
          type: sinkType,
          host: sinkDbHost,
          database: sinkDbDatabase,
          table: sinkDbTableName,
          connected: data.connected,
          tables: data.tables || []
        }
      } else {
        // Для файловых приёмников
        return {
          type: sinkType,
          delimiter: sinkDelimiter
        }
      }
    } catch (error) {
      console.error('Ошибка при получении схемы приёмника:', error)
      return {
        type: sinkType,
        error: 'Не удалось получить схему приёмника'
      }
    }
  }
  
  const {
    file,
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
  } = useDataTransfer()


  return (
    <FormContainer>
      <Title>Перенос данных</Title>
      <Subtitle>Выберите источник, приёмник и загрузите файл для обработки</Subtitle>
      
      <FormGrid>
            <SourceSelector
              sourceType={sourceType}
              delimiter={sourceDelimiter}
              file={file}
              dbHost={sourceDbHost}
              dbPort={sourceDbPort}
              dbDatabase={sourceDbDatabase}
              dbUsername={sourceDbUsername}
              dbPassword={sourceDbPassword}
              dbTableName={sourceDbTableName}
              chHost={sourceChHost}
              chPort={sourceChPort}
              chDatabase={sourceChDatabase}
              chUsername={sourceChUsername}
              chPassword={sourceChPassword}
              chTableName={sourceChTableName}
              kafkaBootstrapServers={sourceKafkaBootstrapServers}
              kafkaTopic={sourceKafkaTopic}
              kafkaGroupId={sourceKafkaGroupId}
              onSourceTypeChange={setSourceType}
              onDelimiterChange={setSourceDelimiter}
              onFileSelect={setFile}
              onFileNameChange={setFileName}
              onDbHostChange={setSourceDbHost}
              onDbPortChange={setSourceDbPort}
              onDbDatabaseChange={setSourceDbDatabase}
              onDbUsernameChange={setSourceDbUsername}
              onDbPasswordChange={setSourceDbPassword}
              onDbTableNameChange={setSourceDbTableName}
              onChHostChange={setSourceChHost}
              onChPortChange={setSourceChPort}
              onChDatabaseChange={setSourceChDatabase}
              onChUsernameChange={setSourceChUsername}
              onChPasswordChange={setSourceChPassword}
              onChTableNameChange={setSourceChTableName}
              onKafkaBootstrapServersChange={setSourceKafkaBootstrapServers}
              onKafkaTopicChange={setSourceKafkaTopic}
              onKafkaGroupIdChange={setSourceKafkaGroupId}
              disabled={isLoading}
            />
        
        <SinkSelector
          sinkType={sinkType}
          delimiter={sinkDelimiter}
          chunkSize={chunkSize}
          dbHost={sinkDbHost}
          dbPort={sinkDbPort}
          dbDatabase={sinkDbDatabase}
          dbUsername={sinkDbUsername}
          dbPassword={sinkDbPassword}
          dbTableName={sinkDbTableName}
          chHost={sinkChHost}
          chPort={sinkChPort}
          chDatabase={sinkChDatabase}
          chUsername={sinkChUsername}
          chPassword={sinkChPassword}
          chTableName={sinkChTableName}
          kafkaBootstrapServers={sinkKafkaBootstrapServers}
          kafkaTopic={sinkKafkaTopic}
          kafkaKeyField={sinkKafkaKeyField}
          useAirflow={useAirflow}
          sourceType={sourceType}
          onSinkTypeChange={setSinkType}
          onDelimiterChange={setSinkDelimiter}
          onChunkSizeChange={setChunkSize}
          onDbHostChange={setSinkDbHost}
          onDbPortChange={setSinkDbPort}
          onDbDatabaseChange={setSinkDbDatabase}
          onDbUsernameChange={setSinkDbUsername}
          onDbPasswordChange={setSinkDbPassword}
          onDbTableNameChange={setSinkDbTableName}
          onChHostChange={setSinkChHost}
          onChPortChange={setSinkChPort}
          onChDatabaseChange={setSinkChDatabase}
          onChUsernameChange={setSinkChUsername}
          onChPasswordChange={setSinkChPassword}
          onChTableNameChange={setSinkChTableName}
          onKafkaBootstrapServersChange={setSinkKafkaBootstrapServers}
          onKafkaTopicChange={setSinkKafkaTopic}
          onKafkaKeyFieldChange={setSinkKafkaKeyField}
          onUseAirflowChange={setUseAirflow}
          disabled={isLoading}
        />
      </FormGrid>

      <CheckboxContainer>
        <Checkbox
          type="checkbox"
          id="llm-analysis"
          checked={enableLLMAnalysis}
          onChange={(e) => setEnableLLMAnalysis(e.target.checked)}
        />
        <CheckboxLabel htmlFor="llm-analysis">
          🤖 Анализ данных LLM
        </CheckboxLabel>
      </CheckboxContainer>

      <div style={{ display: 'flex', justifyContent: 'center', marginTop: '2rem' }}>
        <button
          onClick={handleTransferWithAnalysis}
          disabled={isLoading || (['csv', 'json', 'xml'].includes(sourceType) && !file)}
          style={{
            padding: '1rem 2rem',
            fontSize: '1.1rem',
            fontWeight: '600',
            background: isLoading || (['csv', 'json', 'xml'].includes(sourceType) && !file) ? '#cbd5e0' : 'linear-gradient(135deg, #4299e1 0%, #3182ce 100%)',
            color: 'white',
            border: 'none',
            borderRadius: '12px',
            cursor: isLoading || (['csv', 'json', 'xml'].includes(sourceType) && !file) ? 'not-allowed' : 'pointer',
            opacity: isLoading || (['csv', 'json', 'xml'].includes(sourceType) && !file) ? 0.6 : 1,
            transition: 'all 0.2s ease',
            boxShadow: '0 4px 12px rgba(66, 153, 225, 0.3)'
          }}
          onMouseEnter={(e) => {
            if (!isLoading && !(['csv', 'json', 'xml'].includes(sourceType) && !file)) {
              e.currentTarget.style.transform = 'translateY(-2px)'
              e.currentTarget.style.boxShadow = '0 8px 20px rgba(66, 153, 225, 0.4)'
            }
          }}
          onMouseLeave={(e) => {
            if (!isLoading && !(['csv', 'json', 'xml'].includes(sourceType) && !file)) {
              e.currentTarget.style.transform = 'translateY(0)'
              e.currentTarget.style.boxShadow = '0 4px 12px rgba(66, 153, 225, 0.3)'
            }
          }}
        >
          {isLoading ? 'Обработка...' : enableLLMAnalysis ? 'Запустить перенос с анализом' : 'Запустить перенос'}
        </button>
      </div>

      {isLoading && <ProgressBar />}

      {error && (
        <ErrorContainer>
          <strong>Ошибка:</strong> {typeof error === 'string' ? error : JSON.stringify(error)}
        </ErrorContainer>
      )}

      {result && (
        <DataPreview 
          data={result}
          sinkType={sinkType}
          onReset={reset}
        />
      )}
      
      {showAnalysisModal && analysisData && (
        <DataAnalysisModal
          isOpen={showAnalysisModal}
          onClose={() => setShowAnalysisModal(false)}
          onConfirm={handleConfirmTransfer}
          sourceSchema={analysisData.sourceSchema}
          sinkSchema={analysisData.sinkSchema}
          sourceType={analysisData.sourceType}
          sinkType={analysisData.sinkType}
        />
      )}
    </FormContainer>
  )
}
