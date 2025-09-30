import React from 'react'
import styled from 'styled-components'
import { SourceSelector } from './SourceSelector'
import { SinkSelector } from './SinkSelector'
import { DataPreview } from './DataPreview'
import { ProgressBar } from './ProgressBar'
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


export const DataTransferForm: React.FC = () => {
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

      <div style={{ display: 'flex', justifyContent: 'center', marginTop: '2rem' }}>
        <button
          onClick={handleTransfer}
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
          {isLoading ? 'Обработка...' : 'Запустить перенос'}
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
    </FormContainer>
  )
}
