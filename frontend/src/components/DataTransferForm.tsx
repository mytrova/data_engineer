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
          onSinkTypeChange={setSinkType}
          onDelimiterChange={setSinkDelimiter}
          onChunkSizeChange={setChunkSize}
          onDbHostChange={setSinkDbHost}
          onDbPortChange={setSinkDbPort}
          onDbDatabaseChange={setSinkDbDatabase}
          onDbUsernameChange={setSinkDbUsername}
          onDbPasswordChange={setSinkDbPassword}
          onDbTableNameChange={setSinkDbTableName}
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
