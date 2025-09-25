import React from 'react'
import styled from 'styled-components'
import { Database } from 'lucide-react'
import { FileUpload } from './FileUpload'

const CardContainer = styled.div`
  background: #f7fafc;
  border-radius: 12px;
  padding: 1.5rem;
  border: 1px solid #e2e8f0;
`

const CardHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 1.5rem;
  color: #4a5568;
  font-weight: 600;
  font-size: 1.1rem;
`

const Section = styled.div`
  margin-bottom: 1.5rem;
  
  &:last-child {
    margin-bottom: 0;
  }
`

const SectionTitle = styled.h4`
  color: #4a5568;
  font-weight: 600;
  margin-bottom: 0.75rem;
  font-size: 1rem;
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

  &:focus {
    outline: none;
    border-color: #4299e1;
    box-shadow: 0 0 0 3px rgba(66, 153, 225, 0.1);
  }
`


const InfoText = styled.div`
  background: #e6fffa;
  border: 1px solid #81e6d9;
  border-radius: 8px;
  padding: 0.75rem;
  color: #234e52;
  font-size: 0.9rem;
  margin-top: 0.5rem;
`

interface SourceCardProps {
  sourceType: string
  file: File | null
  delimiter: string
  dbCredentials: {
    host: string
    port: string
    database: string
    username: string
    password: string
  }
  onSourceTypeChange: (type: string) => void
  onFileSelect: (file: File | null) => void
  onDelimiterChange: (delimiter: string) => void
  onDbCredentialsChange: (credentials: any) => void
  disabled?: boolean
}

export const SourceCard: React.FC<SourceCardProps> = ({
  sourceType,
  file,
  delimiter,
  onSourceTypeChange,
  onFileSelect,
  onDelimiterChange,
  disabled = false
}) => {
  const needsFile = ['csv', 'json', 'xml'].includes(sourceType)
  const needsDelimiter = sourceType === 'csv'

  return (
    <CardContainer>
      <CardHeader>
        <Database size={20} />
        Источник данных
      </CardHeader>

      <Section>
        <SectionTitle>Тип источника</SectionTitle>
        <Label htmlFor="source-type">Выберите тип источника</Label>
        <Select
          id="source-type"
          value={sourceType}
          onChange={(e) => onSourceTypeChange(e.target.value)}
          disabled={disabled}
        >
          <option value="csv">CSV файл</option>
          <option value="json">JSON файл</option>
          <option value="xml">XML файл</option>
          <option value="database" disabled>База данных (скоро)</option>
        </Select>
      </Section>

      {needsFile && (
        <Section>
          <SectionTitle>Файл данных</SectionTitle>
          <FileUpload 
            file={file}
            onFileSelect={onFileSelect}
            disabled={disabled}
          />
        </Section>
      )}

      {needsDelimiter && (
        <Section>
          <SectionTitle>Настройки CSV</SectionTitle>
          <Label htmlFor="source-delimiter">Разделитель CSV</Label>
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
        </Section>
      )}

      {sourceType === 'database' && (
        <InfoText>
          <strong>База данных:</strong> Подключение к базам данных будет добавлено в следующих версиях.
        </InfoText>
      )}
    </CardContainer>
  )
}
