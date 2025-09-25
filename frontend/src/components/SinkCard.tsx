import React from 'react'
import styled from 'styled-components'
import { Download } from 'lucide-react'

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

const PreviewInfo = styled.div`
  background: #fef5e7;
  border: 1px solid #f6ad55;
  border-radius: 8px;
  padding: 0.75rem;
  color: #744210;
  font-size: 0.9rem;
  margin-top: 0.5rem;
`

interface SinkCardProps {
  sinkType: string
  delimiter: string
  onSinkTypeChange: (type: string) => void
  onDelimiterChange: (delimiter: string) => void
  disabled?: boolean
}

export const SinkCard: React.FC<SinkCardProps> = ({
  sinkType,
  delimiter,
  onSinkTypeChange,
  onDelimiterChange,
  disabled = false
}) => {
  const needsDelimiter = sinkType === 'csv'
  const isPreview = sinkType === 'preview'

  return (
    <CardContainer>
      <CardHeader>
        <Download size={20} />
        Приёмник данных
      </CardHeader>

      <Section>
        <SectionTitle>Тип приёмника</SectionTitle>
        <Label htmlFor="sink-type">Выберите тип приёмника</Label>
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
          <option value="database" disabled>База данных (скоро)</option>
        </Select>
      </Section>

      {needsDelimiter && (
        <Section>
          <SectionTitle>Настройки CSV</SectionTitle>
          <Label htmlFor="sink-delimiter">Разделитель CSV</Label>
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
        </Section>
      )}

      {isPreview && (
        <PreviewInfo>
          <strong>Предпросмотр:</strong> Показывает первые 10 строк данных для быстрого просмотра структуры.
        </PreviewInfo>
      )}

      {sinkType === 'database' && (
        <InfoText>
          <strong>База данных:</strong> Сохранение в базы данных будет добавлено в следующих версиях.
        </InfoText>
      )}
    </CardContainer>
  )
}
