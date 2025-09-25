import React from 'react'
import styled from 'styled-components'
import { Settings } from 'lucide-react'

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

interface ProcessingSettingsProps {
  chunkSize: number
  onChunkSizeChange: (size: number) => void
  disabled?: boolean
}

export const ProcessingSettings: React.FC<ProcessingSettingsProps> = ({
  chunkSize,
  onChunkSizeChange,
  disabled = false
}) => {
  return (
    <CardContainer>
      <CardHeader>
        <Settings size={20} />
        Настройки обработки
      </CardHeader>
      
      <Label htmlFor="chunk-size">Размер чанка (строк)</Label>
      <Input
        id="chunk-size"
        type="number"
        min="1"
        value={chunkSize}
        onChange={(e) => onChunkSizeChange(parseInt(e.target.value) || 10)}
        disabled={disabled}
      />
      
      <InfoText>
        <strong>Размер чанка:</strong> Количество строк, обрабатываемых за один раз. 
        Большие значения ускоряют обработку, но требуют больше памяти.
      </InfoText>
    </CardContainer>
  )
}
