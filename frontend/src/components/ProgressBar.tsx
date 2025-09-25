import React from 'react'
import styled, { keyframes } from 'styled-components'
import { Loader2 } from 'lucide-react'

const spin = keyframes`
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
`

const ProgressContainer = styled.div`
  background: #f7fafc;
  border-radius: 12px;
  padding: 1.5rem;
  border: 1px solid #e2e8f0;
  margin-bottom: 1rem;
`

const ProgressHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 0.75rem;
  margin-bottom: 1rem;
  color: #4a5568;
  font-weight: 600;
`

const Spinner = styled.div`
  animation: ${spin} 1s linear infinite;
  color: #4299e1;
`

const ProgressBarWrapper = styled.div`
  width: 100%;
  height: 8px;
  background: #e2e8f0;
  border-radius: 4px;
  overflow: hidden;
`

const ProgressFill = styled.div`
  height: 100%;
  background: linear-gradient(90deg, #4299e1 0%, #3182ce 100%);
  border-radius: 4px;
  animation: progress 2s ease-in-out infinite;
  width: 100%;
`

const ProgressText = styled.div`
  text-align: center;
  color: #718096;
  font-size: 0.9rem;
  margin-top: 0.5rem;
`

export const ProgressBar: React.FC = () => {
  return (
    <ProgressContainer>
      <ProgressHeader>
        <Spinner>
          <Loader2 size={20} />
        </Spinner>
        Обработка данных...
      </ProgressHeader>
      
      <ProgressBarWrapper>
        <ProgressFill />
      </ProgressBarWrapper>
      
      <ProgressText>
        Пожалуйста, подождите, пока данные обрабатываются
      </ProgressText>
    </ProgressContainer>
  )
}
