import React, { useState } from 'react'
import styled from 'styled-components'

const ModalOverlay = styled.div`
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 2000;
`

const ModalContent = styled.div`
  background: white;
  border-radius: 12px;
  padding: 24px;
  max-width: 800px;
  max-height: 80vh;
  overflow-y: auto;
  box-shadow: 0 20px 40px rgba(0, 0, 0, 0.15);
  position: relative;
`

const CloseButton = styled.button`
  position: absolute;
  top: 16px;
  right: 16px;
  background: none;
  border: none;
  font-size: 24px;
  cursor: pointer;
  color: #666;
  
  &:hover {
    color: #333;
  }
`

const Title = styled.h2`
  margin: 0 0 20px 0;
  color: #333;
  font-size: 24px;
  font-weight: 600;
`

const AnalysisSection = styled.div`
  margin-bottom: 24px;
  padding: 20px;
  background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
  border-radius: 12px;
  border-left: 4px solid #667eea;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
`

const SectionTitle = styled.h3`
  margin: 0 0 12px 0;
  color: #333;
  font-size: 18px;
  font-weight: 600;
`

const AnalysisText = styled.div`
  color: #555;
  line-height: 1.6;
  white-space: pre-wrap;
  font-size: 14px;
  
  h1, h2, h3, h4, h5, h6 {
    color: #333;
    margin: 16px 0 8px 0;
    font-weight: 600;
  }
  
  h1 { font-size: 18px; }
  h2 { font-size: 16px; }
  h3 { font-size: 15px; }
  
  ul, ol {
    margin: 12px 0;
    padding-left: 24px;
  }
  
  li {
    margin: 6px 0;
    line-height: 1.5;
  }
  
  ul li {
    list-style-type: none;
    position: relative;
  }
  
  ul li:before {
    content: "•";
    color: #667eea;
    font-weight: bold;
    position: absolute;
    left: -20px;
  }
  
  strong {
    color: #2d3748;
    font-weight: 600;
  }
  
  em {
    color: #667eea;
    font-style: italic;
  }
  
  code {
    background: #f7fafc;
    padding: 2px 6px;
    border-radius: 4px;
    font-family: 'Monaco', 'Menlo', monospace;
    font-size: 13px;
    color: #e53e3e;
  }
  
  blockquote {
    border-left: 4px solid #667eea;
    padding-left: 16px;
    margin: 12px 0;
    background: #f8f9fa;
    padding: 12px 16px;
    border-radius: 0 8px 8px 0;
  }
  
  .highlight {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;
    padding: 8px 12px;
    border-radius: 6px;
    margin: 8px 0;
    font-weight: 500;
  }
  
  .warning {
    background: #fff3cd;
    border: 1px solid #ffeaa7;
    color: #856404;
    padding: 12px;
    border-radius: 8px;
    margin: 8px 0;
  }
  
  .success {
    background: #d4edda;
    border: 1px solid #c3e6cb;
    color: #155724;
    padding: 12px;
    border-radius: 8px;
    margin: 8px 0;
  }
  
  .info {
    background: #d1ecf1;
    border: 1px solid #bee5eb;
    color: #0c5460;
    padding: 12px;
    border-radius: 8px;
    margin: 8px 0;
  }
`

const LoadingSpinner = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 40px;
  color: #667eea;
  font-size: 16px;
  
  .spinner {
    width: 20px;
    height: 20px;
    border: 2px solid #f3f3f3;
    border-top: 2px solid #667eea;
    border-radius: 50%;
    animation: spin 1s linear infinite;
    margin-right: 12px;
  }
  
  @keyframes spin {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
  }
`

const ButtonGroup = styled.div`
  display: flex;
  gap: 12px;
  justify-content: flex-end;
  margin-top: 24px;
  padding-top: 20px;
  border-top: 1px solid #e1e5e9;
`

const Button = styled.button<{ variant?: 'primary' | 'secondary' }>`
  padding: 12px 24px;
  border-radius: 8px;
  border: none;
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.2s;
  
  ${props => props.variant === 'primary' ? `
    background: #667eea;
    color: white;
    
    &:hover:not(:disabled) {
      background: #5a6fd8;
    }
  ` : `
    background: #f1f3f4;
    color: #333;
    
    &:hover:not(:disabled) {
      background: #e8eaed;
    }
  `}
  
  &:disabled {
    background: #ccc;
    cursor: not-allowed;
  }
`

interface DataAnalysisModalProps {
  isOpen: boolean
  onClose: () => void
  onConfirm: () => void
  sourceSchema: any
  sinkSchema: any
  sourceType: string
  sinkType: string
}

const DataAnalysisModal: React.FC<DataAnalysisModalProps> = ({
  isOpen,
  onClose,
  onConfirm,
  sourceSchema,
  sinkSchema,
  sourceType,
  sinkType
}) => {
  const [analysis, setAnalysis] = useState<string>('')
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string>('')

  const formatAnalysisText = (text: string): string => {
    return text
      // Заголовки
      .replace(/^# (.+)$/gm, '<h1>$1</h1>')
      .replace(/^## (.+)$/gm, '<h2>$1</h2>')
      .replace(/^### (.+)$/gm, '<h3>$1</h3>')
      
      // Списки
      .replace(/^(\d+\.\s)/gm, '<li>$1')
      .replace(/^[-*]\s/gm, '<li>• ')
      
      // Выделение текста
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/\*(.+?)\*/g, '<em>$1</em>')
      .replace(/`(.+?)`/g, '<code>$1</code>')
      
      // Блоки кода
      .replace(/```([\s\S]*?)```/g, '<blockquote><code>$1</code></blockquote>')
      
      // Предупреждения и важная информация
      .replace(/⚠️\s*(.+)/g, '<div class="warning">⚠️ $1</div>')
      .replace(/✅\s*(.+)/g, '<div class="success">✅ $1</div>')
      .replace(/ℹ️\s*(.+)/g, '<div class="info">ℹ️ $1</div>')
      .replace(/🔍\s*(.+)/g, '<div class="highlight">🔍 $1</div>')
      
      // Переносы строк
      .replace(/\n\n/g, '</p><p>')
      .replace(/\n/g, '<br>')
      
      // Обернуть в параграфы
      .replace(/^(?!<[h1-6]|<li|<div|<blockquote)/gm, '<p>')
      .replace(/(?<!>)$/gm, '</p>')
      
      // Очистить лишние теги
      .replace(/<p><\/p>/g, '')
      .replace(/<p><li>/g, '<li>')
      .replace(/<\/li><\/p>/g, '</li>')
      .replace(/<p><div/g, '<div')
      .replace(/<\/div><\/p>/g, '</div>')
      .replace(/<p><blockquote/g, '<blockquote')
      .replace(/<\/blockquote><\/p>/g, '</blockquote>')
  }

  React.useEffect(() => {
    if (isOpen && sourceSchema && sinkSchema) {
      analyzeDataStructure()
    }
  }, [isOpen, sourceSchema, sinkSchema])

  const analyzeDataStructure = async () => {
    setIsLoading(true)
    setError('')
    
    try {
      const response = await fetch('/api/llm/analyze-data', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          source_schema: sourceSchema,
          sink_schema: sinkSchema,
          source_type: sourceType,
          sink_type: sinkType
        })
      })

      const data = await response.json()
      
      if (data.status === 'success') {
        setAnalysis(data.analysis || data.response)
      } else {
        setError(data.error || 'Ошибка при анализе данных')
      }
    } catch (err) {
      console.error('Ошибка при анализе данных:', err)
      if (err instanceof TypeError && err.message.includes('fetch')) {
        setError('Ошибка подключения к серверу. Проверьте, что backend запущен.')
      } else if (err instanceof Error && err.message.includes('Server disconnected')) {
        setError('Соединение с LLM API прервано. Попробуйте еще раз.')
      } else {
        setError(`Ошибка при обращении к LLM: ${err}`)
      }
    } finally {
      setIsLoading(false)
    }
  }

  if (!isOpen) return null

  return (
    <ModalOverlay onClick={onClose}>
      <ModalContent onClick={(e) => e.stopPropagation()}>
        <CloseButton onClick={onClose}>×</CloseButton>
        
        <Title>Анализ структуры данных</Title>
        
        {isLoading && (
          <LoadingSpinner>
            <div className="spinner"></div>
            Анализируем структуру данных...
          </LoadingSpinner>
        )}
        
        {error && (
          <AnalysisSection>
            <SectionTitle>Ошибка</SectionTitle>
            <AnalysisText style={{ color: '#d32f2f' }}>
              {error}
            </AnalysisText>
          </AnalysisSection>
        )}
        
        {analysis && !isLoading && (
          <AnalysisSection>
            <SectionTitle>Анализ ИИ</SectionTitle>
            <AnalysisText 
              dangerouslySetInnerHTML={{ 
                __html: formatAnalysisText(analysis) 
              }} 
            />
            <div style={{ 
              marginTop: '16px', 
              padding: '8px 12px', 
              background: '#f0f4f8', 
              borderRadius: '6px', 
              fontSize: '12px', 
              color: '#666' 
            }}>
              Длина ответа: {analysis.length} символов
            </div>
          </AnalysisSection>
        )}
        
        {!isLoading && !error && (
          <ButtonGroup>
            <Button variant="secondary" onClick={onClose}>
              Отмена
            </Button>
            <Button variant="primary" onClick={onConfirm} disabled={!analysis}>
              Продолжить перенос
            </Button>
          </ButtonGroup>
        )}
      </ModalContent>
    </ModalOverlay>
  )
}

export default DataAnalysisModal
