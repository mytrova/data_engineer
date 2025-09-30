import React, { useState } from 'react'
import styled from 'styled-components'
import { MessageSquare, TestTube, List, Eye } from 'lucide-react'

const KafkaContainer = styled.div`
  background: #edf2f7;
  border-radius: 8px;
  padding: 1rem;
  margin-top: 1rem;
  border: 1px solid #cbd5e0;
`

const KafkaHeader = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.75rem;
  color: #4a5568;
  font-weight: 500;
  font-size: 0.9rem;
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
  margin-bottom: 1rem;

  &:focus {
    outline: none;
    border-color: #4299e1;
    box-shadow: 0 0 0 3px rgba(66, 153, 225, 0.1);
  }
`

const Button = styled.button<{ disabled: boolean; variant?: 'primary' | 'secondary' }>`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 1rem;
  border: none;
  border-radius: 8px;
  font-weight: 500;
  cursor: ${props => props.disabled ? 'not-allowed' : 'pointer'};
  transition: all 0.2s ease;
  opacity: ${props => props.disabled ? 0.6 : 1};
  margin-right: 0.5rem;
  margin-bottom: 0.5rem;
  
  ${props => props.variant === 'primary' ? `
    background: #4299e1;
    color: white;
    
    &:hover:not(:disabled) {
      background: #3182ce;
    }
  ` : `
    background: #e2e8f0;
    color: #4a5568;
    
    &:hover:not(:disabled) {
      background: #cbd5e0;
    }
  `}
`

const StatusMessage = styled.div<{ type: 'success' | 'error' | 'info' }>`
  padding: 0.75rem;
  border-radius: 8px;
  margin-top: 1rem;
  font-size: 0.9rem;
  
  ${props => props.type === 'success' ? `
    background: #f0fff4;
    border: 1px solid #9ae6b4;
    color: #22543d;
  ` : props.type === 'error' ? `
    background: #fed7d7;
    border: 1px solid #feb2b2;
    color: #c53030;
  ` : `
    background: #ebf8ff;
    border: 1px solid #90cdf4;
    color: #2c5282;
  `}
`

const TopicsList = styled.div`
  margin-top: 1rem;
  max-height: 200px;
  overflow-y: auto;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  background: white;
`

const TopicItem = styled.div`
  padding: 0.5rem 0.75rem;
  border-bottom: 1px solid #e2e8f0;
  cursor: pointer;
  transition: background-color 0.2s;
  
  &:hover {
    background: #f7fafc;
  }
  
  &:last-child {
    border-bottom: none;
  }
`

interface KafkaConnectionFormProps {
  bootstrapServers: string
  topic: string
  groupId?: string
  keyField?: string
  onBootstrapServersChange: (servers: string) => void
  onTopicChange: (topic: string) => void
  onGroupIdChange?: (groupId: string) => void
  onKeyFieldChange?: (keyField: string) => void
  disabled?: boolean
}

export const KafkaConnectionForm: React.FC<KafkaConnectionFormProps> = ({
  bootstrapServers,
  topic,
  groupId = '',
  keyField = '',
  onBootstrapServersChange,
  onTopicChange,
  onGroupIdChange,
  onKeyFieldChange,
  disabled = false
}) => {
  const [isTesting, setIsTesting] = useState(false)
  const [testResult, setTestResult] = useState<{ type: 'success' | 'error' | 'info', message: string } | null>(null)
  const [topics, setTopics] = useState<string[]>([])
  const [selectedTopic, setSelectedTopic] = useState<string>('')

  const testConnection = async () => {
    if (!bootstrapServers || !topic) {
      setTestResult({ type: 'error', message: 'Заполните все обязательные поля' })
      return
    }

    setIsTesting(true)
    setTestResult(null)

    try {
      const formData = new FormData()
      formData.append('bootstrap_servers', bootstrapServers)
      formData.append('topic', topic)
      formData.append('group_id', groupId)
      formData.append('database_type', 'kafka')

      const response = await fetch('http://localhost:8000/database/connect', {
        method: 'POST',
        body: formData,
      })

      const result = await response.json()

      if (result.status === 'success' && result.connected) {
        setTestResult({ type: 'success', message: 'Подключение к Kafka успешно!' })
        setTopics(result.topics || [])
      } else {
        const errorMessage = result.error || 'Ошибка подключения'
        setTestResult({ type: 'error', message: typeof errorMessage === 'string' ? errorMessage : JSON.stringify(errorMessage) })
        setTopics([])
      }
    } catch (error) {
      setTestResult({ type: 'error', message: 'Ошибка сети' })
      setTopics([])
    } finally {
      setIsTesting(false)
    }
  }

  const getTopicInfo = async (topicName: string) => {
    try {
      const formData = new FormData()
      formData.append('bootstrap_servers', bootstrapServers)
      formData.append('topic', topicName)
      formData.append('database_type', 'kafka')

      const response = await fetch('http://localhost:8000/database/topic-info', {
        method: 'POST',
        body: formData,
      })

      const result = await response.json()
      
      if (result.status === 'success') {
        setTestResult({ type: 'info', message: `Топик ${topicName}: ${result.partitions} партиций` })
      }
    } catch (error) {
      setTestResult({ type: 'error', message: 'Ошибка получения информации о топике' })
    }
  }

  return (
    <KafkaContainer>
      <KafkaHeader>
        <MessageSquare size={16} />
        Подключение к Kafka
      </KafkaHeader>
      
      <div style={{ marginBottom: '1rem' }}>
        <Label htmlFor="kafka-servers">Bootstrap Servers</Label>
        <Input
          id="kafka-servers"
          type="text"
          placeholder="localhost:9092"
          value={bootstrapServers}
          onChange={(e) => onBootstrapServersChange(e.target.value)}
          disabled={disabled}
        />
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginBottom: '1rem' }}>
        <div>
          <Label htmlFor="kafka-topic">Топик</Label>
          <Input
            id="kafka-topic"
            type="text"
            placeholder="my_topic"
            value={topic}
            onChange={(e) => onTopicChange(e.target.value)}
            disabled={disabled}
          />
        </div>
        <div>
          <Label htmlFor="kafka-group">Group ID (опционально)</Label>
          <Input
            id="kafka-group"
            type="text"
            placeholder="my_group"
            value={groupId}
            onChange={(e) => onGroupIdChange?.(e.target.value)}
            disabled={disabled}
          />
        </div>
      </div>

      {onKeyFieldChange && (
        <div style={{ marginBottom: '1rem' }}>
          <Label htmlFor="kafka-key">Key Field (опционально)</Label>
          <Input
            id="kafka-key"
            type="text"
            placeholder="id"
            value={keyField}
            onChange={(e) => onKeyFieldChange(e.target.value)}
            disabled={disabled}
          />
        </div>
      )}

      <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
        <Button
          variant="primary"
          onClick={testConnection}
          disabled={disabled || isTesting}
        >
          <TestTube size={16} />
          {isTesting ? 'Тестирование...' : 'Тестировать'}
        </Button>

        {topics.length > 0 && (
          <Button
            variant="secondary"
            onClick={() => setTestResult({ type: 'info', message: `Найдено топиков: ${topics.length}` })}
            disabled={disabled}
          >
            <List size={16} />
            Топики ({topics.length})
          </Button>
        )}
      </div>

      {testResult && (
        <StatusMessage type={testResult.type}>
          {testResult.message}
        </StatusMessage>
      )}

      {topics.length > 0 && (
        <TopicsList>
          {topics.map((topicName) => (
            <TopicItem
              key={topicName}
              onClick={() => {
                setSelectedTopic(topicName)
                getTopicInfo(topicName)
              }}
              style={{
                background: selectedTopic === topicName ? '#e6fffa' : 'transparent'
              }}
            >
              <Eye size={14} style={{ marginRight: '0.5rem' }} />
              {topicName}
            </TopicItem>
          ))}
        </TopicsList>
      )}
    </KafkaContainer>
  )
}
