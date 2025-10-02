import React, { useState, useRef, useEffect } from 'react'
import styled from 'styled-components'

const ChatContainer = styled.div`
  position: fixed;
  bottom: 20px;
  right: 20px;
  width: 400px;
  height: 500px;
  background: white;
  border-radius: 12px;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
  display: flex;
  flex-direction: column;
  z-index: 1000;
  border: 1px solid #e1e5e9;
`

const ChatHeader = styled.div`
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  padding: 16px 20px;
  border-radius: 12px 12px 0 0;
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: 600;
`

const CloseButton = styled.button`
  background: none;
  border: none;
  color: white;
  font-size: 20px;
  cursor: pointer;
  padding: 4px;
  border-radius: 4px;
  transition: background-color 0.2s;

  &:hover {
    background-color: rgba(255, 255, 255, 0.2);
  }
`

const MessagesContainer = styled.div`
  flex: 1;
  overflow-y: auto;
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 12px;
`

const Message = styled.div<{ isUser: boolean }>`
  display: flex;
  justify-content: ${props => props.isUser ? 'flex-end' : 'flex-start'};
  
  .message-content {
    max-width: 80%;
    padding: 12px 16px;
    border-radius: 18px;
    background: ${props => props.isUser ? '#667eea' : '#f1f3f4'};
    color: ${props => props.isUser ? 'white' : '#333'};
    word-wrap: break-word;
    line-height: 1.4;
  }
`

const InputContainer = styled.div`
  padding: 16px;
  border-top: 1px solid #e1e5e9;
  display: flex;
  gap: 8px;
`

const MessageInput = styled.input`
  flex: 1;
  padding: 12px 16px;
  border: 1px solid #e1e5e9;
  border-radius: 24px;
  outline: none;
  font-size: 14px;

  &:focus {
    border-color: #667eea;
  }
`

const SendButton = styled.button`
  background: #667eea;
  color: white;
  border: none;
  border-radius: 50%;
  width: 40px;
  height: 40px;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: background-color 0.2s;

  &:hover:not(:disabled) {
    background: #5a6fd8;
  }

  &:disabled {
    background: #ccc;
    cursor: not-allowed;
  }
`

const ToggleButton = styled.button`
  position: fixed;
  bottom: 20px;
  right: 20px;
  width: 60px;
  height: 60px;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  color: white;
  border: none;
  border-radius: 50%;
  cursor: pointer;
  font-size: 24px;
  box-shadow: 0 4px 16px rgba(102, 126, 234, 0.4);
  transition: transform 0.2s, box-shadow 0.2s;
  z-index: 1001;

  &:hover {
    transform: scale(1.05);
    box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
  }
`

const LoadingDots = styled.div`
  display: inline-flex;
  gap: 4px;
  
  .dot {
    width: 6px;
    height: 6px;
    background: #667eea;
    border-radius: 50%;
    animation: bounce 1.4s infinite ease-in-out both;
  }
  
  .dot:nth-child(1) { animation-delay: -0.32s; }
  .dot:nth-child(2) { animation-delay: -0.16s; }
  
  @keyframes bounce {
    0%, 80%, 100% {
      transform: scale(0);
    }
    40% {
      transform: scale(1);
    }
  }
`

interface MessageType {
  id: string
  text: string
  isUser: boolean
  timestamp: Date
}

const LLMChat: React.FC = () => {
  const [isOpen, setIsOpen] = useState(false)
  const [messages, setMessages] = useState<MessageType[]>([])
  const [inputValue, setInputValue] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const messagesEndRef = useRef<HTMLDivElement>(null)

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  const sendMessage = async () => {
    if (!inputValue.trim() || isLoading) return

    const userMessage: MessageType = {
      id: Date.now().toString(),
      text: inputValue.trim(),
      isUser: true,
      timestamp: new Date()
    }

    setMessages(prev => [...prev, userMessage])
    setInputValue('')
    setIsLoading(true)

    try {
      const response = await fetch('/api/llm/ask', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ message: inputValue.trim() })
      })

      const data = await response.json()

      const aiMessage: MessageType = {
        id: (Date.now() + 1).toString(),
        text: data.status === 'success' ? data.response : `Ошибка: ${data.error}`,
        isUser: false,
        timestamp: new Date()
      }

      setMessages(prev => [...prev, aiMessage])
    } catch (error) {
      const errorMessage: MessageType = {
        id: (Date.now() + 1).toString(),
        text: `Ошибка при отправке сообщения: ${error}`,
        isUser: false,
        timestamp: new Date()
      }
      setMessages(prev => [...prev, errorMessage])
    } finally {
      setIsLoading(false)
    }
  }

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  const clearChat = () => {
    setMessages([])
  }

  if (!isOpen) {
    return (
      <ToggleButton onClick={() => setIsOpen(true)} title="Открыть чат с ИИ">
        💬
      </ToggleButton>
    )
  }

  return (
    <>
      <ChatContainer>
        <ChatHeader>
          <span>Чат с ИИ</span>
          <div>
            <button
              onClick={clearChat}
              style={{
                background: 'none',
                border: 'none',
                color: 'white',
                marginRight: '8px',
                cursor: 'pointer',
                padding: '4px',
                borderRadius: '4px'
              }}
              title="Очистить чат"
            >
              🗑️
            </button>
            <CloseButton onClick={() => setIsOpen(false)} title="Закрыть чат">
              ×
            </CloseButton>
          </div>
        </ChatHeader>
        
        <MessagesContainer>
          {messages.length === 0 && (
            <div style={{ 
              textAlign: 'center', 
              color: '#666', 
              fontStyle: 'italic',
              marginTop: '20px'
            }}>
              Привет! Я ИИ-ассистент. Чем могу помочь?
            </div>
          )}
          
          {messages.map((message) => (
            <Message key={message.id} isUser={message.isUser}>
              <div className="message-content">
                {message.text}
              </div>
            </Message>
          ))}
          
          {isLoading && (
            <Message isUser={false}>
              <div className="message-content">
                <LoadingDots>
                  <div className="dot"></div>
                  <div className="dot"></div>
                  <div className="dot"></div>
                </LoadingDots>
              </div>
            </Message>
          )}
          
          <div ref={messagesEndRef} />
        </MessagesContainer>
        
        <InputContainer>
          <MessageInput
            value={inputValue}
            onChange={(e) => setInputValue(e.target.value)}
            onKeyPress={handleKeyPress}
            placeholder="Введите сообщение..."
            disabled={isLoading}
          />
          <SendButton onClick={sendMessage} disabled={isLoading || !inputValue.trim()}>
            ➤
          </SendButton>
        </InputContainer>
      </ChatContainer>
    </>
  )
}

export default LLMChat
