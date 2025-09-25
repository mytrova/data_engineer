import React from 'react'
import styled from 'styled-components'
import { Database, Zap } from 'lucide-react'

const HeaderContainer = styled.header`
  background: rgba(255, 255, 255, 0.1);
  backdrop-filter: blur(10px);
  border-bottom: 1px solid rgba(255, 255, 255, 0.2);
  padding: 1rem 2rem;
`

const HeaderContent = styled.div`
  max-width: 1200px;
  margin: 0 auto;
  display: flex;
  align-items: center;
  gap: 1rem;
`

const Logo = styled.div`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  color: white;
  font-size: 1.5rem;
  font-weight: 700;
`

const IconWrapper = styled.div`
  display: flex;
  align-items: center;
  gap: 0.25rem;
`

const Subtitle = styled.p`
  color: rgba(255, 255, 255, 0.8);
  font-size: 0.9rem;
  margin-left: 2rem;
`

export const Header: React.FC = () => {
  return (
    <HeaderContainer>
      <HeaderContent>
        <Logo>
          <IconWrapper>
            <Database size={32} />
            <Zap size={20} />
          </IconWrapper>
          Data Orchestrator
        </Logo>
        <Subtitle>Перенос данных между источниками и приёмниками</Subtitle>
      </HeaderContent>
    </HeaderContainer>
  )
}
