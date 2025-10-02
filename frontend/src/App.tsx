import styled from 'styled-components'
import { DataTransferForm } from './components/DataTransferForm'
import { Header } from './components/Header'
import { AirflowDashboard } from './components/AirflowDashboard'
import LargeFileManager from './components/LargeFileManager'
import LLMChat from './components/LLMChat'
import { GlobalStyles } from './styles/GlobalStyles'

const AppContainer = styled.div`
  min-height: 100vh;
  display: flex;
  flex-direction: column;
`

const MainContent = styled.main`
  flex: 1;
  display: flex;
  justify-content: center;
  align-items: flex-start;
  padding: 2rem;
`

const ContentWrapper = styled.div`
  width: 100%;
  max-width: 1200px;
`

function App() {
  return (
    <>
      <GlobalStyles />
      <AppContainer>
        <Header />
        <MainContent>
          <ContentWrapper>
            <AirflowDashboard />
            <LargeFileManager />
            <DataTransferForm />
          </ContentWrapper>
        </MainContent>
        <LLMChat />
      </AppContainer>
    </>
  )
}

export default App
