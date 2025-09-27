import styled from 'styled-components'
import { DataTransferForm } from './components/DataTransferForm'
import { Header } from './components/Header'
import { AirflowDashboard } from './components/AirflowDashboard'
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
            <DataTransferForm />
          </ContentWrapper>
        </MainContent>
      </AppContainer>
    </>
  )
}

export default App
