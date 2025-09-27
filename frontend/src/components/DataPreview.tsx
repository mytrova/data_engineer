import React from 'react'
import styled from 'styled-components'
import { Table as TableIcon, Download, RotateCcw } from 'lucide-react'

const PreviewContainer = styled.div`
  background: #f7fafc;
  border-radius: 12px;
  padding: 1.5rem;
  border: 1px solid #e2e8f0;
  margin-top: 1rem;
`

const PreviewHeader = styled.div`
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 1rem;
`

const PreviewTitle = styled.h3`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  color: #4a5568;
  font-weight: 600;
`

const ButtonGroup = styled.div`
  display: flex;
  gap: 0.5rem;
`

const Button = styled.button<{ variant?: 'primary' | 'secondary' }>`
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 1rem;
  border: none;
  border-radius: 8px;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s ease;
  
  ${props => props.variant === 'primary' ? `
    background: #4299e1;
    color: white;
    
    &:hover {
      background: #3182ce;
    }
  ` : `
    background: #e2e8f0;
    color: #4a5568;
    
    &:hover {
      background: #cbd5e0;
    }
  `}
`

const TableContainer = styled.div`
  overflow-x: auto;
  border-radius: 8px;
  border: 1px solid #e2e8f0;
  background: white;
`

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
`

const TableHeader = styled.thead`
  background: #edf2f7;
`

const TableHeaderCell = styled.th`
  padding: 1rem;
  text-align: left;
  font-weight: 600;
  color: #4a5568;
  border-bottom: 1px solid #e2e8f0;
`

const TableBody = styled.tbody``

const TableRow = styled.tr`
  &:nth-child(even) {
    background: #f7fafc;
  }
  
  &:hover {
    background: #edf2f7;
  }
`

const TableCell = styled.td`
  padding: 0.75rem 1rem;
  border-bottom: 1px solid #e2e8f0;
  color: #4a5568;
  max-width: 200px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`

const NoDataMessage = styled.div`
  text-align: center;
  padding: 2rem;
  color: #718096;
  font-style: italic;
`

interface DataPreviewProps {
  data: any
  sinkType: string
  onReset: () => void
}

export const DataPreview: React.FC<DataPreviewProps> = ({ 
  data, 
  sinkType, 
  onReset 
}) => {
  const handleDownload = () => {
    if (data && data.out_path) {
      // Создаем ссылку для скачивания файла
      const link = document.createElement('a')
      link.href = `/api/download/${data.out_path}`
      link.download = `export.${sinkType}`
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
    }
  }

  const renderTable = () => {
    if (!data || !data.headers || !data.rows) {
      return <NoDataMessage>Нет данных для отображения</NoDataMessage>
    }

    const { headers, rows } = data

    return (
      <>
        <div style={{ 
          marginBottom: '1rem', 
          padding: '0.75rem', 
          background: '#e6fffa', 
          border: '1px solid #81e6d9', 
          borderRadius: '8px',
          color: '#234e52',
          fontSize: '0.9rem'
        }}>
          <strong>ℹ️ Предпросмотр:</strong> Показаны первые {rows.length} строк из файла. 
          Для полной обработки выберите другой тип приёмника.
        </div>
        <TableContainer>
          <Table>
            <TableHeader>
              <tr>
                {headers.map((header: string, index: number) => (
                  <TableHeaderCell key={index}>{header}</TableHeaderCell>
                ))}
              </tr>
            </TableHeader>
            <TableBody>
              {rows.map((row: any[], rowIndex: number) => (
                <TableRow key={rowIndex}>
                  {headers.map((_: string, cellIndex: number) => (
                    <TableCell key={cellIndex}>
                      {row[cellIndex] ?? ''}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      </>
    )
  }

  return (
    <PreviewContainer>
      <PreviewHeader>
        <PreviewTitle>
          <TableIcon size={20} />
          {sinkType === 'preview' ? 'Предпросмотр данных' : 'Результат обработки'}
        </PreviewTitle>
        
        <ButtonGroup>
          {sinkType !== 'preview' && data?.out_path && (
            <Button variant="primary" onClick={handleDownload}>
              <Download size={16} />
              Скачать файл
            </Button>
          )}
          <Button variant="secondary" onClick={onReset}>
            <RotateCcw size={16} />
            Новый перенос
          </Button>
        </ButtonGroup>
      </PreviewHeader>

      {sinkType === 'preview' ? renderTable() : (
        <div>
          <p><strong>Статус:</strong> {
            // Если это Airflow DAG
            data?.dag_id ? (
              data.result || 'Создание DAG в фоновом режиме...'
            ) : (
              // Обычная обработка данных
              typeof data?.result === 'string' ? data.result : 
              (data?.result as any)?.status === 'success' ? 
                `Данные успешно записаны в таблицу "${(data?.result as any)?.table}". Записано строк: ${(data?.result as any)?.rows_written}` :
                JSON.stringify(data?.result) || 'Обработка завершена'
            )
          }</p>
          {data?.out_path && (
            <p><strong>Путь к файлу:</strong> {data.out_path}</p>
          )}
          {data?.dag_id && (
            <p><strong>DAG ID:</strong> {data.dag_id}</p>
          )}
        </div>
      )}
    </PreviewContainer>
  )
}
