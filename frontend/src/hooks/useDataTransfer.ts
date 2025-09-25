import { useState } from 'react'

interface TransferResult {
  result?: string
  out_path?: string
  headers?: string[]
  rows?: any[]
}

export const useDataTransfer = () => {
  const [file, setFile] = useState<File | null>(null)
  const [sourceType, setSourceType] = useState('csv')
  const [sinkType, setSinkType] = useState('preview')
  const [chunkSize, setChunkSize] = useState(10)
  const [delimiter, setDelimiter] = useState(';')
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [result, setResult] = useState<TransferResult | null>(null)

  const getApiBase = () => {
    const hostname = window.location.hostname
    if (hostname === 'localhost' || hostname === '127.0.0.1') {
      return 'http://localhost:8000'
    }
    return window.location.origin.replace(':3000', ':8000')
  }

  const handleTransfer = async () => {
    // Проверяем, нужен ли файл для выбранного типа источника
    const needsFile = ['csv', 'json', 'xml'].includes(sourceType)
    if (needsFile && !file) {
      setError('Пожалуйста, выберите файл')
      return
    }

    setIsLoading(true)
    setError(null)
    setResult(null)

    try {
      const formData = new FormData()
      if (file) {
        formData.append('file', file)
      }
      formData.append('source_type', sourceType)
      formData.append('sink_type', sinkType)
      formData.append('chunk_size', chunkSize.toString())
      formData.append('delimiter', delimiter)

      const endpoint = sinkType === 'preview' ? '/upload' : '/transfer'
      const response = await fetch(`${getApiBase()}${endpoint}`, {
        method: 'POST',
        body: formData,
      })

      if (!response.ok) {
        throw new Error(`Ошибка сервера: ${response.status}`)
      }

      if (sinkType === 'preview') {
        const data = await response.json()
        setResult(data)
      } else {
        // Для файловых приёмников
        const blob = await response.blob()
        const url = window.URL.createObjectURL(blob)
        
        // Пытаемся получить имя файла из заголовков
        const contentDisposition = response.headers.get('Content-Disposition') || ''
        const match = contentDisposition.match(/filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/)
        let filename = 'export'
        if (match) {
          filename = decodeURIComponent(match[1] || match[2] || filename)
        } else {
          filename = `export.${sinkType}`
        }

        // Создаем ссылку для скачивания
        const link = document.createElement('a')
        link.href = url
        link.download = filename
        document.body.appendChild(link)
        link.click()
        document.body.removeChild(link)
        window.URL.revokeObjectURL(url)

        setResult({
          result: 'Файл успешно обработан и скачан',
          out_path: filename
        })
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Произошла неизвестная ошибка')
    } finally {
      setIsLoading(false)
    }
  }

  const reset = () => {
    setFile(null)
    setSourceType('csv')
    setSinkType('preview')
    setChunkSize(10)
    setDelimiter(';')
    setIsLoading(false)
    setError(null)
    setResult(null)
  }

  return {
    file,
    sourceType,
    sinkType,
    chunkSize,
    delimiter,
    isLoading,
    error,
    result,
    setFile,
    setSourceType,
    setSinkType,
    setChunkSize,
    setDelimiter,
    handleTransfer,
    reset
  }
}
