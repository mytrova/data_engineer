import React from 'react'
import styled from 'styled-components'
import { useDropzone } from 'react-dropzone'
import { Upload, File, X } from 'lucide-react'

const UploadContainer = styled.div<{ isDragActive: boolean; disabled: boolean }>`
  border: 2px dashed ${props => 
    props.isDragActive ? '#4299e1' : 
    props.disabled ? '#cbd5e0' : '#a0aec0'
  };
  border-radius: 12px;
  padding: 2rem;
  text-align: center;
  cursor: ${props => props.disabled ? 'not-allowed' : 'pointer'};
  background: ${props => 
    props.isDragActive ? 'rgba(66, 153, 225, 0.1)' : 
    props.disabled ? '#f7fafc' : 'transparent'
  };
  transition: all 0.2s ease;
  opacity: ${props => props.disabled ? 0.6 : 1};

  &:hover {
    border-color: ${props => props.disabled ? '#cbd5e0' : '#4299e1'};
    background: ${props => props.disabled ? '#f7fafc' : 'rgba(66, 153, 225, 0.05)'};
  }
`

const UploadIcon = styled.div`
  color: #4299e1;
  margin-bottom: 1rem;
`

const UploadText = styled.div`
  color: #4a5568;
  font-size: 1.1rem;
  margin-bottom: 0.5rem;
`

const UploadSubtext = styled.div`
  color: #718096;
  font-size: 0.9rem;
`

const FileInfo = styled.div`
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0.5rem;
  margin-top: 1rem;
  padding: 0.75rem;
  background: #edf2f7;
  border-radius: 8px;
  color: #4a5568;
`

const RemoveButton = styled.button`
  background: none;
  border: none;
  color: #e53e3e;
  cursor: pointer;
  padding: 0.25rem;
  border-radius: 4px;
  display: flex;
  align-items: center;
  justify-content: center;

  &:hover {
    background: #fed7d7;
  }
`

interface FileUploadProps {
  file: File | null
  onFileSelect: (file: File | null) => void
  disabled?: boolean
}

export const FileUpload: React.FC<FileUploadProps> = ({ 
  file, 
  onFileSelect, 
  disabled = false 
}) => {
  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop: (acceptedFiles) => {
      if (acceptedFiles.length > 0) {
        onFileSelect(acceptedFiles[0])
      }
    },
    accept: {
      'text/csv': ['.csv'],
      'application/json': ['.json'],
      'application/xml': ['.xml'],
      'text/xml': ['.xml']
    },
    multiple: false,
    disabled
  })

  const handleRemove = (e: React.MouseEvent) => {
    e.stopPropagation()
    onFileSelect(null)
  }

  return (
    <UploadContainer
      {...getRootProps()}
      isDragActive={isDragActive}
      disabled={disabled}
    >
      <input {...getInputProps()} />
      
      {file ? (
        <FileInfo>
          <File size={20} />
          <span>{file.name}</span>
          <RemoveButton onClick={handleRemove} disabled={disabled}>
            <X size={16} />
          </RemoveButton>
        </FileInfo>
      ) : (
        <>
          <UploadIcon>
            <Upload size={48} />
          </UploadIcon>
          <UploadText>
            {isDragActive ? 'Отпустите файл здесь' : 'Перетащите файл или нажмите для выбора'}
          </UploadText>
          <UploadSubtext>
            Поддерживаются форматы: CSV, JSON, XML
          </UploadSubtext>
        </>
      )}
    </UploadContainer>
  )
}
