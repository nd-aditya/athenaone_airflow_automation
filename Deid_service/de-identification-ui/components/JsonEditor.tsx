'use client'

import { useState } from 'react'
import JsonView from '@uiw/react-json-view'

interface JsonEditorProps {
  value: string
  onChange: (value: string) => void
  onValidate?: () => void
  onFormat?: () => void
  rows?: number
}

export default function JsonEditor({ value, onChange, onValidate, onFormat, rows = 25 }: JsonEditorProps) {
  const [isEditMode, setIsEditMode] = useState(true)
  const [jsonData, setJsonData] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)

  const handleToggleMode = () => {
    if (isEditMode) {
      // Switch to view mode
      try {
        const parsed = JSON.parse(value)
        setJsonData(parsed)
        setError(null)
        setIsEditMode(false)
      } catch (err) {
        setError('Invalid JSON. Cannot switch to view mode.')
      }
    } else {
      // Switch to edit mode
      setIsEditMode(true)
    }
  }

  const handleJsonChange = (updatedData: any) => {
    try {
      const jsonString = JSON.stringify(updatedData, null, 2)
      onChange(jsonString)
      setJsonData(updatedData)
      setError(null)
    } catch (err) {
      setError('Error updating JSON')
    }
  }

  return (
    <div className="w-full">
      <div className="flex items-center justify-between mb-2">
        <div className="flex gap-2">
          <button
            onClick={handleToggleMode}
            className="px-3 py-1 text-sm bg-gray-100 text-gray-700 rounded hover:bg-gray-200 transition-colors"
          >
            {isEditMode ? 'View Mode' : 'Edit Mode'}
          </button>
          {onValidate && (
            <button
              onClick={onValidate}
              className="px-3 py-1 text-sm bg-blue-100 text-blue-700 rounded hover:bg-blue-200 transition-colors"
            >
              Validate JSON
            </button>
          )}
          {onFormat && (
            <button
              onClick={onFormat}
              className="px-3 py-1 text-sm bg-white text-gray-700 rounded hover:bg-gray-100 transition-colors border border-gray-300"
            >
              Format Code
            </button>
          )}
        </div>
      </div>

      {error && (
        <div className="mb-2 p-2 bg-red-100 text-red-700 rounded text-sm">
          {error}
        </div>
      )}

      {isEditMode ? (
        <div className="border border-gray-300 rounded-lg">
          <textarea
            value={value}
            onChange={(e) => {
              onChange(e.target.value)
              setError(null)
            }}
            rows={rows}
            className="w-full px-4 py-3 font-mono text-sm focus:outline-none resize-none"
            style={{ fontFamily: 'monospace' }}
            placeholder="Enter JSON configuration..."
          />
        </div>
      ) : (
        <div className="border border-gray-300 rounded-lg p-4 bg-gray-50 max-h-[600px] overflow-auto">
          <JsonView
            value={jsonData}
            displayDataTypes={false}
            collapsed={2}
            style={{ 
              backgroundColor: 'transparent',
              '--w-rjv-font-family': 'monospace',
              '--w-rjv-border-radius': '4px',
            } as React.CSSProperties}
            onEdit={(edit) => {
              if (edit.updated_src) {
                handleJsonChange(edit.updated_src)
              }
            }}
            onAdd={(add) => {
              if (add.updated_src) {
                handleJsonChange(add.updated_src)
              }
            }}
            onDelete={(del) => {
              if (del.updated_src) {
                handleJsonChange(del.updated_src)
              }
            }}
          />
        </div>
      )}
    </div>
  )
}

