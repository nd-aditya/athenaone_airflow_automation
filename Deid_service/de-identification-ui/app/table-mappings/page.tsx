'use client'

import { useState, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import Layout from '@/components/Layout'
import { ArrowLeft } from 'lucide-react'
import { configApi } from '@/lib/api'
import JsonEditor from '@/components/JsonEditor'

export default function TableMappings() {
  const router = useRouter()
  const [config, setConfig] = useState(`{
  "mapping_schema": "",
  "patient_mapping": {
    "auto_generate_tables": true,
    "primary_id_column": "",
    "tables": []
  },
  "encounter_mapping": {
    "table": "",
    "encounter_id": "",
    "patient_id_column": "",
    "patient_identifier_type": "",
    "encounter_date": ""
  },
  "appointment_mapping": {
    "enabled": false,
    "table": "",
    "appointment_id": "",
    "patient_id_column": "",
    "patient_identifier_type": "",
    "appointment_date": ""
  }
}`)
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null)

  useEffect(() => {
    loadConfig()
  }, [])

  const loadConfig = async () => {
    setLoading(true)
    try {
      const response = await configApi.getMappingConfig()
      if (response.success && response.mapping_config) {
        // Build the full config object
        const fullConfig = {
          mapping_schema: response.mapping_schema || '',
          ...response.mapping_config,
        }
        setConfig(JSON.stringify(fullConfig, null, 2))
      }
    } catch (error) {
      console.error('Error loading config:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleSave = async () => {
    try {
      const parsed = JSON.parse(config)
      setSaving(true)
      setMessage(null)
      
      // Extract mapping_schema and mapping_config
      const { mapping_schema, ...mapping_config } = parsed
      
      const response = await configApi.saveMappingConfig({
        mapping_config: mapping_config,
        mapping_schema: mapping_schema || '',
      })
      
      if (response.success) {
        setMessage({ type: 'success', text: 'Configuration saved successfully!' })
      } else {
        setMessage({ type: 'error', text: response.message || 'Failed to save configuration' })
      }
    } catch (error) {
      setMessage({ type: 'error', text: 'Invalid JSON format. Please fix the JSON before saving.' })
    } finally {
      setSaving(false)
    }
  }

  const handleValidate = () => {
    try {
      JSON.parse(config)
      alert('JSON is valid!')
    } catch (error) {
      alert('Invalid JSON: ' + (error as Error).message)
    }
  }

  const handleFormat = () => {
    try {
      const parsed = JSON.parse(config)
      setConfig(JSON.stringify(parsed, null, 2))
    } catch (error) {
      alert('Invalid JSON: ' + (error as Error).message)
    }
  }

  return (
    <Layout>
      <div>
        <button
          onClick={() => router.push('/configuration')}
          className="flex items-center gap-2 text-gray-600 hover:text-gray-800 mb-4 transition-colors"
        >
          <ArrowLeft className="w-5 h-5" />
          <span>Back to Configuration</span>
        </button>
        
        <div className="bg-white rounded-lg shadow p-6">
          <h1 className="text-3xl font-bold text-gray-800 mb-6">Mappings Configuration</h1>

          <JsonEditor
            value={config}
            onChange={setConfig}
            onValidate={handleValidate}
            onFormat={handleFormat}
            rows={25}
          />

          {message && (
            <div
              className={`p-4 rounded-lg mb-4 ${
                message.type === 'success'
                  ? 'bg-green-100 text-green-700'
                  : 'bg-red-100 text-red-700'
              }`}
            >
              {message.text}
            </div>
          )}

          <button
            onClick={handleSave}
            disabled={saving || loading}
            className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors font-medium w-full disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {saving ? 'Saving...' : 'Save Configuration'}
          </button>
        </div>
      </div>
    </Layout>
  )
}
