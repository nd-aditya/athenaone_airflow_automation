'use client'

import { useState, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import Layout from '@/components/Layout'
import { ArrowLeft } from 'lucide-react'
import { configApi } from '@/lib/api'
import JsonEditor from '@/components/JsonEditor'

export default function PIIMasking() {
  const router = useRouter()
  const [primaryConfig, setPrimaryConfig] = useState(`{
  "masking_rules": [
    {
      "field": "patient_name",
      "type": "pseudonymize",
      "algorithm": "HASH"
    },
    {
      "field": "email",
      "type": "mask",
      "algorithm": "MASK_LAST_FOUR"
    }
  ]
}`)

  const [secondaryConfig, setSecondaryConfig] = useState(`{
  "fallback_rules": [
    {
      "field": "ssn",
      "type": "redact",
      "algorithm": "REDACT_LAST_TWO"
    }
  ]
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
      const response = await configApi.getPIIMaskingConfig()
      if (response.success) {
        if (response.pii_masking_config) {
          setPrimaryConfig(JSON.stringify(response.pii_masking_config, null, 2))
        }
        if (response.secondary_config) {
          setSecondaryConfig(JSON.stringify(response.secondary_config, null, 2))
        }
      }
    } catch (error) {
      console.error('Error loading config:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleSave = async () => {
    try {
      const parsedPrimary = JSON.parse(primaryConfig)
      const parsedSecondary = JSON.parse(secondaryConfig)
      setSaving(true)
      setMessage(null)
      const response = await configApi.savePIIMaskingConfig({
        pii_masking_config: parsedPrimary,
        secondary_config: parsedSecondary,
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

  const handleValidatePrimary = () => {
    try {
      JSON.parse(primaryConfig)
      alert('Primary JSON is valid!')
    } catch (error) {
      alert('Invalid JSON: ' + (error as Error).message)
    }
  }

  const handleFormatPrimary = () => {
    try {
      const parsed = JSON.parse(primaryConfig)
      setPrimaryConfig(JSON.stringify(parsed, null, 2))
    } catch (error) {
      alert('Invalid JSON: ' + (error as Error).message)
    }
  }

  const handleValidateSecondary = () => {
    try {
      JSON.parse(secondaryConfig)
      alert('Secondary JSON is valid!')
    } catch (error) {
      alert('Invalid JSON: ' + (error as Error).message)
    }
  }

  const handleFormatSecondary = () => {
    try {
      const parsed = JSON.parse(secondaryConfig)
      setSecondaryConfig(JSON.stringify(parsed, null, 2))
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
        
        <h1 className="text-3xl font-bold text-gray-800 mb-6">PII Masking Configuration</h1>

        <div className="grid grid-cols-2 gap-6">
          {/* Primary Masking Config */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-xl font-semibold text-gray-800 mb-4">Primary Masking Config</h2>
            
            <JsonEditor
              value={primaryConfig}
              onChange={setPrimaryConfig}
              onValidate={handleValidatePrimary}
              onFormat={handleFormatPrimary}
              rows={20}
            />
          </div>

          {/* Secondary Config */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-xl font-semibold text-gray-800 mb-4">Secondary Config (Optional)</h2>
            
            <JsonEditor
              value={secondaryConfig}
              onChange={setSecondaryConfig}
              onValidate={handleValidateSecondary}
              onFormat={handleFormatSecondary}
              rows={20}
            />
          </div>
        </div>

        {message && (
          <div
            className={`p-4 rounded-lg mt-6 ${
              message.type === 'success'
                ? 'bg-green-100 text-green-700'
                : 'bg-red-100 text-red-700'
            }`}
          >
            {message.text}
          </div>
        )}

        <div className="mt-6">
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
