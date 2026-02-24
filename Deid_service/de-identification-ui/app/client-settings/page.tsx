'use client'

import { useState, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import Layout from '@/components/Layout'
import { ArrowLeft, Plus, X } from 'lucide-react'
import { configApi } from '@/lib/api'

export default function ClientSettings() {
  const router = useRouter()
  const [adminConnectionStr, setAdminConnectionStr] = useState('')
  const [patientIdentifierColumns, setPatientIdentifierColumns] = useState<string[]>([])
  const [newIdentifierColumn, setNewIdentifierColumn] = useState('')
  const [ndPatientStartValue, setNdPatientStartValue] = useState<number>(0)
  const [defaultOffsetValue, setDefaultOffsetValue] = useState<number>(0)
  const [ehrType, setEhrType] = useState<string>('')
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null)

  useEffect(() => {
    loadConfig()
  }, [])

  const loadConfig = async () => {
    setLoading(true)
    try {
      const response = await configApi.getClientRunConfig()
      if (response.success && response.id) {
        setAdminConnectionStr(response.admin_connection_str || '')
        setPatientIdentifierColumns(response.patient_identifier_columns || [])
        setNdPatientStartValue(response.nd_patient_start_value || 0)
        setDefaultOffsetValue(response.default_offset_value || 0)
        setEhrType(response.ehr_type || '')
      }
    } catch (error) {
      console.error('Error loading config:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleSave = async () => {
    setSaving(true)
    setMessage(null)
    try {
      const response = await configApi.saveClientRunConfig({
        patient_identifier_columns: patientIdentifierColumns,
        admin_connection_str: adminConnectionStr,
        nd_patient_start_value: ndPatientStartValue,
        default_offset_value: defaultOffsetValue,
        ehr_type: ehrType,
        enable_auto_qc: false,
        enable_auto_gcp: false,
        enable_auto_embd: false,
      })

      if (response.success) {
        setMessage({ type: 'success', text: 'Configuration saved successfully!' })
      } else {
        setMessage({ type: 'error', text: response.message || 'Failed to save configuration' })
      }
    } catch (error) {
      setMessage({ type: 'error', text: 'Error saving configuration' })
    } finally {
      setSaving(false)
    }
  }

  const addIdentifierColumn = () => {
    if (newIdentifierColumn.trim() && !patientIdentifierColumns.includes(newIdentifierColumn.trim())) {
      setPatientIdentifierColumns([...patientIdentifierColumns, newIdentifierColumn.trim()])
      setNewIdentifierColumn('')
    }
  }

  const removeIdentifierColumn = (index: number) => {
    setPatientIdentifierColumns(patientIdentifierColumns.filter((_, i) => i !== index))
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
        <h1 className="text-3xl font-bold text-gray-800 mb-2">Client Run Settings</h1>
        <p className="text-gray-600 mb-6">Configure Client Run Parameters</p>

        <div className="bg-white rounded-lg shadow p-6 space-y-6">
          {/* Connection Details */}
          <div>
            <h2 className="text-xl font-semibold text-gray-800 mb-3">Connection Details</h2>
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  EHR Type
                </label>
                <select
                  value={ehrType}
                  onChange={(e) => setEhrType(e.target.value)}
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="">Select EHR Type</option>
                  <option value="anthenaone">AnthenaOne</option>
                  <option value="ecw">eCW</option>
                  <option value="athenpractice">AthenPractice</option>
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Admin Connection String
                </label>
                <input
                  type="text"
                  value={adminConnectionStr}
                  onChange={(e) => setAdminConnectionStr(e.target.value)}
                  placeholder="Enter admin connection string"
                  className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
            </div>
          </div>

          {/* Patient Identifiers */}
          <div>
            <h2 className="text-xl font-semibold text-gray-800 mb-3">Patient Identifiers</h2>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Patient Identifier Columns
            </label>
            <div className="space-y-3">
              {/* List of identifier columns */}
              <div className="flex flex-wrap gap-2">
                {patientIdentifierColumns.map((column, index) => (
                  <div
                    key={index}
                    className="flex items-center gap-2 bg-blue-100 text-blue-800 px-3 py-1 rounded-lg"
                  >
                    <span className="text-sm font-medium">{column}</span>
                    <button
                      onClick={() => removeIdentifierColumn(index)}
                      className="text-blue-600 hover:text-blue-800"
                    >
                      <X className="w-4 h-4" />
                    </button>
                  </div>
                ))}
              </div>
              {/* Add new identifier column */}
              <div className="flex gap-2">
                <input
                  type="text"
                  value={newIdentifierColumn}
                  onChange={(e) => setNewIdentifierColumn(e.target.value)}
                  onKeyPress={(e) => e.key === 'Enter' && addIdentifierColumn()}
                  placeholder="Enter patient identifier column name"
                  className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
                <button
                  onClick={addIdentifierColumn}
                  className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2"
                >
                  <Plus className="w-4 h-4" />
                  Add
                </button>
              </div>
            </div>
          </div>

          {/* Start Values */}
          <div className="grid grid-cols-2 gap-4">
            <div>
              <h2 className="text-xl font-semibold text-gray-800 mb-3">Start Values</h2>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                ND Patient Start Value
              </label>
              <input
                type="number"
                value={ndPatientStartValue}
                onChange={(e) => setNdPatientStartValue(Number(e.target.value))}
                placeholder="0"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <p className="text-xs text-gray-500 mt-1">BigInt value</p>
            </div>
            <div>
              <h2 className="text-xl font-semibold text-gray-800 mb-3">Offset Values</h2>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Default Offset Value
              </label>
              <input
                type="number"
                value={defaultOffsetValue}
                onChange={(e) => setDefaultOffsetValue(Number(e.target.value))}
                placeholder="0"
                className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <p className="text-xs text-gray-500 mt-1">Integer value</p>
            </div>
          </div>

              {/* Message */}
              {message && (
                <div
                  className={`p-4 rounded-lg ${
                    message.type === 'success'
                      ? 'bg-green-100 text-green-700'
                      : 'bg-red-100 text-red-700'
                  }`}
                >
                  {message.text}
                </div>
              )}

              {/* Save Button */}
              <div className="pt-4">
                <button
                  onClick={handleSave}
                  disabled={saving || loading}
                  className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors font-medium disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {saving ? 'Saving...' : 'Save Configuration'}
                </button>
              </div>
        </div>
      </div>
    </Layout>
  )
}

