'use client'

import { useState, useEffect } from 'react'
import Layout from '@/components/Layout'
import { CheckCircle, X, ArrowRight } from 'lucide-react'
import { useRouter } from 'next/navigation'
import { configApi } from '@/lib/api'

interface Step {
  id: number
  title: string
  status: 'complete' | 'pending' | 'in-progress'
  actionLabel: string
  actionHref?: string
}

export default function Configuration() {
  const router = useRouter()
  const [steps, setSteps] = useState<Step[]>([
    {
      id: 1,
      title: 'Client Global Settings',
      status: 'pending',
      actionLabel: 'Configure',
      actionHref: '/client-settings',
    },
    {
      id: 2,
      title: 'Mappings Configuration',
      status: 'pending',
      actionLabel: 'Configure Mappings',
      actionHref: '/table-mappings',
    },
    {
      id: 3,
      title: 'Master Table Configuration',
      status: 'pending',
      actionLabel: 'Configure Master Table',
      actionHref: '/table-metadata',
    },
    {
      id: 4,
      title: 'PII Masking Configuration',
      status: 'pending',
      actionLabel: 'Configure PII Masking',
      actionHref: '/pii-masking',
    },
    {
      id: 5,
      title: 'Quality Control Rules',
      status: 'pending',
      actionLabel: 'Set QC Rules',
      actionHref: '/qc-rules',
    },
  ])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    loadConfigStatuses()
    
    // Refresh status when page becomes visible (user returns from config page)
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') {
        loadConfigStatuses()
      }
    }
    
    document.addEventListener('visibilitychange', handleVisibilityChange)
    
    return () => {
      document.removeEventListener('visibilitychange', handleVisibilityChange)
    }
  }, [])

  const loadConfigStatuses = async () => {
    setLoading(true)
    try {
      // Check each configuration status
      const [clientRun, mapping, masterTable, piiMasking, qc] = await Promise.all([
        configApi.getClientRunConfig(),
        configApi.getMappingConfig(),
        configApi.getMasterTableConfig(),
        configApi.getPIIMaskingConfig(),
        configApi.getQCConfig(),
      ])

      setSteps([
        {
          id: 1,
          title: 'Client Global Settings',
          status: clientRun.success && clientRun.id && clientRun.is_configured ? 'complete' : 'pending',
          actionLabel: clientRun.success && clientRun.id && clientRun.is_configured ? 'COMPLETE' : 'Configure',
          actionHref: '/client-settings',
        },
        {
          id: 2,
          title: 'Mappings Configuration',
          status: mapping.success && (mapping.id || mapping.is_configured) ? 'complete' : 'pending',
          actionLabel: mapping.success && (mapping.id || mapping.is_configured) ? 'COMPLETE' : 'Configure Mappings',
          actionHref: '/table-mappings',
        },
        {
          id: 3,
          title: 'Master Table Configuration',
          status: masterTable.success && masterTable.id && masterTable.is_configured ? 'complete' : 'pending',
          actionLabel: masterTable.success && masterTable.id && masterTable.is_configured ? 'COMPLETE' : 'Configure Master Table',
          actionHref: '/master-table',
        },
        {
          id: 4,
          title: 'PII Masking Configuration',
          status: piiMasking.success && piiMasking.id && piiMasking.is_configured ? 'complete' : 'pending',
          actionLabel: piiMasking.success && piiMasking.id && piiMasking.is_configured ? 'COMPLETE' : 'Configure PII Masking',
          actionHref: '/pii-masking',
        },
        {
          id: 5,
          title: 'Quality Control Rules',
          status: qc.success && qc.id && qc.is_configured ? 'complete' : 'pending',
          actionLabel: qc.success && qc.id && qc.is_configured ? 'COMPLETE' : 'Set QC Rules',
          actionHref: '/qc-rules',
        },
      ])
    } catch (error) {
      console.error('Error loading config statuses:', error)
    } finally {
      setLoading(false)
    }
  }

  const completedSteps = steps.filter(s => s.status === 'complete').length
  const totalSteps = steps.length
  const progressPercentage = Math.round((completedSteps / totalSteps) * 100)

  const handleAction = (step: Step) => {
    if (step.actionHref) {
      router.push(step.actionHref)
    }
  }

  const refreshStatus = () => {
    loadConfigStatuses()
  }

  const getStatusIcon = (status: string, stepNumber: number) => {
    if (status === 'complete') {
      return (
        <div className="w-10 h-10 rounded-full bg-green-500 flex items-center justify-center text-white font-bold">
          <CheckCircle className="w-6 h-6" />
        </div>
      )
    } else if (status === 'pending') {
      return (
        <div className="w-10 h-10 rounded-full bg-gray-300 flex items-center justify-center text-gray-600 font-bold">
          <X className="w-5 h-5" />
        </div>
      )
    } else {
      return (
        <div className="w-10 h-10 rounded-full bg-gray-300 flex items-center justify-center text-gray-600 font-bold">
          {stepNumber}
        </div>
      )
    }
  }

  const getStatusBadge = (status: string) => {
    if (status === 'complete') {
      return (
        <span className="px-3 py-1 bg-green-100 text-green-700 rounded-full text-xs font-medium">
          COMPLETE
        </span>
      )
    } else if (status === 'pending') {
      return (
        <span className="px-3 py-1 bg-orange-100 text-orange-700 rounded-full text-xs font-medium">
          PENDING
        </span>
      )
    } else {
      return (
        <span className="px-3 py-1 bg-orange-100 text-orange-700 rounded-full text-xs font-medium">
          IN PROGRESS
        </span>
      )
    }
  }

  return (
    <Layout>
      <div>
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-800 mb-2">Data Pipeline Configuration</h1>
            <p className="text-gray-600">Complete the checklist to finalize your data processing setup</p>
          </div>
          <button
            onClick={refreshStatus}
            disabled={loading}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? 'Loading...' : 'Refresh Status'}
          </button>
        </div>

        {/* Progress Bar */}
        <div className="bg-white rounded-lg shadow p-6 mb-6">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm font-medium text-gray-700">
              {loading ? 'Loading...' : `${progressPercentage}% Completed`}
            </span>
            <span className="text-sm font-medium text-gray-700">
              {loading ? '...' : `${completedSteps}/${totalSteps} Steps Finalized`}
            </span>
          </div>
          <div className="w-full bg-gray-200 rounded-full h-3">
            <div
              className="bg-blue-600 h-3 rounded-full transition-all duration-300"
              style={{ width: loading ? '0%' : `${progressPercentage}%` }}
            ></div>
          </div>
        </div>

        {/* Steps Checklist */}
        <div className="space-y-4">
          {steps.map((step) => (
            <div
              key={step.id}
              className="bg-white rounded-lg shadow p-6 flex items-center justify-between"
            >
              <div className="flex items-center gap-4 flex-1">
                {getStatusIcon(step.status, step.id)}
                <div className="flex-1">
                  <div className="flex items-center gap-3 mb-2">
                    <h3 className="text-lg font-semibold text-gray-800">
                      {step.id}. {step.title}
                    </h3>
                    {step.status !== 'complete' && getStatusBadge(step.status)}
                  </div>
                </div>
              </div>
              <div className="flex items-center gap-2">
                {step.status === 'complete' && (
                  <button
                    onClick={() => handleAction(step)}
                    className="px-4 py-2 bg-green-600 text-white rounded-lg font-medium hover:bg-green-700 transition-colors"
                  >
                    {step.actionLabel}
                  </button>
                )}
                {step.status !== 'complete' && (
                  <button
                    onClick={() => handleAction(step)}
                    className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg font-medium hover:bg-gray-300 transition-colors"
                  >
                    {step.actionLabel}
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>
    </Layout>
  )
}

