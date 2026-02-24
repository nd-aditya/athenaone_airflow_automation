'use client'

import { useEffect, useMemo, useState } from 'react'
import Layout from '@/components/Layout'
import { AlertCircle, CheckCircle, Loader2 } from 'lucide-react'
import { operationsApi, queueApi } from '@/lib/api'

type OperationKey =
  | 'start_deid'
  | 'hold_deid'
  | 'start_qc'
  | 'hold_qc'
  | 'gcp_move'
  | 'start_embd'

interface OperationConfig {
  key: OperationKey
  label: string
  actionLabel: string
  description: string
  style: {
    active: string
    idle: string
    hover: string
  }
  badgeClass: string
}

const operations: OperationConfig[] = [
  {
    key: 'start_deid',
    label: 'Start DeID',
    actionLabel: 'Execute DeID',
    description: 'Kick off the de-identification pipeline for the provided tables.',
    style: {
      active: 'bg-red-600 text-white',
      idle: 'text-red-200 border border-red-500/40',
      hover: 'hover:bg-red-500/80',
    },
    badgeClass: 'bg-red-500/20 text-red-200',
  },
  {
    key: 'hold_deid',
    label: 'Hold DeID',
    actionLabel: 'Apply DeID Hold',
    description: 'Pause DeID for tables already registered in a run.',
    style: {
      active: 'bg-red-200 text-red-900',
      idle: 'text-red-200 border border-red-200/40',
      hover: 'hover:bg-red-200/80 hover:text-red-900',
    },
    badgeClass: 'bg-red-200/70 text-red-900',
  },
  {
    key: 'start_qc',
    label: 'Start QC',
    actionLabel: 'Execute QC',
    description: 'Start quality control validations for the tables.',
    style: {
      active: 'bg-blue-500 text-white',
      idle: 'text-blue-200 border border-blue-500/40',
      hover: 'hover:bg-blue-500/80',
    },
    badgeClass: 'bg-blue-500/20 text-blue-100',
  },
  {
    key: 'hold_qc',
    label: 'Hold QC',
    actionLabel: 'Apply QC Hold',
    description: 'Pause in-flight QC runs or keep tables on hold.',
    style: {
      active: 'bg-blue-200 text-blue-900',
      idle: 'text-blue-200 border border-blue-200/40',
      hover: 'hover:bg-blue-200/80 hover:text-blue-900',
    },
    badgeClass: 'bg-blue-200/70 text-blue-900',
  },
  {
    key: 'gcp_move',
    label: 'GCP Move',
    actionLabel: 'Trigger GCP Move',
    description: 'Move processed artifacts to the configured cloud storage bucket.',
    style: {
      active: 'bg-sky-400 text-gray-900',
      idle: 'text-sky-200 border border-sky-400/40',
      hover: 'hover:bg-sky-300/80 hover:text-gray-900',
    },
    badgeClass: 'bg-sky-400/30 text-gray-900',
  },
  {
    key: 'start_embd',
    label: 'Start Embedding',
    actionLabel: 'Execute Embedding',
    description: 'Kick off embedding computation for vector search.',
    style: {
      active: 'bg-purple-500 text-white',
      idle: 'text-purple-200 border border-purple-500/40',
      hover: 'hover:bg-purple-500/80',
    },
    badgeClass: 'bg-purple-500/20 text-purple-100',
  },
]

const parseTableNames = (input: string) =>
  input
    .split(/[\n,]+/)
    .map((name) => name.trim())
    .filter(Boolean)

export default function ProcessingOptions() {
  const [selectedOperation, setSelectedOperation] = useState<OperationConfig | null>(operations[0])
  const [tableInput, setTableInput] = useState('')
  const [isExecuting, setIsExecuting] = useState(false)
  const [toast, setToast] = useState<{ type: 'success' | 'error'; text: string } | null>(null)
  const [queues, setQueues] = useState<{ id: number; queue_name: string }[]>([])
  const [queueLoading, setQueueLoading] = useState(false)
  const [selectedQueueId, setSelectedQueueId] = useState<number | null>(null)

  const tableList = useMemo(() => parseTableNames(tableInput), [tableInput])

  useEffect(() => {
    const loadQueues = async () => {
      setQueueLoading(true)
      try {
        const response = await queueApi.getQueues({ page_size: 50 })
        if (response.success && response.results) {
          const simplified = response.results
            .filter((queue: any) => queue.id !== null && queue.queue_name)
            .map((queue: any) => ({ id: queue.id as number, queue_name: queue.queue_name as string }))
          setQueues(simplified)
          if (simplified.length > 0) {
            setSelectedQueueId((prev) => prev ?? simplified[0].id)
          }
        }
      } catch (error) {
        console.error('Failed to load queues', error)
      } finally {
        setQueueLoading(false)
      }
    }

    loadQueues()
  }, [])

  const handleExecute = async () => {
    if (!selectedOperation) {
      setToast({ type: 'error', text: 'Select an operation first.' })
      return
    }

    if (tableList.length === 0) {
      setToast({ type: 'error', text: 'Provide at least one table name.' })
      return
    }

    setIsExecuting(true)
    setToast(null)

    try {
      if (selectedOperation.key === 'start_deid') {
        if (!selectedQueueId) {
          setToast({ type: 'error', text: 'Select a queue to start DeID.' })
          return
        }
        const response = await operationsApi.startDeid(selectedQueueId, { tables_name: tableList })
        if (response.success) {
          setToast({
            type: 'success',
            text: response.message || `DeID started for ${tableList.length} table(s).`,
          })
          setTableInput('')
        } else {
          setToast({
            type: 'error',
            text: response.message || 'Failed to start DeID.',
          })
        }
      } else if (selectedOperation.key === 'start_qc') {
        if (!selectedQueueId) {
          setToast({ type: 'error', text: 'Select a queue to start QC.' })
          return
        }
        const response = await operationsApi.startQc(selectedQueueId, { tables_name: tableList })
        if (response.success) {
          setToast({
            type: 'success',
            text: response.message || `QC started for ${tableList.length} table(s).`,
          })
          setTableInput('')
        } else {
          setToast({
            type: 'error',
            text: response.message || 'Failed to start QC.',
          })
        }
      } else {
        await new Promise((resolve) => setTimeout(resolve, 500))
        setToast({
          type: 'error',
          text: `${selectedOperation.label} is not wired yet. Please use DeID or QC options.`,
        })
      }
    } catch (error) {
      setToast({
        type: 'error',
        text: 'Failed to trigger the operation. Please retry.',
      })
    } finally {
      setIsExecuting(false)
    }
  }

  return (
    <Layout dark>
      <div className="text-white space-y-6">
        <div>
          <h1 className="text-3xl font-bold mb-2">Processing Options</h1>
          <p className="text-gray-400">
            Trigger or pause bulk pipeline stages for specific tables. Select the desired operation and provide the table
            names exactly as registered.
          </p>
        </div>

        <div className="bg-gray-800 rounded-lg border border-gray-700 p-6 space-y-5">
          <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
            <div>
              <h2 className="text-xl font-semibold">Bulk Table Operations</h2>
              <p className="text-sm text-gray-400">Select the stage update you want to perform.</p>
            </div>
            <div className="flex flex-col items-start md:items-end">
              <label className="text-xs uppercase tracking-wide text-gray-400 mb-1">Select Queue</label>
              <select
                value={selectedQueueId ?? ''}
                onChange={(event) => {
                  const value = event.target.value
                  setSelectedQueueId(value ? Number(value) : null)
                }}
                disabled={queueLoading || queues.length === 0}
                className="bg-gray-900 border border-gray-700 text-white rounded-lg px-3 py-2 text-sm min-w-[220px] focus:ring-2 focus:ring-blue-500 focus:outline-none disabled:opacity-60"
              >
                {queueLoading ? (
                  <option>Loading queues...</option>
                ) : queues.length === 0 ? (
                  <option value="">No queues available</option>
                ) : (
                  queues.map((queue) => (
                    <option key={queue.id} value={queue.id}>
                      {queue.queue_name}
                    </option>
                  ))
                )}
              </select>
            </div>
          </div>

          <div>
            {selectedOperation && (
              <div className="flex items-center justify-between mb-3">
                <span className={`text-xs uppercase tracking-wide px-3 py-1 rounded-full ${selectedOperation.badgeClass}`}>
                  {selectedOperation.label}
                </span>
                <span className="text-xs text-gray-500">
                  Queue:{' '}
                  {selectedQueueId
                    ? queues.find((queue) => queue.id === selectedQueueId)?.queue_name || selectedQueueId
                    : 'Not selected'}
                </span>
              </div>
            )}
            <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
              {operations.map((operation) => {
                const isActive = selectedOperation?.key === operation.key
                return (
                  <button
                    key={operation.key}
                    type="button"
                    onClick={() => setSelectedOperation(operation)}
                    className={`w-full rounded-lg px-4 py-3 text-sm font-semibold transition-colors ${
                      isActive
                        ? `${operation.style.active}`
                        : `${operation.style.idle} ${operation.style.hover} bg-gray-900/40 rounded-lg`
                    }`}
                  >
                    {operation.label}
                  </button>
                )
              })}
            </div>
          </div>

          {selectedOperation && (
            <div className="bg-gray-900/40 rounded-lg p-4 border border-gray-700">
              <p className="text-sm text-gray-300">{selectedOperation.description}</p>
            </div>
          )}

          <div className="space-y-3">
            <label className="block text-sm font-medium text-gray-300">Table Names (comma-separated or newline)</label>
            <textarea
              value={tableInput}
              onChange={(event) => setTableInput(event.target.value)}
              placeholder="Enter table names separated by commas or new lines, e.g. table1, table2, table3"
              rows={5}
              className="w-full bg-gray-900/60 border border-gray-700 rounded-lg px-4 py-3 text-white placeholder-gray-500 focus:ring-2 focus:ring-blue-500 focus:outline-none"
            />
            <p className="text-xs text-gray-500">
              Enter table names exactly as they appear in the system. Accepted separators: comma, space, or newline.
            </p>

            <div className="flex items-center justify-between text-sm text-gray-400">
              <span>{tableList.length} table{tableList.length === 1 ? '' : 's'} detected</span>
              {tableList.length > 0 && (
                <span className="font-mono text-gray-300 truncate max-w-[60%]">
                  {tableList.slice(0, 3).join(', ')}
                  {tableList.length > 3 && ' ...'}
                </span>
              )}
            </div>
          </div>

          {toast && (
            <div
              className={`flex items-center gap-3 rounded-lg px-4 py-3 text-sm ${
                toast.type === 'success' ? 'bg-green-500/10 text-green-200' : 'bg-red-500/10 text-red-200'
              }`}
            >
              {toast.type === 'success' ? <CheckCircle className="w-4 h-4" /> : <AlertCircle className="w-4 h-4" />}
              <span>{toast.text}</span>
            </div>
          )}

          <div className="flex items-center justify-end">
            <button
              type="button"
              onClick={handleExecute}
              disabled={isExecuting || !selectedOperation}
              className={`flex items-center gap-2 px-6 py-3 rounded-lg font-semibold transition-colors ${
                selectedOperation ? 'bg-blue-600 hover:bg-blue-500 text-white' : 'bg-gray-600 text-gray-300'
              } disabled:opacity-60 disabled:cursor-not-allowed`}
            >
              {isExecuting && <Loader2 className="w-4 h-4 animate-spin" />}
              {selectedOperation ? selectedOperation.actionLabel : 'Select Operation'}
            </button>
          </div>
        </div>
      </div>
    </Layout>
  )
}

