'use client'

import { useState, useEffect } from 'react'
import { useSearchParams } from 'next/navigation'
import Layout from '@/components/Layout'
import { CheckCircle, X, Clock, AlertCircle, Search, ChevronDown } from 'lucide-react'
import { monitoringApi, queueApi } from '@/lib/api'

interface TableStatus {
  table_id: number
  table_name: string
  run_range: string
  deid_status: string
  qc_status: string
  embd_status: string
  gcp_status: string
  pipeline_state: string
}

type StatusSummarySection = Record<string, number>

interface StatusSummary {
  deid: StatusSummarySection
  qc: StatusSummarySection
  embd: StatusSummarySection
  gcp: StatusSummarySection
}

const STATUS_ORDER = ['Not Started', 'In Progress', 'Completed', 'Interrupted', 'Failed']
const GCP_STATUS_ORDER = ['Not Started', 'In Process', 'Moved', 'Interrupted', 'Failed']

const DEFAULT_STATUS_SUMMARY: StatusSummary = {
  deid: STATUS_ORDER.reduce((acc, status) => ({ ...acc, [status]: 0 }), {}),
  qc: STATUS_ORDER.reduce((acc, status) => ({ ...acc, [status]: 0 }), {}),
  embd: STATUS_ORDER.reduce((acc, status) => ({ ...acc, [status]: 0 }), {}),
  gcp: GCP_STATUS_ORDER.reduce((acc, status) => ({ ...acc, [status]: 0 }), {}),
}

const normalizeStatusSummary = (summary?: Partial<StatusSummary>): StatusSummary => ({
  deid: { ...DEFAULT_STATUS_SUMMARY.deid, ...(summary?.deid || {}) },
  qc: { ...DEFAULT_STATUS_SUMMARY.qc, ...(summary?.qc || {}) },
  embd: { ...DEFAULT_STATUS_SUMMARY.embd, ...(summary?.embd || {}) },
  gcp: { ...DEFAULT_STATUS_SUMMARY.gcp, ...(summary?.gcp || {}) },
})

export default function Monitoring() {
  const searchParams = useSearchParams()
  const queueFromUrl = searchParams.get('queue')
  const [selectedQueueRun, setSelectedQueueRun] = useState(queueFromUrl || '')
  const [searchTableName, setSearchTableName] = useState('')
  const [availableQueues, setAvailableQueues] = useState<string[]>([])
  const [tables, setTables] = useState<TableStatus[]>([])
  const [loading, setLoading] = useState(false)
  const [totalTables, setTotalTables] = useState(0)
  const [statusSummary, setStatusSummary] = useState<StatusSummary>(normalizeStatusSummary())
  const [isStatusOpen, setIsStatusOpen] = useState(false)

  useEffect(() => {
    loadAvailableQueues()
  }, [])

  useEffect(() => {
    if (selectedQueueRun) {
      loadQueueTables(selectedQueueRun)
    }
  }, [selectedQueueRun])

  const loadAvailableQueues = async () => {
    try {
      const response = await monitoringApi.getAvailableQueues()
      if (response.success && response.queue_names) {
        setAvailableQueues(response.queue_names)
        if (response.queue_names.length > 0 && !selectedQueueRun) {
          setSelectedQueueRun(response.queue_names[0])
        }
      }
    } catch (error) {
      console.error('Error loading queues:', error)
      // Fallback to queue management API
      const queueResponse = await queueApi.getQueues()
      if (queueResponse.success && queueResponse.results) {
        const queueNames = queueResponse.results.map((q: any) => q.queue_name)
        setAvailableQueues(queueNames)
        if (queueNames.length > 0 && !selectedQueueRun) {
          setSelectedQueueRun(queueNames[0])
        }
      }
    }
  }

  const loadQueueTables = async (queueName: string) => {
    setLoading(true)
    try {
      const response = await monitoringApi.getQueueTables(queueName)
      if (response.success && response.tables) {
        setTables(response.tables)
        setTotalTables(response.total_tables || 0)
        setStatusSummary(normalizeStatusSummary(response.status_summary))
      }
    } catch (error) {
      console.error('Error loading queue tables:', error)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (queueFromUrl) {
      setSelectedQueueRun(queueFromUrl)
    }
  }, [queueFromUrl])

  const [filters, setFilters] = useState({
    deid: [] as string[],
    qc: [] as string[],
    embd: [] as string[],
    gcp: [] as string[],
  })

  const getStatusIcon = (status: string) => {
    if (status.toLowerCase().includes('completed')) {
      return <CheckCircle className="w-5 h-5 text-green-500" />
    } else if (status.toLowerCase().includes('failed')) {
      return <X className="w-5 h-5 text-red-500" />
    } else if (status.toLowerCase().includes('pending') || status.toLowerCase().includes('in progress')) {
      return <Clock className="w-5 h-5 text-blue-500" />
    } else if (status.toLowerCase().includes('interrupted')) {
      return <AlertCircle className="w-5 h-5 text-orange-500" />
    }
    return <Clock className="w-5 h-5 text-gray-400" />
  }

  const getStatusColor = (status: string) => {
    if (status.toLowerCase().includes('completed')) {
      return 'text-green-600'
    } else if (status.toLowerCase().includes('failed')) {
      return 'text-red-600'
    } else if (status.toLowerCase().includes('pending') || status.toLowerCase().includes('in progress')) {
      return 'text-blue-600'
    } else if (status.toLowerCase().includes('interrupted')) {
      return 'text-orange-600'
    }
    return 'text-gray-600'
  }

  const toggleFilter = (category: keyof typeof filters, value: string) => {
    setFilters((prev) => {
      const current = prev[category]
      if (current.includes(value)) {
        return { ...prev, [category]: current.filter((v) => v !== value) }
      } else {
        return { ...prev, [category]: [...current, value] }
      }
    })
  }

  const filteredTables = tables.filter((table) => {
    const matchesSearch = searchTableName === '' || 
      table.table_name.toLowerCase().includes(searchTableName.toLowerCase())
    
    const matchesDeid = filters.deid.length === 0 || filters.deid.includes(table.deid_status)
    const matchesQc = filters.qc.length === 0 || filters.qc.includes(table.qc_status)
    const matchesEmbd = filters.embd.length === 0 || filters.embd.includes(table.embd_status)
    const matchesGcp = filters.gcp.length === 0 || filters.gcp.includes(table.gcp_status)

    return matchesSearch && matchesDeid && matchesQc && matchesEmbd && matchesGcp
  })

  return (
    <Layout dark>
      <div className="text-white">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold mb-2">Pipeline Monitoring Dashboard</h1>
            <p className="text-gray-400">Detailed view for selected queue run</p>
          </div>
        <div className="flex items-center gap-6">
          <div className="bg-blue-600 rounded-lg px-6 py-3 min-w-[180px] text-right">
            <p className="text-sm text-blue-100 mb-1">Total Tables</p>
            <p className="text-3xl font-bold">{loading ? '...' : totalTables}</p>
          </div>
          <div className="flex items-center gap-3">
            <label className="text-sm text-gray-400">Select Queue Run</label>
            <select
              value={selectedQueueRun}
              onChange={(e) => setSelectedQueueRun(e.target.value)}
              className="bg-gray-800 text-white px-4 py-2 rounded-lg border border-gray-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {availableQueues.length === 0 ? (
                <option value="">No queues available</option>
              ) : (
                availableQueues.map((queue) => (
                  <option key={queue} value={queue}>
                    {queue}
                  </option>
                ))
              )}
              {queueFromUrl && !availableQueues.includes(queueFromUrl) && (
                <option value={queueFromUrl}>{queueFromUrl}</option>
              )}
            </select>
          </div>
        </div>
        </div>

        {/* Collapsible Status Summary */}
        <div className="bg-gray-800 rounded-lg mb-6">
          <button
            className="w-full flex items-center justify-between px-4 py-3 text-left text-white border-b border-gray-700"
            onClick={() => setIsStatusOpen((prev) => !prev)}
          >
            <div>
              <h2 className="text-lg font-semibold">Status Overview</h2>
              <p className="text-sm text-gray-400">Counts of each stage (click to {isStatusOpen ? 'collapse' : 'expand'})</p>
            </div>
            <ChevronDown
              className={`w-5 h-5 transition-transform ${isStatusOpen ? 'rotate-180' : 'rotate-0'}`}
            />
          </button>
          {isStatusOpen && (
            <div className="p-4">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                {([
                  { key: 'deid', label: 'DEID Status', order: STATUS_ORDER },
                  { key: 'qc', label: 'QC Status', order: STATUS_ORDER },
                  { key: 'embd', label: 'EMBD Status', order: STATUS_ORDER },
                  { key: 'gcp', label: 'GCP Status', order: GCP_STATUS_ORDER },
                ] as const).map(({ key, label, order }) => (
                  <div key={key} className="bg-gray-700 rounded-lg p-4 h-full">
                    <h3 className="text-sm font-semibold text-white mb-3">{label}</h3>
                    <div className="space-y-2">
                      {order.map((status) => (
                        <div key={status} className="flex items-center justify-between text-sm text-gray-300">
                          <span>{status}</span>
                          <span className="font-semibold text-white">{statusSummary[key][status] || 0}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Search and Filter Section */}
        <div className="bg-gray-800 rounded-lg p-4 mb-6">
          <div className="mb-4">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
              <input
                type="text"
                placeholder="Search table name..."
                value={searchTableName}
                onChange={(e) => setSearchTableName(e.target.value)}
                className="w-full pl-10 pr-4 py-2 bg-gray-700 text-white border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
          </div>
          <div className="grid grid-cols-4 gap-4">
            {/* DEID Status Filter */}
            <div className="bg-gray-700 rounded p-3">
              <h3 className="text-sm font-semibold text-white mb-2">Filter by DEID Status</h3>
              <div className="space-y-1">
                {['Not Started', 'Failed', 'In Progress', 'Completed', 'Interrupted'].map((status) => (
                  <label key={status} className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={filters.deid.includes(status)}
                      onChange={() => toggleFilter('deid', status)}
                      className="w-4 h-4 text-blue-600 border-gray-500 rounded"
                    />
                    <span>{status}</span>
                  </label>
                ))}
              </div>
            </div>

            {/* QC Status Filter */}
            <div className="bg-gray-700 rounded p-3">
              <h3 className="text-sm font-semibold text-white mb-2">QC Status</h3>
              <div className="space-y-1">
                {['Not Started', 'In Progress', 'Completed', 'Interrupted', 'Failed'].map((status) => (
                  <label key={status} className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={filters.qc.includes(status)}
                      onChange={() => toggleFilter('qc', status)}
                      className="w-4 h-4 text-blue-600 border-gray-500 rounded"
                    />
                    <span>{status}</span>
                  </label>
                ))}
              </div>
            </div>

            {/* EMBD Status Filter */}
            <div className="bg-gray-700 rounded p-3">
              <h3 className="text-sm font-semibold text-white mb-2">EMBD Status</h3>
              <div className="space-y-1">
                {['Not Started', 'In Progress', 'Completed', 'Interrupted', 'Failed'].map((status) => (
                  <label key={status} className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={filters.embd.includes(status)}
                      onChange={() => toggleFilter('embd', status)}
                      className="w-4 h-4 text-blue-600 border-gray-500 rounded"
                    />
                    <span>{status}</span>
                  </label>
                ))}
              </div>
            </div>

            {/* GCP Status Filter */}
            <div className="bg-gray-700 rounded p-3">
              <h3 className="text-sm font-semibold text-white mb-2">GCP Status</h3>
              <div className="space-y-1">
                {['Not Started', 'In Process', 'Moved', 'Interrupted', 'Failed'].map((status) => (
                  <label key={status} className="flex items-center gap-2 text-sm text-gray-300 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={filters.gcp.includes(status)}
                      onChange={() => toggleFilter('gcp', status)}
                      className="w-4 h-4 text-blue-600 border-gray-500 rounded"
                    />
                    <span>{status}</span>
                  </label>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Main Data Table */}
        <div className="bg-gray-800 rounded-lg shadow overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-700 border-b border-gray-600">
              <tr>
                <th className="text-left py-4 px-4 text-sm font-semibold text-white">Table Name</th>
                <th className="text-left py-4 px-4 text-sm font-semibold text-white">Run Range</th>
                <th className="text-left py-4 px-4 text-sm font-semibold text-white">DEID Status</th>
                <th className="text-left py-4 px-4 text-sm font-semibold text-white">QC Status</th>
                <th className="text-left py-4 px-4 text-sm font-semibold text-white">EMBD Status</th>
                <th className="text-left py-4 px-4 text-sm font-semibold text-white">GCP Status</th>
                <th className="text-left py-4 px-4 text-sm font-semibold text-white">Pipeline State</th>
                <th className="text-left py-4 px-4 text-sm font-semibold text-white">Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={8} className="py-8 px-4 text-center text-gray-400">
                    Loading tables...
                  </td>
                </tr>
              ) : filteredTables.length === 0 ? (
                <tr>
                  <td colSpan={8} className="py-8 px-4 text-center text-gray-400">
                    No tables found
                  </td>
                </tr>
              ) : (
                filteredTables.map((table, index) => {
                  const isFailed = table.pipeline_state === 'Failed'
                  return (
                    <tr
                      key={table.table_id || index}
                      className={`border-b border-gray-700 ${
                        isFailed ? 'bg-red-900/20' : index % 2 === 0 ? 'bg-gray-800' : 'bg-gray-750'
                      }`}
                    >
                      <td className="py-4 px-4">
                        {isFailed ? (
                          <span className="text-red-500 font-semibold">Failed {table.table_name}</span>
                        ) : (
                          <span>{table.table_name}</span>
                        )}
                      </td>
                      <td className="py-4 px-4 text-sm text-gray-300">{table.run_range}</td>
                      <td className="py-4 px-4">
                        <div className="flex items-center gap-2">
                          {getStatusIcon(table.deid_status)}
                          <span className={`text-sm ${getStatusColor(table.deid_status)}`}>{table.deid_status}</span>
                        </div>
                      </td>
                      <td className="py-4 px-4">
                        <div className="flex items-center gap-2">
                          {getStatusIcon(table.qc_status)}
                          <span className={`text-sm ${getStatusColor(table.qc_status)}`}>{table.qc_status}</span>
                        </div>
                      </td>
                      <td className="py-4 px-4">
                        <div className="flex items-center gap-2">
                          {getStatusIcon(table.embd_status)}
                          <span className={`text-sm ${getStatusColor(table.embd_status)}`}>{table.embd_status}</span>
                        </div>
                      </td>
                      <td className="py-4 px-4">
                        <div className="flex items-center gap-2">
                          {getStatusIcon(table.gcp_status)}
                          <span className={`text-sm ${getStatusColor(table.gcp_status)}`}>{table.gcp_status}</span>
                        </div>
                      </td>
                      <td className="py-4 px-4">
                        <div className="flex items-center gap-2">
                          {getStatusIcon(table.pipeline_state)}
                          <span className={`text-sm ${getStatusColor(table.pipeline_state)}`}>{table.pipeline_state}</span>
                        </div>
                      </td>
                      <td className="py-4 px-4">
                        <button className="bg-blue-600 text-white px-3 py-1 rounded text-sm hover:bg-blue-700">
                          View Details
                        </button>
                      </td>
                    </tr>
                  )
                })
              )}
            </tbody>
          </table>
        </div>
      </div>
    </Layout>
  )
}
