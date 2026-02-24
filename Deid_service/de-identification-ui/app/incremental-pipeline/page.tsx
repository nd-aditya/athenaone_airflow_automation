'use client'

import { useState, useEffect, useRef } from 'react'
import Layout from '@/components/Layout'
import { CheckCircle, Play, Circle, Download, RefreshCw, PlayCircle, StopCircle, AlertCircle, Clock, Edit2, Save, X } from 'lucide-react'
import { incrementalPipelineApi, ecwPipelineApi, incrementalPipelineTypeApi } from '@/lib/api'

interface WorkflowStep {
  id: number
  name: string
  status: 'completed' | 'active' | 'pending' | 'failed'
}

interface PipelineStatus {
  status: 'idle' | 'running' | 'completed' | 'failed'
  current_step: number | null
  total_steps: number
  step_name?: string
  progress: number
  logs: string[]
  log_file?: string
  is_running?: boolean
}

interface PipelineHistory {
  date: string
  status: string
  file: string
  size: number
  modified: string
  lines?: number
}

interface SchedulerStatus {
  enabled: boolean
  time: string
  timezone: string
  last_run: string | null
  next_run: string | null
  jobs_count: number
}

export default function IncrementalPipeline() {
  const [processType, setProcessType] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'logs' | 'scheduler' | 'configuration' | 'history'>('logs')
  const [logs, setLogs] = useState<string[]>([])
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatus>({
    status: 'idle',
    current_step: null,
    total_steps: 9,
    progress: 0,
    logs: []
  })
  const [config, setConfig] = useState<any>(null)
  const [configContent, setConfigContent] = useState<string>('')
  const [isEditingConfig, setIsEditingConfig] = useState(false)
  const [history, setHistory] = useState<PipelineHistory[]>([])
  const [schedulerStatus, setSchedulerStatus] = useState<SchedulerStatus>({
    enabled: false,
    time: '02:00',
    timezone: 'UTC',
    last_run: null,
    next_run: null,
    jobs_count: 0
  })
  const [scheduleTime, setScheduleTime] = useState('02:00')
  const [scheduleTimezone, setScheduleTimezone] = useState('UTC')
  const [loading, setLoading] = useState(false)
  const [autoRefresh, setAutoRefresh] = useState(true)
  const intervalRef = useRef<NodeJS.Timeout | null>(null)
  const logsEndRef = useRef<HTMLDivElement>(null)

  // Get the appropriate API based on process type
  const getApi = () => {
    if (processType === 'ecw_with_diff_script') {
      return ecwPipelineApi
    }
    return incrementalPipelineApi
  }

  // Workflow steps definition - different for each process type
  const getWorkflowSteps = (): WorkflowStep[] => {
    if (processType === 'ecw_with_diff_script') {
      return [
        { id: 1, name: 'Find Incremental Diff', status: 'pending' },
        { id: 2, name: 'Merge Incremental Diff', status: 'pending' },
      ]
    }
    // AthenaOne default
    return [
      { id: 1, name: 'Create Schema', status: 'pending' },
      { id: 2, name: 'Extract Data', status: 'pending' },
      { id: 3, name: 'Fix Two Tables', status: 'pending' },
      { id: 4, name: 'Extract Two Tables', status: 'pending' },
      { id: 5, name: 'Appointment Alter', status: 'pending' },
      { id: 6, name: 'Scheduling Schema', status: 'pending' },
      { id: 7, name: 'Financials Schema', status: 'pending' },
      { id: 8, name: 'Add Date Column', status: 'pending' },
      { id: 9, name: 'Merge Data', status: 'pending' },
    ]
  }

  const workflowSteps = getWorkflowSteps()

  // Common timezones
  const timezones = [
    'UTC',
    'America/New_York',
    'America/Chicago',
    'America/Los_Angeles',
    'Europe/London',
    'Europe/Paris',
    'Asia/Kolkata',
    'Asia/Tokyo',
    'Asia/Shanghai',
    'Australia/Sydney',
  ]

  // Update step statuses based on current progress
  const getUpdatedSteps = (): WorkflowStep[] => {
    return workflowSteps.map(step => {
      if (pipelineStatus.current_step === null) {
        return { ...step, status: 'pending' }
      }
      
      if (pipelineStatus.status === 'failed' && step.id === pipelineStatus.current_step) {
        return { ...step, status: 'failed' }
      }
      
      if (step.id < pipelineStatus.current_step) {
        return { ...step, status: 'completed' }
      } else if (step.id === pipelineStatus.current_step) {
        return { ...step, status: pipelineStatus.status === 'running' ? 'active' : 'completed' }
      } else {
        return { ...step, status: 'pending' }
      }
    })
  }

  // Load process type
  const loadProcessType = async () => {
    try {
      const response = await incrementalPipelineTypeApi.getType()
      if (response.success) {
        setProcessType(response.incremental_process_type)
        // Update total steps based on type
        if (response.incremental_process_type === 'ecw_with_diff_script') {
          setPipelineStatus(prev => ({ ...prev, total_steps: 2 }))
        } else {
          setPipelineStatus(prev => ({ ...prev, total_steps: 9 }))
        }
      }
    } catch (error) {
      console.error('Error loading process type:', error)
    }
  }

  // Load pipeline status
  const loadStatus = async () => {
    if (!processType) return
    try {
      const api = getApi()
      const response = await api.getStatus()
      if (response.success) {
        setPipelineStatus(response as PipelineStatus)
        setLogs(response.logs || [])
      }
    } catch (error) {
      console.error('Error loading status:', error)
    }
  }

  // Load configuration
  const loadConfig = async () => {
    if (!processType) return
    try {
      const api = getApi()
      const response = await api.getConfig()
      if (response.success) {
        setConfig(response.config)
        // Format config as JSON string for editing
        if (response.config) {
          setConfigContent(JSON.stringify(response.config, null, 2))
        }
      }
    } catch (error) {
      console.error('Error loading config:', error)
    }
  }

  // Load history
  const loadHistory = async () => {
    if (!processType) return
    try {
      const api = getApi()
      const response = await api.getHistory()
      if (response.success) {
        setHistory(response.history || [])
      }
    } catch (error) {
      console.error('Error loading history:', error)
    }
  }

  // Load scheduler status
  const loadSchedulerStatus = async () => {
    if (!processType) return
    try {
      const api = getApi()
      const response = await api.getSchedulerStatus()
      if (response.success) {
        setSchedulerStatus(response as SchedulerStatus)
        setScheduleTime(response.time || '02:00')
        setScheduleTimezone(response.timezone || 'UTC')
      }
    } catch (error) {
      console.error('Error loading scheduler status:', error)
    }
  }

  // Start pipeline
  const handleStartPipeline = async () => {
    if (!processType) return
    const stepCount = processType === 'ecw_with_diff_script' ? '2-step' : '9-step'
    if (!confirm(`Start the incremental data pipeline? This will begin the ${stepCount} extraction process.`)) {
      return
    }
    
    setLoading(true)
    try {
      const api = getApi()
      const response = await api.startPipeline()
      if (response.success) {
        alert('Pipeline started successfully! Check the logs tab for real-time progress.')
        setAutoRefresh(true)
        setActiveTab('logs')
        setTimeout(() => {
          loadStatus()
        }, 2000)
      } else {
        alert('Failed to start pipeline: ' + (response.message || 'Unknown error'))
      }
    } catch (error) {
      alert('Error starting pipeline: ' + (error instanceof Error ? error.message : 'Unknown error'))
    } finally {
      setLoading(false)
    }
  }

  // Stop pipeline
  const handleStopPipeline = async () => {
    if (!processType) return
    if (!confirm('Are you sure you want to stop the running pipeline? This may leave data in an incomplete state.')) {
      return
    }
    
    setLoading(true)
    try {
      const api = getApi()
      const response = await api.stopPipeline()
      if (response.success) {
        alert('Pipeline stopped successfully')
        loadStatus()
      } else {
        alert('Failed to stop pipeline: ' + (response.message || 'Pipeline may not be running'))
      }
    } catch (error) {
      alert('Error stopping pipeline: ' + (error instanceof Error ? error.message : 'Unknown error'))
    } finally {
      setLoading(false)
    }
  }

  // Enable scheduler
  const handleEnableScheduler = async () => {
    if (!processType) return
    setLoading(true)
    try {
      const api = getApi()
      const response = await api.enableScheduler({
        time: scheduleTime,
        timezone: scheduleTimezone
      })
      if (response.success) {
        alert(`Scheduler enabled! Next run: ${response.next_run}`)
        loadSchedulerStatus()
      } else {
        alert('Failed to enable scheduler: ' + (response.message || response.error))
      }
    } catch (error) {
      alert('Error enabling scheduler: ' + (error instanceof Error ? error.message : 'Unknown error'))
    } finally {
      setLoading(false)
    }
  }

  // Disable scheduler
  const handleDisableScheduler = async () => {
    if (!processType) return
    if (!confirm('Disable the scheduler? Automated runs will stop.')) {
      return
    }
    
    setLoading(true)
    try {
      const api = getApi()
      const response = await api.disableScheduler()
      if (response.success) {
        alert('Scheduler disabled successfully')
        loadSchedulerStatus()
      } else {
        alert('Failed to disable scheduler: ' + (response.message || 'Unknown error'))
      }
    } catch (error) {
      alert('Error disabling scheduler: ' + (error instanceof Error ? error.message : 'Unknown error'))
    } finally {
      setLoading(false)
    }
  }

  // Save config
  const handleSaveConfig = async () => {
    if (!processType) return
    if (!confirm('Save configuration? Changes will be applied immediately.')) {
      return
    }
    
    // Validate JSON before saving
    try {
      JSON.parse(configContent)
    } catch (error) {
      alert('Invalid JSON format. Please check your configuration.')
      return
    }
    
    setLoading(true)
    try {
      const api = getApi()
      const response = await api.saveConfig(configContent)
      if (response.success) {
        alert('Configuration saved successfully!')
        setIsEditingConfig(false)
        loadConfig()
      } else {
        alert('Failed to save config: ' + (response.error || 'Unknown error'))
      }
    } catch (error) {
      alert('Error saving config: ' + (error instanceof Error ? error.message : 'Unknown error'))
    } finally {
      setLoading(false)
    }
  }

  // Download logs
  const handleDownloadLogs = async () => {
    if (!processType) return
    try {
      const api = getApi()
      const response = await api.getLogs()
      if (response.success && response.logs) {
        const logContent = response.logs.join('\n')
        const blob = new Blob([logContent], { type: 'text/plain' })
        const url = window.URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = response.file || `pipeline_logs_${new Date().toISOString()}.txt`
        a.click()
        window.URL.revokeObjectURL(url)
      } else {
        alert('No logs available to download')
      }
    } catch (error) {
      alert('Error downloading logs: ' + (error instanceof Error ? error.message : 'Unknown error'))
    }
  }

  // Auto-scroll logs to bottom
  useEffect(() => {
    if (logsEndRef.current && activeTab === 'logs') {
      logsEndRef.current.scrollIntoView({ behavior: 'smooth' })
    }
  }, [logs, activeTab])

  // Auto-refresh when pipeline is running
  useEffect(() => {
    if (autoRefresh && (pipelineStatus.status === 'running' || pipelineStatus.is_running)) {
      // Refresh more frequently when pipeline is running
      intervalRef.current = setInterval(() => {
        loadStatus()
      }, 2000) // Refresh every 2 seconds for better real-time updates
    } else {
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
        intervalRef.current = null
      }
    }

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current)
      }
    }
  }, [autoRefresh, pipelineStatus.status, pipelineStatus.is_running])

  // Initial load
  useEffect(() => {
    const initialize = async () => {
      await loadProcessType()
    }
    initialize()
  }, [])

  // Reload data when process type changes
  useEffect(() => {
    if (processType) {
      loadStatus()
      loadConfig()
      loadHistory()
      loadSchedulerStatus()
    }
  }, [processType])

  const updatedSteps = getUpdatedSteps()
  const estimatedTime = pipelineStatus.status === 'running' ? '~14 minutes remaining' : 
                        pipelineStatus.status === 'completed' ? 'Completed' : 
                        pipelineStatus.status === 'failed' ? 'Failed' : 
                        'Not running'

  const getStepIcon = (step: WorkflowStep) => {
    if (step.status === 'completed') {
      return <CheckCircle className="w-5 h-5 text-green-500" />
    } else if (step.status === 'active') {
      return <Play className="w-5 h-5 text-blue-500 animate-pulse" />
    } else if (step.status === 'failed') {
      return <AlertCircle className="w-5 h-5 text-red-500" />
    } else {
      return <Circle className="w-5 h-5 text-gray-400" />
    }
  }

  const getStatusBadge = () => {
    const statusColors = {
      running: 'bg-blue-100 text-blue-700',
      completed: 'bg-green-100 text-green-700',
      failed: 'bg-red-100 text-red-700',
      idle: 'bg-gray-100 text-gray-700'
    }
    
    return (
      <span className={`px-3 py-1 rounded-full text-sm font-semibold ${statusColors[pipelineStatus.status]}`}>
        {pipelineStatus.status.toUpperCase()}
      </span>
    )
  }

  // Show loading state if process type hasn't been loaded
  if (processType === null) {
    return (
      <Layout>
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <RefreshCw className="w-8 h-8 animate-spin text-blue-600 mx-auto mb-4" />
            <p className="text-gray-600">Loading pipeline configuration...</p>
          </div>
        </div>
      </Layout>
    )
  }

  return (
    <Layout>
      <div>
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-800">Incremental Pipeline</h1>
            <p className="text-gray-600 mt-1">
              {processType === 'ecw_with_diff_script' 
                ? 'ECW incremental diff find and merge' 
                : 'Snowflake to MySQL data extraction and merge'}
            </p>
          </div>
          
          <div className="flex gap-2 items-center">
            <label className="flex items-center gap-2 text-sm text-gray-600">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
                className="w-4 h-4"
              />
              Auto-refresh
            </label>
            
            <button
              onClick={loadStatus}
              disabled={loading}
              className="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors disabled:opacity-50 flex items-center gap-2"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
            
            <button
              onClick={handleStartPipeline}
              disabled={loading || pipelineStatus.status === 'running' || pipelineStatus.is_running}
              className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
            >
              <PlayCircle className="w-4 h-4" />
              Start Pipeline
            </button>
            
            <button
              onClick={handleStopPipeline}
              disabled={loading || (pipelineStatus.status !== 'running' && !pipelineStatus.is_running)}
              className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-2"
            >
              <StopCircle className="w-4 h-4" />
              Stop Pipeline
            </button>
          </div>
        </div>

        {/* Pipeline Progress Section */}
        <div className="bg-white rounded-lg shadow p-6 mb-6">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h2 className="text-2xl font-bold text-gray-800 mb-2">Pipeline Progress</h2>
              <p className="text-3xl font-bold text-blue-600">{pipelineStatus.progress}%</p>
            </div>
            <div className="text-right">
              <p className="text-sm text-gray-600 mb-1">Status</p>
              {getStatusBadge()}
            </div>
          </div>

          {/* Workflow Steps */}
          <div className="mb-4">
            <div className="flex items-center gap-2 overflow-x-auto pb-4">
              {updatedSteps.map((step, index) => (
                <div key={index} className="flex items-center gap-2 flex-shrink-0">
                  <div className="flex flex-col items-center gap-1">
                    {getStepIcon(step)}
                    <span className={`text-xs text-center ${
                      step.status === 'active' ? 'text-blue-600 font-semibold' : 
                      step.status === 'completed' ? 'text-green-600' : 
                      step.status === 'failed' ? 'text-red-600' :
                      'text-gray-500'
                    }`}>
                      {step.name}
                    </span>
                  </div>
                  {index < updatedSteps.length - 1 && (
                    <div className={`w-8 h-0.5 ${step.status === 'completed' ? 'bg-green-500' : 'bg-gray-300'}`}></div>
                  )}
                </div>
              ))}
            </div>
          </div>

          {/* Progress Bar */}
          <div className="mb-4">
            <div className="w-full bg-gray-200 rounded-full h-3">
              <div
                className={`h-3 rounded-full transition-all duration-300 ${
                  pipelineStatus.status === 'failed' ? 'bg-red-600' : 
                  pipelineStatus.status === 'completed' ? 'bg-green-600' : 
                  'bg-blue-600'
                }`}
                style={{ width: `${pipelineStatus.progress}%` }}
              ></div>
            </div>
            <div className="flex justify-between items-center mt-2">
              <p className="text-sm text-gray-600">{estimatedTime}</p>
              {pipelineStatus.log_file && (
                <p className="text-xs text-gray-500 font-mono">{pipelineStatus.log_file}</p>
              )}
            </div>
          </div>

          {/* Current Step Info */}
          {pipelineStatus.current_step && pipelineStatus.step_name && (
          <div className="bg-gray-50 rounded p-3">
              <p className="text-sm text-gray-700 font-mono">
                <Clock className="inline w-4 h-4 mr-2" />
                Step {pipelineStatus.current_step}/{pipelineStatus.total_steps}: {pipelineStatus.step_name}
              </p>
          </div>
          )}
        </div>

        {/* Statistics Cards */}
        <div className="grid grid-cols-4 gap-6 mb-6">
          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-sm font-medium text-gray-600 mb-2">Current Step</h3>
            <p className="text-2xl font-bold text-gray-800">
              {pipelineStatus.current_step || 0} / {pipelineStatus.total_steps}
            </p>
            </div>
          
          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-sm font-medium text-gray-600 mb-2">Total Runs</h3>
            <p className="text-2xl font-bold text-gray-800">{history.length}</p>
          </div>

          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-sm font-medium text-gray-600 mb-2">Log Lines</h3>
            <p className="text-2xl font-bold text-gray-800">{logs.length}</p>
              </div>
          
          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-sm font-medium text-gray-600 mb-2">Scheduler</h3>
            <p className={`text-2xl font-bold ${schedulerStatus.enabled ? 'text-green-600' : 'text-gray-400'}`}>
              {schedulerStatus.enabled ? 'Enabled' : 'Disabled'}
            </p>
          </div>
        </div>

        {/* Tabs Section */}
        <div className="bg-white rounded-lg shadow">
          <div className="border-b border-gray-200">
            <nav className="flex -mb-px">
              {[
                { id: 'logs', label: 'Live Logs' },
                { id: 'scheduler', label: 'Scheduler' },
                { id: 'configuration', label: 'Configuration' },
                { id: 'history', label: 'Run History' },
              ].map((tab) => (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id as typeof activeTab)}
                  className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors ${
                    activeTab === tab.id
                      ? 'border-blue-600 text-blue-600'
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                  }`}
                >
                  {tab.label}
                </button>
              ))}
            </nav>
          </div>

          <div className="p-6">
            {/* Live Logs Tab */}
            {activeTab === 'logs' && (
              <div>
                <div className="flex items-center justify-between mb-2">
                  <p className="text-sm text-gray-600">
                    {pipelineStatus.is_running ? (
                      <span className="text-blue-600 font-semibold">● Live</span>
                    ) : pipelineStatus.status === 'running' ? (
                      <span className="text-blue-600 font-semibold">● Running</span>
                    ) : (
                      <span className="text-gray-500">Logs</span>
                    )}
                  </p>
                  {pipelineStatus.log_file && (
                    <p className="text-xs text-gray-500 font-mono">{pipelineStatus.log_file}</p>
                  )}
                </div>
                <div className="bg-gray-900 rounded-lg p-4 mb-4 font-mono text-sm text-green-400 max-h-96 overflow-y-auto">
                  {logs.length > 0 ? (
                    <>
                      {logs.map((log, index) => (
                        <div key={index} className="mb-1 hover:bg-gray-800 px-1 whitespace-pre-wrap break-words">
                          {log || ' '}
                        </div>
                      ))}
                      <div ref={logsEndRef} />
                    </>
                  ) : (
                    <div className="text-gray-500 text-center py-8">
                      {pipelineStatus.status === 'idle' 
                        ? 'No logs available. Start the pipeline to see logs.' 
                        : 'Loading logs...'}
                    </div>
                  )}
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={loadStatus}
                    className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors text-sm flex items-center gap-2"
                  >
                    <RefreshCw className="w-4 h-4" />
                    Refresh Logs
                  </button>
                  <button
                    onClick={handleDownloadLogs}
                    disabled={logs.length === 0}
                    className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors text-sm flex items-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <Download className="w-4 h-4" />
                    Download Full Log
                  </button>
                </div>
              </div>
            )}

            {/* Scheduler Tab */}
            {activeTab === 'scheduler' && (
              <div>
                <h3 className="text-lg font-semibold text-gray-800 mb-4">Automated Pipeline Scheduling</h3>
                
                <div className="grid grid-cols-2 gap-6 mb-6">
                  <div className="bg-gray-50 p-4 rounded-lg">
                    <h4 className="font-semibold text-gray-700 mb-3">Scheduler Status</h4>
                    <div className="space-y-2">
                      <div className="flex justify-between">
                        <span className="text-gray-600">Status:</span>
                        <span className={`font-semibold ${schedulerStatus.enabled ? 'text-green-600' : 'text-gray-400'}`}>
                          {schedulerStatus.enabled ? 'Enabled' : 'Disabled'}
                        </span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-gray-600">Scheduled Time:</span>
                        <span className="font-mono">{schedulerStatus.time} {schedulerStatus.timezone}</span>
                      </div>
                      {schedulerStatus.next_run && (
                        <div className="flex justify-between">
                          <span className="text-gray-600">Next Run:</span>
                          <span className="font-mono text-sm">{new Date(schedulerStatus.next_run).toLocaleString()}</span>
                        </div>
                      )}
                      {schedulerStatus.last_run && (
                        <div className="flex justify-between">
                          <span className="text-gray-600">Last Run:</span>
                          <span className="font-mono text-sm">{new Date(schedulerStatus.last_run).toLocaleString()}</span>
                        </div>
                      )}
                    </div>
                  </div>

                  <div className="bg-gray-50 p-4 rounded-lg">
                    <h4 className="font-semibold text-gray-700 mb-3">Schedule Configuration</h4>
                    <div className="space-y-3">
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Time (24-hour)</label>
                        <input
                          type="time"
                          value={scheduleTime}
                          onChange={(e) => setScheduleTime(e.target.value)}
                          disabled={schedulerStatus.enabled}
                          className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
                        />
                      </div>
                      <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">Timezone</label>
                        <select
                          value={scheduleTimezone}
                          onChange={(e) => setScheduleTimezone(e.target.value)}
                          disabled={schedulerStatus.enabled}
                          className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          {timezones.map(tz => (
                            <option key={tz} value={tz}>{tz}</option>
                          ))}
                        </select>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="flex gap-3">
                  {schedulerStatus.enabled ? (
                    <button
                      onClick={handleDisableScheduler}
                      disabled={loading}
                      className="bg-red-600 text-white px-6 py-3 rounded-lg hover:bg-red-700 transition-colors disabled:opacity-50 flex items-center gap-2"
                    >
                      <StopCircle className="w-5 h-5" />
                      Disable Scheduler
                    </button>
                  ) : (
                    <button
                      onClick={handleEnableScheduler}
                      disabled={loading}
                      className="bg-green-600 text-white px-6 py-3 rounded-lg hover:bg-green-700 transition-colors disabled:opacity-50 flex items-center gap-2"
                    >
                      <PlayCircle className="w-5 h-5" />
                      Enable Scheduler
                    </button>
                  )}
                  <button
                    onClick={loadSchedulerStatus}
                    className="bg-gray-600 text-white px-6 py-3 rounded-lg hover:bg-gray-700 transition-colors flex items-center gap-2"
                  >
                    <RefreshCw className="w-5 h-5" />
                    Refresh Status
                  </button>
                </div>

                <div className="mt-6 p-4 bg-blue-50 rounded-lg border border-blue-200">
                  <p className="text-sm text-blue-800">
                    <strong>Note:</strong> The scheduler will automatically run the pipeline daily at the specified time. 
                    The pipeline must complete before the next scheduled run. System timezone is used for scheduling.
                  </p>
                </div>
              </div>
            )}

            {/* Configuration Tab */}
            {activeTab === 'configuration' && (
              <div>
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-lg font-semibold text-gray-800">Pipeline Configuration</h3>
                  <div className="flex gap-2">
                    {isEditingConfig ? (
                      <>
                        <button
                          onClick={handleSaveConfig}
                          disabled={loading}
                          className="bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 transition-colors flex items-center gap-2 text-sm disabled:opacity-50"
                        >
                          <Save className="w-4 h-4" />
                          Save Changes
                        </button>
                        <button
                          onClick={() => {
                            setIsEditingConfig(false)
                            loadConfig()
                          }}
                          className="bg-gray-600 text-white px-4 py-2 rounded-lg hover:bg-gray-700 transition-colors flex items-center gap-2 text-sm"
                        >
                          <X className="w-4 h-4" />
                          Cancel
                        </button>
                      </>
                    ) : (
                      <button
                        onClick={() => setIsEditingConfig(true)}
                        className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2 text-sm"
                      >
                        <Edit2 className="w-4 h-4" />
                        Edit Configuration
                      </button>
                    )}
                  </div>
                </div>
                
                {isEditingConfig ? (
                  <div>
                    <textarea
                      value={configContent}
                      onChange={(e) => setConfigContent(e.target.value)}
                      rows={30}
                      className="w-full px-4 py-3 font-mono text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 bg-gray-900 text-green-400 resize-none"
                      placeholder="Edit configuration as JSON..."
                      spellCheck={false}
                      style={{ maxWidth: '100%' }}
                    />
                    <div className="mt-2 space-y-1">
                      <p className="text-sm text-orange-600">
                        ⚠️ Be careful when editing! Invalid JSON may break the pipeline.
                      </p>
                      <p className="text-xs text-gray-500">
                        Configuration structure: source_connection_details, destination_connection_details, schemas, extraction_settings, notification_settings, date_settings, machine_name
                      </p>
                    </div>
                  </div>
                ) : configContent ? (
                  <div className="max-w-full overflow-hidden">
                    <pre className="bg-gray-900 text-green-400 p-4 rounded-lg font-mono text-sm max-h-96 overflow-y-auto whitespace-pre-wrap break-words" style={{ maxWidth: '100%', wordBreak: 'break-all', overflowX: 'hidden' }}>
                      {configContent}
                    </pre>
                    <p className="text-sm text-gray-500 mt-2 italic">
                      Click "Edit Configuration" to modify settings
                    </p>
                  </div>
                ) : (
                  <p className="text-gray-600">Loading configuration...</p>
                )}
              </div>
            )}

            {/* History Tab */}
            {activeTab === 'history' && (
              <div>
                <h3 className="text-lg font-semibold text-gray-800 mb-4">Pipeline Execution History</h3>
                {history.length > 0 ? (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead className="bg-gray-50 border-b border-gray-200">
                        <tr>
                          <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Date</th>
                          <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Status</th>
                          <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">File</th>
                          <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Lines</th>
                          <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Size</th>
                        </tr>
                      </thead>
                      <tbody>
                        {history.map((run, idx) => (
                          <tr key={idx} className="border-b border-gray-100 hover:bg-gray-50">
                            <td className="py-3 px-4 text-sm">{run.date}</td>
                            <td className="py-3 px-4 text-sm">
                              <span className={`px-2 py-1 rounded text-xs font-semibold ${
                                run.status === 'completed' ? 'bg-green-100 text-green-700' :
                                run.status === 'failed' ? 'bg-red-100 text-red-700' :
                                'bg-gray-100 text-gray-700'
                              }`}>
                                {run.status}
                              </span>
                            </td>
                            <td className="py-3 px-4 text-sm font-mono text-gray-600">{run.file}</td>
                            <td className="py-3 px-4 text-sm">{run.lines?.toLocaleString() || 'N/A'}</td>
                            <td className="py-3 px-4 text-sm">{(run.size / 1024).toFixed(2)} KB</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <div className="text-center py-8 text-gray-500">
                    No pipeline execution history found
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      </div>
    </Layout>
  )
}
