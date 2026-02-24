// Get API URL from environment variable, default to localhost
// Note: 0.0.0.0 doesn't work in browsers, use localhost or 127.0.0.1
const getApiUrl = () => {
  const envUrl = process.env.NEXT_PUBLIC_API_URL
  if (envUrl) {
    // Replace 0.0.0.0 with localhost for browser compatibility
    let url = envUrl.replace('0.0.0.0', 'localhost')
    // Ensure URL ends with a slash
    if (!url.endsWith('/')) {
      url += '/'
    }
    return url
  }
  return 'http://localhost:9000/'
}

const API_URL = getApiUrl()

interface ApiResponse<T> {
  success: boolean
  message?: string
  error?: string
  data?: T
  [key: string]: any
}

async function apiRequest<T>(
  endpoint: string,
  options: RequestInit = {}
): Promise<ApiResponse<T>> {
  try {
    // Remove leading slash from endpoint and ensure proper URL construction
    const cleanEndpoint = endpoint.replace(/^\//, '')
    const url = `${API_URL}${cleanEndpoint}`
    console.log('API Request:', url, options.method || 'GET')

    const isFormData = typeof FormData !== 'undefined' && options.body instanceof FormData
    const headers = {
      ...(isFormData ? {} : { 'Content-Type': 'application/json' }),
      ...options.headers,
    }

    const response = await fetch(url, {
      ...options,
      headers,
    })

    if (!response.ok) {
      let errorData
      try {
        errorData = await response.json()
      } catch {
        errorData = { message: `HTTP ${response.status}: ${response.statusText}` }
      }
      console.error('API Error:', errorData)
      return {
        success: false,
        message: errorData.message || 'Request failed',
        error: errorData.error || `HTTP ${response.status}`,
      }
    }

    const data = await response.json()
    console.log('API Response:', data)
    
    return {
      success: true,
      ...data,
    }
  } catch (error) {
    console.error('Network Error:', error)
    return {
      success: false,
      message: 'Network error',
      error: error instanceof Error ? error.message : 'Unknown error',
    }
  }
}

// Config APIs
export const configApi = {
  // Client Run Config
  getClientRunConfig: () => apiRequest('configs/client-run/'),
  saveClientRunConfig: (data: {
    patient_identifier_columns: string[]
    admin_connection_str: string
    nd_patient_start_value: number
    default_offset_value: number
    ehr_type: string
    enable_auto_qc?: boolean
    enable_auto_gcp?: boolean
    enable_auto_embd?: boolean
  }) =>
    apiRequest('configs/client-run/', {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  // Mapping Config
  getMappingConfig: () => apiRequest('configs/mapping/'),
  saveMappingConfig: (data: {
    mapping_config: any
    mapping_schema: string
  }) =>
    apiRequest('configs/mapping/', {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  // Master Table Config
  getMasterTableConfig: () => apiRequest('configs/master-table/'),
  saveMasterTableConfig: (data: {
    pii_tables_config: any
    pii_schema_name: string
  }) =>
    apiRequest('configs/master-table/', {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  // PII Masking Config
  getPIIMaskingConfig: () => apiRequest('configs/pii-masking/'),
  savePIIMaskingConfig: (data: {
    pii_masking_config: any
    secondary_config?: any
  }) =>
    apiRequest('configs/pii-masking/', {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  // QC Config
  getQCConfig: () => apiRequest('configs/qc/'),
  saveQCConfig: (data: { qc_config: any }) =>
    apiRequest('configs/qc/', {
      method: 'POST',
      body: JSON.stringify(data),
    }),
}

// Table Metadata APIs
export const tableMetadataApi = {
  getTableMetadataList: (params?: {
    search?: string
    priority?: number
    is_required?: boolean
    is_phi_marking_done?: boolean
    page?: number
    page_size?: number
  }) => {
    const queryParams = new URLSearchParams()
    if (params) {
      Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null && value !== '') {
          queryParams.append(key, String(value))
        }
      })
    }
    const query = queryParams.toString()
    return apiRequest(`table-metadata/${query ? `?${query}` : ''}`)
  },

  getTableMetadata: (tableId: number) =>
    apiRequest(`table-metadata/${tableId}/`),

  saveTableMetadata: (data: {
    id?: number
    table_name: string
    columns?: any
    primary_key?: any
    max_nd_auto_increment_id?: number
    table_details_for_ui?: any
    run_config?: any
    is_required?: boolean
    priority?: number
    is_phi_marking_done?: boolean
  }) =>
    apiRequest(`table-metadata/${data.id ? `${data.id}/` : ''}`, {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  deleteTableMetadata: (tableId: number) =>
    apiRequest(`table-metadata/${tableId}/`, {
      method: 'DELETE',
    }),

  bulkUpdateTableMetadata: (data: {
    table_ids: number[]
    updates: {
      priority?: number
      is_required?: boolean
      is_phi_marking_done?: boolean
      table_details_for_ui?: any
      run_config?: any
    }
  }) =>
    apiRequest('table-metadata/bulk-update/', {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  exportTableMetadata: () =>
    apiRequest('table-metadata/export/', {
      method: 'GET',
    }),
}

// Queue Management APIs
export const queueApi = {
  startDailyDump: (data?: { queue_name?: string }) =>
    apiRequest('queue-management/start-daily-dump/', {
      method: 'POST',
      body: JSON.stringify(data || {}),
    }),

  getQueues: (params?: {
    search?: string
    status?: number
    page?: number
    page_size?: number
  }) => {
    const queryParams = new URLSearchParams()
    if (params) {
      Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined && value !== null && value !== '') {
          queryParams.append(key, String(value))
        }
      })
    }
    const query = queryParams.toString()
    return apiRequest(`queue-management/queues/${query ? `?${query}` : ''}`)
  },

  getQueueDetail: (queueName: string) =>
    apiRequest(`queue-management/queues/${queueName}/`),

  updateQueueStatus: (queueId: number, queueStatus: number) =>
    apiRequest(`queue-management/queues/${queueId}/update/`, {
      method: 'POST',
      body: JSON.stringify({ queue_status: queueStatus }),
    }),

  bulkUpdateQueueStatus: (data: {
    queue_name: string
    queue_status: number
  }) =>
    apiRequest('queue-management/queues/bulk-update/', {
      method: 'POST',
      body: JSON.stringify(data),
    }),
}

// Monitoring APIs
export const monitoringApi = {
  getAvailableQueues: () => apiRequest('monitoring/queues/'),

  getQueueTables: (queueName: string) =>
    apiRequest(`monitoring/queues/${queueName}/tables/`),
}

// Operations APIs
export const operationsApi = {
  startDeid: (queueId: number, data: { table_ids?: number[]; tables_name?: string[] }) =>
    apiRequest(`operations/deid/start/${queueId}/`, {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  startQc: (queueId: number, data: { tables_id?: number[]; tables_name?: string[] }) =>
    apiRequest(`operations/qc/start/${queueId}/`, {
      method: 'POST',
      body: JSON.stringify(data),
    }),
}

// De-Identification Rules APIs
export const deidRulesApi = {
  getDeidRules: () => apiRequest('deid-rules/'),
}

// Incremental Pipeline Type API
export const incrementalPipelineTypeApi = {
  getType: () => apiRequest('incremental-pipeline/type/'),
}

// Incremental Pipeline APIs (AthenaOne)
export const incrementalPipelineApi = {
  // Get configuration
  getConfig: () => apiRequest('incremental-pipeline/config/'),
  
  // Update configuration
  updateConfig: (data: {
    SNOWFLAKE_USER?: string
    SNOWFLAKE_PASSWORD?: string
    MYSQL_HOST?: string
    EXTRACTION_DATE?: string
    BATCH_SIZE?: number
    MAX_THREADS?: number
    [key: string]: any
  }) =>
    apiRequest('incremental-pipeline/config/', {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  // Start pipeline
  startPipeline: () =>
    apiRequest('incremental-pipeline/control/', {
      method: 'POST',
      body: JSON.stringify({ action: 'start' }),
    }),

  // Stop pipeline
  stopPipeline: () =>
    apiRequest('incremental-pipeline/control/', {
      method: 'POST',
      body: JSON.stringify({ action: 'stop' }),
    }),

  // Get current status
  getStatus: () => apiRequest('incremental-pipeline/status/'),

  // Get logs
  getLogs: (filename?: string) => {
    const params = new URLSearchParams()
    if (filename) params.append('file', filename)
    return apiRequest(`incremental-pipeline/logs/${params.toString() ? `?${params.toString()}` : ''}`)
  },

  // Get history
  getHistory: () => apiRequest('incremental-pipeline/history/'),
  
  // Scheduler APIs
  getSchedulerStatus: () => apiRequest('incremental-pipeline/scheduler/status/'),
  
  enableScheduler: (data: {
    time: string
    timezone: string
  }) =>
    apiRequest('incremental-pipeline/scheduler/enable/', {
      method: 'POST',
      body: JSON.stringify(data),
    }),
  
  disableScheduler: () =>
    apiRequest('incremental-pipeline/scheduler/disable/', {
      method: 'POST',
      body: JSON.stringify({}),
    }),
  
  // Config Editor API
  saveConfig: (configContent: string) =>
    apiRequest('incremental-pipeline/config/save/', {
      method: 'POST',
      body: JSON.stringify({ config_content: configContent }),
    }),
}

// ECW with Diff Pipeline APIs
export const ecwPipelineApi = {
  // Get configuration
  getConfig: () => apiRequest('ecw-with-diff-pipeline/config/'),
  
  // Update configuration
  updateConfig: (data: any) =>
    apiRequest('ecw-with-diff-pipeline/config/', {
      method: 'POST',
      body: JSON.stringify({ run_config: data }),
    }),

  // Start pipeline
  startPipeline: () =>
    apiRequest('ecw-with-diff-pipeline/control/', {
      method: 'POST',
      body: JSON.stringify({ action: 'start' }),
    }),

  // Stop pipeline
  stopPipeline: () =>
    apiRequest('ecw-with-diff-pipeline/control/', {
      method: 'POST',
      body: JSON.stringify({ action: 'stop' }),
    }),

  // Get current status
  getStatus: () => apiRequest('ecw-with-diff-pipeline/status/'),

  // Get logs
  getLogs: (filename?: string) => {
    const params = new URLSearchParams()
    if (filename) params.append('file', filename)
    return apiRequest(`ecw-with-diff-pipeline/logs/${params.toString() ? `?${params.toString()}` : ''}`)
  },

  // Get history
  getHistory: () => apiRequest('ecw-with-diff-pipeline/history/'),
  
  // Scheduler APIs
  getSchedulerStatus: () => apiRequest('ecw-with-diff-pipeline/scheduler/status/'),
  
  enableScheduler: (data: {
    time: string
    timezone: string
  }) =>
    apiRequest('ecw-with-diff-pipeline/scheduler/enable/', {
      method: 'POST',
      body: JSON.stringify(data),
    }),
  
  disableScheduler: () =>
    apiRequest('ecw-with-diff-pipeline/scheduler/disable/', {
      method: 'POST',
      body: JSON.stringify({}),
    }),
  
  // Config Editor API
  saveConfig: (configContent: string) =>
    apiRequest('ecw-with-diff-pipeline/config/save/', {
      method: 'POST',
      body: JSON.stringify({ config_content: configContent }),
    }),
}

// PHI Marking APIs
export const phiMarkingApi = {
  uploadFromCsv: async (file: File) => {
    const formData = new FormData()
    formData.append('file', file)
    try {
      const response = await fetch(`${API_URL}phi-marking/upload/`, {
        method: 'POST',
        body: formData,
      })
      if (!response.ok) {
        const errorText = await response.text()
        return { success: false, message: errorText || 'Upload failed' }
      }
      const data = await response.json()
      return { success: true, ...data }
    } catch (error) {
      return {
        success: false,
        message: error instanceof Error ? error.message : 'Unknown error',
      }
    }
  },

  downloadCsv: async () => {
    try {
      const response = await fetch(`${API_URL}phi-marking/download/`)
      if (!response.ok) {
        const errorText = await response.text()
        return { success: false, message: errorText || 'Download failed' }
      }
      const blob = await response.blob()
      const disposition = response.headers.get('content-disposition')
      let filename = 'phi_marking.csv'
      if (disposition) {
        const match = disposition.match(/filename="?([^"]+)"?/)
        if (match && match[1]) {
          filename = match[1]
        }
      }
      return { success: true, blob, filename }
    } catch (error) {
      return {
        success: false,
        message: error instanceof Error ? error.message : 'Unknown error',
      }
    }
  },
}

