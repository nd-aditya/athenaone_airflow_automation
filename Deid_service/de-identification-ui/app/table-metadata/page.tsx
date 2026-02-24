'use client'

import { useState, useEffect, useRef } from 'react'
import { useRouter } from 'next/navigation'
import Layout from '@/components/Layout'
import { ArrowLeft, Plus, Search, Upload, X, Edit2 } from 'lucide-react'
import { tableMetadataApi, deidRulesApi, phiMarkingApi } from '@/lib/api'

interface ColumnDetail {
  is_phi: boolean
  mask_value: string | null
  column_name: string
  ignore_rows: Record<string, any>
  de_identification_rule: string | null
  column_name_for_phi_table: string | null
}

interface TableConfig {
  batch_size: number
  ignore_rows: Record<string, any>
  columns_details: ColumnDetail[]
  reference_mapping: Record<string, any>
  patient_identifier_type: string | null
  patient_identifier_column: string | null
}

interface TableMetadata {
  id: string
  table_name: string
  priority: number
  is_required: boolean
  is_phi_marking_done: boolean
  run_config: Record<string, any>
  table_details_for_ui: Record<string, any>
  config: TableConfig
}

export default function TableMetadata() {
  const router = useRouter()
  const [viewMode, setViewMode] = useState<'list' | 'edit'>('list')
  const [selectedTableId, setSelectedTableId] = useState<string | null>(null)
  const [tables, setTables] = useState<TableMetadata[]>([])
  const [loading, setLoading] = useState(false)
  const [currentPage, setCurrentPage] = useState(1)
  const [totalPages, setTotalPages] = useState(1)
  const [totalCount, setTotalCount] = useState(0)
  const [pageSize] = useState(50)

  const [searchQuery, setSearchQuery] = useState('')
  const [filterPriority, setFilterPriority] = useState<string>('all')
  const [filterRequired, setFilterRequired] = useState<string>('all')
  const [filterConfigStatus, setFilterConfigStatus] = useState<string>('all')
  const [uploadingPhi, setUploadingPhi] = useState(false)
  const [downloadingPhi, setDownloadingPhi] = useState(false)
  const uploadInputRef = useRef<HTMLInputElement | null>(null)

  useEffect(() => {
    loadTables()
  }, [currentPage, searchQuery, filterPriority, filterRequired, filterConfigStatus])

  const loadTables = async () => {
    setLoading(true)
    try {
      const params: any = {
        page: currentPage,
        page_size: pageSize,
      }
      if (searchQuery) params.search = searchQuery
      if (filterPriority !== 'all') params.priority = parseInt(filterPriority)
      if (filterRequired !== 'all') params.is_required = filterRequired === 'yes'
      if (filterConfigStatus !== 'all') {
        params.is_phi_marking_done = filterConfigStatus === 'ui_set'
      }

      const response = await tableMetadataApi.getTableMetadataList(params)
      if (response.success && response.results) {
        // Transform API response to match our interface
        const transformedTables = response.results.map((item: any) => ({
          id: String(item.id),
          table_name: item.table_name,
          priority: item.priority,
          is_required: item.is_required,
          is_phi_marking_done: item.is_phi_marking_done,
          run_config: {},
          table_details_for_ui: {},
          config: {
            batch_size: 1000,
            ignore_rows: {},
            columns_details: [],
            reference_mapping: {},
            patient_identifier_type: null,
            patient_identifier_column: null,
          },
          pii_status: item.pii_status,
          updated_at: item.updated_at,
        }))
        setTables(transformedTables)
        setTotalPages(response.total_pages || 1)
        setTotalCount(response.count || 0)
      }
    } catch (error) {
      console.error('Error loading tables:', error)
    } finally {
      setLoading(false)
    }
  }

  const selectedTable = tables.find((t) => t.id === selectedTableId)

  const getPIIStatus = (table: TableMetadata) => {
    // Use pii_status from API if available, otherwise calculate from config
    if ('pii_status' in table && (table as any).pii_status) {
      return (table as any).pii_status
    }
    const phiColumns = table.config.columns_details.filter((c) => c.is_phi).length
    const totalColumns = table.config.columns_details.length
    if (phiColumns === 0) return 'Not PII'
    if (phiColumns === totalColumns) return 'All PII'
    return `${phiColumns} PII Columns`
  }

  const handleEditTable = async (tableId: string) => {
    setLoading(true)
    try {
      const response = await tableMetadataApi.getTableMetadata(parseInt(tableId))
      if (response.success) {
        // Extract columns_details from table_details_for_ui if present
        let columnsDetails: ColumnDetail[] = []
        
        // Get columns_details from table_details_for_ui (this is the source of truth)
        if (response.table_details_for_ui && typeof response.table_details_for_ui === 'object') {
          if (Array.isArray(response.table_details_for_ui.columns_details)) {
            columnsDetails = response.table_details_for_ui.columns_details.map((col: any) => ({
              is_phi: col.is_phi || false,
              mask_value: col.mask_value || null,
              column_name: col.column_name || '',
              ignore_rows: col.ignore_rows || {},
              de_identification_rule: col.de_identification_rule || null,
              column_name_for_phi_table: col.column_name_for_phi_table || null,
            }))
          }
        }
        
        // If no columns_details found in table_details_for_ui, try to extract from columns dict
        if (columnsDetails.length === 0 && response.columns && typeof response.columns === 'object') {
          columnsDetails = Object.entries(response.columns).map(([columnName, col]: [string, any]) => ({
            is_phi: col?.is_phi || false,
            mask_value: col?.mask_value || null,
            column_name: columnName,
            ignore_rows: col?.ignore_rows || {},
            de_identification_rule: col?.de_identification_rule || null,
            column_name_for_phi_table: col?.column_name_for_phi_table || null,
          }))
        }
        
        // Transform API response to match our interface
        // table_details_for_ui contains: batch_size, ignore_rows, columns_details, reference_mapping, patient_identifier_type, patient_identifier_column
        const tableDetailsForUI = response.table_details_for_ui || {}
        const tableData: TableMetadata = {
          id: String(response.id),
          table_name: response.table_name,
          priority: response.priority || 0,
          is_required: response.is_required !== undefined ? response.is_required : true,
          is_phi_marking_done: response.is_phi_marking_done || false,
          run_config: response.run_config || {},
          table_details_for_ui: tableDetailsForUI,
          config: {
            batch_size: tableDetailsForUI.batch_size || 1000,
            ignore_rows: tableDetailsForUI.ignore_rows || {},
            columns_details: columnsDetails,
            reference_mapping: tableDetailsForUI.reference_mapping || {},
            patient_identifier_type: tableDetailsForUI.patient_identifier_type || null,
            patient_identifier_column: tableDetailsForUI.patient_identifier_column || null,
          },
        }
        // Update or add to tables list
        setTables((prev) => {
          const existing = prev.find((t) => t.id === tableId)
          if (existing) {
            return prev.map((t) => (t.id === tableId ? tableData : t))
          }
          return [...prev, tableData]
        })
        setSelectedTableId(tableId)
        setViewMode('edit')
      }
    } catch (error) {
      console.error('Error loading table details:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleBackToList = () => {
    setViewMode('list')
    setSelectedTableId(null)
  }

  const handleUpdateTable = async (updates: Partial<TableMetadata>) => {
    if (!selectedTableId) return

    const existingTable = tables.find((t) => t.id === selectedTableId)
    if (!existingTable) return

    // Start with current config and merge any incoming config updates
    const mergedConfig = { ...existingTable.config, ...(updates.config || {}) }

    // If caller provided table_details_for_ui (e.g., from Save UI Details), sync it into config
    if (updates.table_details_for_ui) {
      const ui = updates.table_details_for_ui
      mergedConfig.batch_size = ui.batch_size ?? mergedConfig.batch_size ?? 1000
      mergedConfig.ignore_rows = ui.ignore_rows ?? mergedConfig.ignore_rows ?? {}
      mergedConfig.columns_details = ui.columns_details ?? mergedConfig.columns_details ?? []
      mergedConfig.reference_mapping = ui.reference_mapping ?? mergedConfig.reference_mapping ?? {}
      mergedConfig.patient_identifier_type =
        ui.patient_identifier_type ?? mergedConfig.patient_identifier_type ?? null
      mergedConfig.patient_identifier_column =
        ui.patient_identifier_column ?? mergedConfig.patient_identifier_column ?? null
    }

    const updatedTable: TableMetadata = {
      ...existingTable,
      ...updates,
      config: mergedConfig,
      table_details_for_ui: updates.table_details_for_ui
        ? {
            batch_size: updates.table_details_for_ui.batch_size ?? mergedConfig.batch_size ?? 1000,
            ignore_rows: updates.table_details_for_ui.ignore_rows ?? mergedConfig.ignore_rows ?? {},
            columns_details:
              updates.table_details_for_ui.columns_details ?? mergedConfig.columns_details ?? [],
            reference_mapping:
              updates.table_details_for_ui.reference_mapping ?? mergedConfig.reference_mapping ?? {},
            patient_identifier_type:
              updates.table_details_for_ui.patient_identifier_type ??
              mergedConfig.patient_identifier_type ??
              null,
            patient_identifier_column:
              updates.table_details_for_ui.patient_identifier_column ??
              mergedConfig.patient_identifier_column ??
              null,
          }
        : {
            batch_size: mergedConfig.batch_size || 1000,
            ignore_rows: mergedConfig.ignore_rows || {},
            columns_details: mergedConfig.columns_details,
            reference_mapping: mergedConfig.reference_mapping || {},
            patient_identifier_type: mergedConfig.patient_identifier_type || null,
            patient_identifier_column: mergedConfig.patient_identifier_column || null,
          },
    }

    setTables(tables.map((t) => (t.id === selectedTableId ? updatedTable : t)))

    // Persist to backend with the now-synced table_details_for_ui + config
    try {
      const response = await tableMetadataApi.saveTableMetadata({
        id: parseInt(selectedTableId),
        table_name: updatedTable.table_name,
        columns: updatedTable.config.columns_details.reduce((acc: any, col) => {
          acc[col.column_name] = {
            is_phi: col.is_phi,
            mask_value: col.mask_value,
            ignore_rows: col.ignore_rows,
            de_identification_rule: col.de_identification_rule,
            column_name_for_phi_table: col.column_name_for_phi_table,
          }
          return acc
        }, {}),
        primary_key: updatedTable.run_config?.primary_key || {},
        max_nd_auto_increment_id: updatedTable.run_config?.max_nd_auto_increment_id || 0,
        table_details_for_ui: updatedTable.table_details_for_ui,
        run_config: updatedTable.run_config,
        is_required: updatedTable.is_required,
        priority: updatedTable.priority,
        is_phi_marking_done: updatedTable.is_phi_marking_done,
      })
      if (!response.success) {
        console.error('Error saving table:', response.message)
        alert('Error saving table: ' + (response.message || 'Unknown error'))
      } else {
        // Reload the table data to ensure we have the latest from backend
        await handleEditTable(selectedTableId)
      }
    } catch (error) {
      console.error('Error saving table:', error)
      alert('Error saving table: ' + (error instanceof Error ? error.message : 'Unknown error'))
    }
  }

  const handleCSVImport = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file || !selectedTableId) return

    const reader = new FileReader()
    reader.onload = (e) => {
      const text = e.target?.result as string
      const lines = text.split('\n')
      const headers = lines[0].split(',').map((h) => h.trim())

      const columnNameIdx = headers.indexOf('column_name')
      const isPhiIdx = headers.indexOf('is_phi')
      const deIdRuleIdx = headers.indexOf('de_identification_rule')
      const maskValueIdx = headers.indexOf('mask_value')

      if (columnNameIdx === -1) {
        alert('CSV must contain column_name column')
        return
      }

      const updatedColumns: ColumnDetail[] = []

      for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',').map((v) => v.trim())
        if (values.length < 1) continue

        const columnName = values[columnNameIdx]
        if (!columnName) continue

        const isPhi = values[isPhiIdx]?.toLowerCase() === 'true' || values[isPhiIdx] === '1'
        const deIdRule = values[deIdRuleIdx] || null
        const maskValue = values[maskValueIdx] || null

        updatedColumns.push({
          is_phi: isPhi,
          mask_value: maskValue,
          column_name: columnName,
          ignore_rows: {},
          de_identification_rule: deIdRule,
          column_name_for_phi_table: null,
        })
      }

      handleUpdateTable({
        config: {
          ...selectedTable!.config,
          columns_details: updatedColumns,
        },
      })

      alert('CSV imported successfully!')
    }
    reader.readAsText(file)
  }

  const handlePhiMarkingUploadClick = () => {
    uploadInputRef.current?.click()
  }

  const handlePhiMarkingUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (!file) return

    setUploadingPhi(true)
    try {
      const response = await phiMarkingApi.uploadFromCsv(file)
      if (!response.success) {
        alert('Failed to upload PHI marking CSV: ' + (response.message || 'Unknown error'))
        return
      }
      alert('PHI marking CSV uploaded successfully')
      await loadTables()
    } catch (error) {
      console.error('Error uploading PHI marking CSV:', error)
      alert('Error uploading PHI marking CSV')
    } finally {
      setUploadingPhi(false)
      event.target.value = ''
    }
  }

  const handlePhiMarkingDownload = async () => {
    setDownloadingPhi(true)
    try {
      const response = await phiMarkingApi.downloadCsv()
      if (!response.success || !response.blob) {
        alert('Failed to download PHI marking CSV: ' + (response.message || 'Unknown error'))
        return
      }
      const url = window.URL.createObjectURL(response.blob)
      const a = document.createElement('a')
      a.href = url
      a.download = response.filename || 'phi_marking.csv'
      a.click()
      window.URL.revokeObjectURL(url)
    } catch (error) {
      console.error('Error downloading PHI marking CSV:', error)
      alert('Error downloading PHI marking CSV')
    } finally {
      setDownloadingPhi(false)
    }
  }

  const handleCSVExport = () => {
    if (!selectedTable) return

    const headers = ['column_name', 'is_phi', 'de_identification_rule', 'mask_value']
    const rows = selectedTable.config.columns_details.map((col) => [
      col.column_name,
      col.is_phi ? 'true' : 'false',
      col.de_identification_rule || '',
      col.mask_value || '',
    ])

    const csvContent = [headers.join(','), ...rows.map((row) => row.join(','))].join('\n')
    const blob = new Blob([csvContent], { type: 'text/csv' })
    const url = window.URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${selectedTable.table_name}_columns.csv`
    a.click()
  }

  if (viewMode === 'edit' && selectedTable) {
    return (
      <TableMetadataEditView
        table={selectedTable}
        onBack={handleBackToList}
        onUpdate={handleUpdateTable}
        onCSVImport={handleCSVImport}
        onCSVExport={handleCSVExport}
      />
    )
  }

  return (
    <Layout>
      <div>
        <input
          ref={uploadInputRef}
          type="file"
          accept=".csv"
          className="hidden"
          onChange={handlePhiMarkingUpload}
        />
        <button
          onClick={() => router.push('/configuration')}
          className="flex items-center gap-2 text-gray-600 hover:text-gray-800 mb-4 transition-colors"
        >
          <ArrowLeft className="w-5 h-5" />
          <span>Back to Configuration</span>
        </button>

        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-800 mb-2">Table Metadata List View</h1>
            <p className="text-gray-600">Search, filter, and manage schemas for 3000+ tables</p>
          </div>
          <div className="flex gap-2">
            <button
              onClick={handlePhiMarkingUploadClick}
              disabled={uploadingPhi}
              className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-60 disabled:cursor-not-allowed"
            >
              {uploadingPhi ? 'Uploading...' : 'Upload PHI Marking'}
            </button>
            <button
              onClick={handlePhiMarkingDownload}
              disabled={downloadingPhi}
              className="bg-gray-600 text-white px-4 py-2 rounded-lg hover:bg-gray-700 transition-colors disabled:opacity-60 disabled:cursor-not-allowed"
            >
              {downloadingPhi ? 'Downloading...' : 'Download PHI Marking'}
            </button>
            <button className="bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 transition-colors flex items-center gap-2">
              <Plus className="w-4 h-4" />
              Add New Table
            </button>
          </div>
        </div>

        {/* Search and Filter */}
        <div className="bg-white rounded-lg shadow p-4 mb-6">
          <div className="flex gap-4 items-center mb-4">
            <div className="relative flex-1">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
              <input
                type="text"
                placeholder="Search tables..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <Plus className="w-5 h-5 text-gray-400" />
          </div>
          <div className="flex gap-4">
            <select
              value={filterPriority}
              onChange={(e) => setFilterPriority(e.target.value)}
              className="px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="all">All (Priority)</option>
              <option value="1">High</option>
              <option value="2">Medium</option>
              <option value="3">Low</option>
            </select>
            <select
              value={filterRequired}
              onChange={(e) => setFilterRequired(e.target.value)}
              className="px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="all">All (Required)</option>
              <option value="yes">Required</option>
              <option value="no">Not Required</option>
            </select>
            <select
              value={filterConfigStatus}
              onChange={(e) => setFilterConfigStatus(e.target.value)}
              className="px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="all">All (Config Status)</option>
              <option value="ui_set">UI Details Set</option>
              <option value="not_set">UI Details Not Set</option>
            </select>
          </div>
        </div>

        {/* Tables List */}
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <table className="w-full">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Table Name</th>
                <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Priority</th>
                <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Required?</th>
                <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">PII Status</th>
                <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Last Updated</th>
                <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={6} className="py-8 px-4 text-center text-gray-500">
                    Loading tables...
                  </td>
                </tr>
              ) : tables.length === 0 ? (
                <tr>
                  <td colSpan={6} className="py-8 px-4 text-center text-gray-500">
                    No tables found
                  </td>
                </tr>
              ) : (
                tables.map((table) => (
                <tr key={table.id} className="border-b border-gray-100 hover:bg-gray-50">
                  <td className="py-3 px-4 text-sm font-medium text-gray-800">{table.table_name}</td>
                  <td className="py-3 px-4">
                    <div className="w-4 h-4 rounded-full bg-blue-600"></div>
                  </td>
                  <td className="py-3 px-4">
                    <div className={`w-4 h-4 rounded-full ${table.is_required ? 'bg-blue-600' : 'bg-gray-300'}`}></div>
                  </td>
                  <td className="py-3 px-4">
                    <div className="flex items-center gap-2">
                      <span className="text-sm text-gray-700">{getPIIStatus(table)}</span>
                      <button
                        className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${
                          table.is_phi_marking_done ? 'bg-blue-600' : 'bg-gray-300'
                        }`}
                      >
                        <span
                          className={`inline-block h-3 w-3 transform rounded-full bg-white transition-transform ${
                            table.is_phi_marking_done ? 'translate-x-5' : 'translate-x-1'
                          }`}
                        />
                      </button>
                    </div>
                  </td>
                  <td className="py-3 px-4 text-sm text-gray-600">{getPIIStatus(table)}</td>
                  <td className="py-3 px-4">
                    <button
                      onClick={() => handleEditTable(table.id)}
                      className="bg-green-600 text-white px-3 py-1 rounded text-sm hover:bg-green-700 flex items-center gap-1"
                    >
                      <Edit2 className="w-4 h-4" />
                      Edit Metadata
                    </button>
                  </td>
                </tr>
                ))
              )}
            </tbody>
          </table>
          <div className="px-4 py-3 border-t border-gray-200 flex items-center justify-between">
            <span className="text-sm text-gray-600">
              Showing {tables.length} of {totalCount} tables
            </span>
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-600">
                Page {currentPage} of {totalPages}
              </span>
              {currentPage > 1 && (
                <button
                  onClick={() => setCurrentPage(currentPage - 1)}
                  className="px-3 py-1 border border-gray-300 rounded hover:bg-gray-100 text-sm"
                >
                  Previous
                </button>
              )}
              {currentPage < totalPages && (
                <button
                  onClick={() => setCurrentPage(currentPage + 1)}
                  className="px-3 py-1 border border-gray-300 rounded hover:bg-gray-100 text-sm"
                >
                  Next
                </button>
              )}
              <button className="p-2 text-gray-600 hover:text-gray-800">
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                </svg>
              </button>
            </div>
          </div>
        </div>
      </div>
    </Layout>
  )
}

interface TableMetadataEditViewProps {
  table: TableMetadata
  onBack: () => void
  onUpdate: (updates: Partial<TableMetadata>) => void
  onCSVImport: (event: React.ChangeEvent<HTMLInputElement>) => void
  onCSVExport: () => void
}

function TableMetadataEditView({ table, onBack, onUpdate, onCSVImport, onCSVExport }: TableMetadataEditViewProps) {
  const [activeTab, setActiveTab] = useState<'core' | 'columns' | 'ui'>('core')
  const [runConfig, setRunConfig] = useState(JSON.stringify(table.run_config || {}, null, 2))
  const [uiDetails, setUiDetails] = useState(JSON.stringify(table.table_details_for_ui || {}, null, 2))
  const [priority, setPriority] = useState(table.priority)
  const [isRequired, setIsRequired] = useState(table.is_required)
  const [deidRules, setDeidRules] = useState<string[]>([])
  const [loadingRules, setLoadingRules] = useState(false)
  
  // Fetch de-identification rules on component mount
  useEffect(() => {
    const fetchDeidRules = async () => {
      setLoadingRules(true)
      try {
        const response = await deidRulesApi.getDeidRules()
        if (response.success && response.rules) {
          setDeidRules(response.rules)
        }
      } catch (error) {
        console.error('Error loading de-identification rules:', error)
      } finally {
        setLoadingRules(false)
      }
    }
    fetchDeidRules()
  }, [])
  
  // Update local state when table prop changes (e.g., after fetching from backend)
  useEffect(() => {
    setRunConfig(JSON.stringify(table.run_config || {}, null, 2))
    setUiDetails(JSON.stringify(table.table_details_for_ui || {}, null, 2))
    setPriority(table.priority)
    setIsRequired(table.is_required)
  }, [table])

  const handleUpdateColumn = (index: number, field: keyof ColumnDetail, value: any) => {
    const updatedColumns = [...table.config.columns_details]
    updatedColumns[index] = { ...updatedColumns[index], [field]: value }
    onUpdate({
      config: { ...table.config, columns_details: updatedColumns },
    })
  }

  const handleAddColumn = () => {
    const columnName = prompt('Enter column name:')
    if (!columnName) return

    const newColumn: ColumnDetail = {
      is_phi: false,
      mask_value: null,
      column_name: columnName,
      ignore_rows: {},
      de_identification_rule: null,
      column_name_for_phi_table: null,
    }

    onUpdate({
      config: {
        ...table.config,
        columns_details: [...table.config.columns_details, newColumn],
      },
    })
  }

  const handleRemoveColumn = (index: number) => {
    const updatedColumns = table.config.columns_details.filter((_, i) => i !== index)
    onUpdate({
      config: { ...table.config, columns_details: updatedColumns },
    })
  }

  const handleSaveRunConfig = () => {
    try {
      const parsed = JSON.parse(runConfig)
      onUpdate({ run_config: parsed })
      alert('Run config saved!')
    } catch (error) {
      alert('Invalid JSON: ' + (error as Error).message)
    }
  }

  const handleSaveUiDetails = () => {
    try {
      const parsed = JSON.parse(uiDetails)
      // Ensure table_details_for_ui has the correct structure
      // If user provides columns_details in JSON, use it; otherwise use from table config
      const tableDetailsForUI = {
        batch_size: parsed.batch_size ?? table.config.batch_size ?? 1000,
        ignore_rows: parsed.ignore_rows ?? table.config.ignore_rows ?? {},
        columns_details: parsed.columns_details ?? table.config.columns_details ?? [],
        reference_mapping: parsed.reference_mapping ?? table.config.reference_mapping ?? {},
        patient_identifier_type: parsed.patient_identifier_type ?? table.config.patient_identifier_type ?? null,
        patient_identifier_column: parsed.patient_identifier_column ?? table.config.patient_identifier_column ?? null,
      }
      onUpdate({ table_details_for_ui: tableDetailsForUI })
      alert('UI details saved!')
    } catch (error) {
      alert('Invalid JSON: ' + (error as Error).message)
    }
  }

  const handleSavePriority = () => {
    onUpdate({ priority, is_required })
  }

  return (
    <Layout>
      <div>
        <button
          onClick={onBack}
          className="flex items-center gap-2 text-gray-600 hover:text-gray-800 mb-4 transition-colors"
        >
          <ArrowLeft className="w-5 h-5" />
          <span>Back to List</span>
        </button>

        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-800 mb-2">Edit Metadata: {table.table_name}</h1>
            <p className="text-gray-600">Configure table metadata and column details</p>
          </div>
        </div>

        {/* Tabs */}
        <div className="bg-white rounded-lg shadow mb-6">
          <div className="border-b border-gray-200">
            <nav className="flex -mb-px">
              <button
                onClick={() => setActiveTab('core')}
                className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors ${
                  activeTab === 'core'
                    ? 'border-blue-600 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                Core Settings
              </button>
              <button
                onClick={() => setActiveTab('columns')}
                className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors ${
                  activeTab === 'columns'
                    ? 'border-blue-600 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                Column Configuration
              </button>
              <button
                onClick={() => setActiveTab('ui')}
                className={`px-6 py-3 text-sm font-medium border-b-2 transition-colors ${
                  activeTab === 'ui'
                    ? 'border-blue-600 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                UI Details
              </button>
            </nav>
          </div>

          <div className="p-6">
            {/* Tab 1: Core Settings */}
            {activeTab === 'core' && (
              <div className="space-y-6">
                <div className="grid grid-cols-2 gap-6">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">Priority</label>
                    <input
                      type="number"
                      value={priority}
                      onChange={(e) => setPriority(parseInt(e.target.value) || 1)}
                      className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">Is Required</label>
                    <div className="flex items-center gap-2 mt-2">
                      <input
                        type="checkbox"
                        checked={isRequired}
                        onChange={(e) => setIsRequired(e.target.checked)}
                        className="w-4 h-4 text-blue-600 border-gray-300 rounded"
                      />
                      <span className="text-sm text-gray-700">{isRequired ? 'Required' : 'Not Required'}</span>
                    </div>
                  </div>
                </div>
                <button
                  onClick={handleSavePriority}
                  className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors"
                >
                  Save Priority & Required
                </button>

                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Run Config (JSON)</label>
                  <textarea
                    value={runConfig}
                    onChange={(e) => setRunConfig(e.target.value)}
                    rows={15}
                    className="w-full px-4 py-3 font-mono text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="Enter JSON configuration..."
                  />
                  <div className="flex gap-2 mt-2">
                    <button
                      onClick={handleSaveRunConfig}
                      className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors text-sm"
                    >
                      Save Run Config
                    </button>
                    <button
                      onClick={() => {
                        try {
                          const parsed = JSON.parse(runConfig)
                          setRunConfig(JSON.stringify(parsed, null, 2))
                        } catch (error) {
                          alert('Invalid JSON')
                        }
                      }}
                      className="bg-gray-200 text-gray-700 px-4 py-2 rounded-lg hover:bg-gray-300 transition-colors text-sm"
                    >
                      Format JSON
                    </button>
                  </div>
                </div>
              </div>
            )}

            {/* Tab 2: Column Configuration */}
            {activeTab === 'columns' && (
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-lg font-semibold text-gray-800">Column Configuration</h3>
                  <div className="flex gap-2">
                    <label className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors cursor-pointer flex items-center gap-2 text-sm">
                      <Upload className="w-4 h-4" />
                      Import Column Config
                      <input type="file" accept=".csv" onChange={onCSVImport} className="hidden" />
                    </label>
                    <button
                      onClick={onCSVExport}
                      className="bg-gray-600 text-white px-4 py-2 rounded-lg hover:bg-gray-700 transition-colors text-sm"
                    >
                      Export Column Config
                    </button>
                    <button
                      onClick={handleAddColumn}
                      className="bg-green-600 text-white px-4 py-2 rounded-lg hover:bg-green-700 transition-colors flex items-center gap-2 text-sm"
                    >
                      <Plus className="w-4 h-4" />
                      Add Column
                    </button>
                  </div>
                </div>

                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead className="bg-gray-50 border-b border-gray-200">
                      <tr>
                        <th className="text-left py-2 px-3 text-sm font-semibold text-gray-700">Column Name</th>
                        <th className="text-left py-2 px-3 text-sm font-semibold text-gray-700">Is PII</th>
                        <th className="text-left py-2 px-3 text-sm font-semibold text-gray-700">De-ID Rule</th>
                        <th className="text-left py-2 px-3 text-sm font-semibold text-gray-700">Mask Value</th>
                        <th className="text-left py-2 px-3 text-sm font-semibold text-gray-700">Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {table.config.columns_details.map((column, idx) => (
                        <tr key={idx} className="border-b border-gray-100">
                          <td className="py-2 px-3 text-sm text-gray-700">{column.column_name}</td>
                          <td className="py-2 px-3">
                            <input
                              type="checkbox"
                              checked={column.is_phi}
                              onChange={(e) => handleUpdateColumn(idx, 'is_phi', e.target.checked)}
                              className="w-4 h-4 text-blue-600 border-gray-300 rounded"
                            />
                          </td>
                          <td className="py-2 px-3">
                            <select
                              value={column.de_identification_rule || ''}
                              onChange={(e) => handleUpdateColumn(idx, 'de_identification_rule', e.target.value || null)}
                              className="w-full px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white"
                            >
                              <option value="">Select Rule</option>
                              {deidRules.map((rule) => (
                                <option key={rule} value={rule}>
                                  {rule}
                                </option>
                              ))}
                            </select>
                          </td>
                          <td className="py-2 px-3">
                            <input
                              type="text"
                              value={column.mask_value || ''}
                              onChange={(e) => handleUpdateColumn(idx, 'mask_value', e.target.value || null)}
                              placeholder="Mask value"
                              className="w-full px-2 py-1 border border-gray-300 rounded text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                            />
                          </td>
                          <td className="py-2 px-3">
                            <button
                              onClick={() => handleRemoveColumn(idx)}
                              className="text-red-600 hover:text-red-800 text-sm"
                            >
                              <X className="w-4 h-4" />
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Tab 3: UI Details */}
            {activeTab === 'ui' && (
              <div className="space-y-4">
                <h3 className="text-lg font-semibold text-gray-800">UI Details</h3>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Table Details for UI (JSON)
                  </label>
                  <textarea
                    value={uiDetails}
                    onChange={(e) => setUiDetails(e.target.value)}
                    rows={20}
                    className="w-full px-4 py-3 font-mono text-sm border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="Enter JSON configuration for UI details..."
                  />
                  <div className="flex gap-2 mt-2">
                    <button
                      onClick={handleSaveUiDetails}
                      className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 transition-colors text-sm"
                    >
                      Save UI Details
                    </button>
                    <button
                      onClick={() => {
                        try {
                          const parsed = JSON.parse(uiDetails)
                          setUiDetails(JSON.stringify(parsed, null, 2))
                        } catch (error) {
                          alert('Invalid JSON')
                        }
                      }}
                      className="bg-gray-200 text-gray-700 px-4 py-2 rounded-lg hover:bg-gray-300 transition-colors text-sm"
                    >
                      Format JSON
                    </button>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </Layout>
  )
}
