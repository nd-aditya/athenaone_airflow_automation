'use client'

import { useState, useEffect } from 'react'
import { useRouter } from 'next/navigation'
import Layout from '@/components/Layout'
import { Eye } from 'lucide-react'
import { queueApi } from '@/lib/api'

interface Queue {
  id: number | null
  queue_name: string
  dump_date: string | null
  dump_date_status: string
  status: string
  total_tables: number
  created_at: string | null
  updated_at: string | null
}

export default function QueueManagement() {
  const router = useRouter()
  const [queues, setQueues] = useState<Queue[]>([])
  const [loading, setLoading] = useState(false)
  const [starting, setStarting] = useState(false)
  const [message, setMessage] = useState<{ type: 'success' | 'error'; text: string } | null>(null)

  useEffect(() => {
    loadQueues()
  }, [])

  const loadQueues = async () => {
    setLoading(true)
    try {
      const response = await queueApi.getQueues()
      if (response.success && response.results) {
        setQueues(response.results)
      }
    } catch (error) {
      console.error('Error loading queues:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleStartDailyDump = async () => {
    setStarting(true)
    setMessage(null)
    try {
      const response = await queueApi.startDailyDump()
      if (response.success) {
        setMessage({ type: 'success', text: 'Daily dump processing started successfully!' })
        loadQueues() // Reload queues
      } else {
        setMessage({ type: 'error', text: response.message || 'Failed to start daily dump' })
      }
    } catch (error) {
      setMessage({ type: 'error', text: 'Error starting daily dump' })
    } finally {
      setStarting(false)
    }
  }

  const handleViewQueue = (queueName: string) => {
    router.push(`/monitoring?queue=${encodeURIComponent(queueName)}`)
  }

  const getStatusColor = (status: string) => {
    if (status === 'COMPLETED') return 'text-green-600'
    if (status === 'IN_PROGRESS') return 'text-blue-600'
    if (status === 'FAILED') return 'text-red-600'
    return 'text-gray-600'
  }

  const getStatusDotColor = (status: string) => {
    if (status === 'COMPLETED') return 'bg-green-500'
    if (status === 'IN_PROGRESS') return 'bg-blue-500'
    if (status === 'FAILED') return 'bg-red-500'
    return 'bg-gray-500'
  }

  return (
    <Layout>
      <div>
        <h1 className="text-3xl font-bold text-gray-800 mb-6">Queue Management & Trigger</h1>
        
        <div className="bg-white rounded-lg shadow p-6 mb-6">
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
            onClick={handleStartDailyDump}
            disabled={starting}
            className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 transition-colors font-medium disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {starting ? 'Starting...' : 'Start Daily Dump Processing'}
          </button>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold text-gray-800 mb-4">Recent Incremental Queues</h2>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-gray-200">
                  <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Queue Name</th>
                  <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Dump Date</th>
                  <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Status</th>
                  <th className="text-left py-3 px-4 text-sm font-semibold text-gray-700">Actions</th>
                </tr>
              </thead>
              <tbody>
                {loading ? (
                  <tr>
                    <td colSpan={4} className="py-8 px-4 text-center text-gray-500">
                      Loading queues...
                    </td>
                  </tr>
                ) : queues.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="py-8 px-4 text-center text-gray-500">
                      No queues found
                    </td>
                  </tr>
                ) : (
                  queues.map((queue, index) => (
                    <tr key={index} className="border-b border-gray-100">
                      <td className="py-3 px-4 text-sm text-gray-700">{queue.queue_name}</td>
                      <td className="py-3 px-4">
                        <div className="flex items-center gap-2">
                          <span className={`w-2 h-2 rounded-full ${getStatusDotColor(queue.dump_date_status)}`}></span>
                          <span className={`text-sm font-medium ${getStatusColor(queue.dump_date_status)}`}>
                            {queue.dump_date_status}
                          </span>
                        </div>
                      </td>
                      <td className="py-3 px-4">
                        <div className="flex items-center gap-2">
                          <span className={`w-2 h-2 rounded-full ${getStatusDotColor(queue.status)}`}></span>
                          <span className={`text-sm font-medium ${getStatusColor(queue.status)}`}>
                            {queue.status}
                          </span>
                        </div>
                      </td>
                      <td className="py-3 px-4">
                        <button
                          onClick={() => handleViewQueue(queue.queue_name)}
                          className="bg-blue-600 text-white px-3 py-1 rounded text-sm hover:bg-blue-700 transition-colors flex items-center gap-1"
                        >
                          <Eye className="w-4 h-4" />
                          View
                        </button>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </Layout>
  )
}

