'use client'

import Layout from '@/components/Layout'

export default function Dashboard() {
  return (
    <Layout>
      <div>
        <h1 className="text-3xl font-bold text-gray-800 mb-2">Dashboard</h1>
        <p className="text-gray-600 mb-6">Welcome to De-Identification Management System</p>
        <div className="bg-white rounded-lg shadow p-6">
          <p className="text-gray-600">Dashboard content goes here</p>
        </div>
      </div>
    </Layout>
  )
}

