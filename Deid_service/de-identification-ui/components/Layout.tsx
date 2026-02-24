import Sidebar from './Sidebar'

interface LayoutProps {
  children: React.ReactNode
  dark?: boolean
}

export default function Layout({ children, dark = false }: LayoutProps) {
  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <main className={`flex-1 ${dark ? 'bg-gray-900' : 'bg-gray-100'} p-8`}>
        {children}
      </main>
    </div>
  )
}

