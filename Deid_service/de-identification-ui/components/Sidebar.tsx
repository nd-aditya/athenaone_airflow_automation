'use client'

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { 
  LayoutDashboard, 
  Settings, 
  List,
  Monitor,
  Table,
  GitBranch,
  Sliders
} from 'lucide-react'

interface NavItem {
  name: string
  href: string
  icon: React.ComponentType<{ className?: string }>
}

const navItems: NavItem[] = [
  { name: 'Dashboard', href: '/', icon: LayoutDashboard },
  { name: 'Configuration', href: '/configuration', icon: Settings },
  { name: 'Queue Management', href: '/queue-management', icon: List },
  { name: 'Monitoring', href: '/monitoring', icon: Monitor },
  { name: 'Processing Options', href: '/processing-options', icon: Sliders },
  { name: 'Table Metadata', href: '/table-metadata', icon: Table },
  { name: 'Incremental Pipeline', href: '/incremental-pipeline', icon: GitBranch },
]

export default function Sidebar() {
  const pathname = usePathname()

  return (
    <div className="w-64 bg-gray-800 text-white min-h-screen">
      <div className="p-6">
        <h2 className="text-xl font-bold">De-Identification</h2>
      </div>
      <nav className="px-4">
        {navItems.map((item) => {
          const Icon = item.icon
          const isActive = pathname === item.href
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`flex items-center gap-3 px-4 py-3 rounded-lg mb-2 transition-colors ${
                isActive
                  ? 'bg-blue-600 text-white'
                  : 'text-gray-300 hover:bg-gray-700 hover:text-white'
              }`}
            >
              <Icon className="w-5 h-5" />
              <span>{item.name}</span>
            </Link>
          )
        })}
      </nav>
    </div>
  )
}

