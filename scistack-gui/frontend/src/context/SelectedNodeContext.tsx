import { createContext, useContext, useState } from 'react'
import type { Node } from '@xyflow/react'

interface SelectedNodeContextType {
  selectedNode: Node | null
  setSelectedNode: (node: Node | null) => void
}

const SelectedNodeContext = createContext<SelectedNodeContextType>({
  selectedNode: null,
  setSelectedNode: () => {},
})

export function SelectedNodeProvider({ children }: { children: React.ReactNode }) {
  const [selectedNode, setSelectedNode] = useState<Node | null>(null)
  return (
    <SelectedNodeContext.Provider value={{ selectedNode, setSelectedNode }}>
      {children}
    </SelectedNodeContext.Provider>
  )
}

export function useSelectedNode() {
  return useContext(SelectedNodeContext)
}
