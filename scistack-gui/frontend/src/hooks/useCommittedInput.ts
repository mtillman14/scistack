import { useState, useEffect, useRef, useCallback } from 'react'

interface Options {
  initialValue: string
  /**
   * When this value changes the draft and committed baseline are reset to
   * initialValue.  Pass the node id (or any key that identifies "which item
   * is being edited") so switching to a different node starts fresh.
   */
  resetKey?: unknown
  /** Called on every keystroke — use for live canvas / preview updates. */
  onLiveChange?: (val: string) => void
  /** Called on Enter or blur — use for backend persistence. */
  onSave: (val: string) => void
}

interface CommittedInputProps {
  value: string
  /** Always-current ref to the draft value — safe to read from sibling callbacks. */
  valueRef: React.MutableRefObject<string>
  onChange: (e: React.ChangeEvent<HTMLInputElement>) => void
  onBlur: () => void
  onKeyDown: (e: React.KeyboardEvent<HTMLInputElement>) => void
}

/**
 * Manages a text input whose changes are previewed live but only persisted
 * on Enter or blur.  Escape reverts to the last saved value.
 *
 * Spread the returned props directly onto an <input>:
 *   const props = useCommittedInput({ initialValue, resetKey: id, onLiveChange, onSave })
 *   <input {...props} style={...} />
 */
export function useCommittedInput({ initialValue, resetKey, onLiveChange, onSave }: Options): CommittedInputProps {
  const [draft, setDraft] = useState(initialValue)
  const draftRef = useRef(initialValue)
  const committed = useRef(initialValue)
  const skipNextSave = useRef(false)

  useEffect(() => {
    setDraft(initialValue)
    draftRef.current = initialValue
    committed.current = initialValue
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [resetKey])

  const onChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value
    setDraft(val)
    draftRef.current = val
    onLiveChange?.(val)
  }, [onLiveChange])

  const onBlur = useCallback(() => {
    if (skipNextSave.current) {
      skipNextSave.current = false
      return
    }
    committed.current = draftRef.current
    onSave(draftRef.current)
  }, [onSave])

  const onKeyDown = useCallback((e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      committed.current = draftRef.current
      onSave(draftRef.current)
      e.currentTarget.blur()
    } else if (e.key === 'Escape') {
      skipNextSave.current = true
      const revert = committed.current
      setDraft(revert)
      draftRef.current = revert
      onLiveChange?.(revert)
      e.currentTarget.blur()
    }
  }, [onSave, onLiveChange])

  return { value: draft, valueRef: draftRef, onChange, onBlur, onKeyDown }
}
