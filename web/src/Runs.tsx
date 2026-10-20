import { Alert, Box, DataTable, Drawer, EmptyState, Input, MultiSelect, Select, StatusChip, Typography } from '@aivenio/aquarium'
import { useCallback, useEffect, useState } from 'react'
import { api, fmtDur, fmtTime, type Run, type Source } from './api'

const STATUSES = [
  { value: 'problems', label: 'Problems only' },
  { value: '', label: 'All runs' },
  ...['FAILURE', 'TIMEOUT', 'CANCELLED', 'ABORTED', 'RUNNING', 'SUCCESS'].map((s) => ({ value: s, label: s })),
]
const FAILED = ['FAILURE', 'TIMEOUT', 'ABORTED']

const chipOf = (r: Run) =>
  r.stuck ? { status: 'warning' as const, text: 'STUCK' }
  : r.status === 'SUCCESS' ? { status: 'success' as const, text: r.status }
  : FAILED.includes(r.status) ? { status: 'danger' as const, text: r.status }
  : r.status === 'CANCELLED' ? { status: 'warning' as const, text: r.status }
  : { status: 'info' as const, text: r.status }

const esc = (s: string) => s.replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' })[c]!)

function LogView({ text, q }: { text: string; q: string }) {
  const html = text.split('\n').map((l) => {
    let h = esc(l)
    if (q) h = h.split(esc(q)).join(`<mark>${esc(q)}</mark>`)
    return /\b(ERROR|Traceback|Exception|Error:)\b/.test(l) ? `<span class="err">${h}</span>` : h + '\n'
  }).join('')
  useEffect(() => { document.querySelector('.log .err')?.scrollIntoView({ block: 'center' }) }, [text])
  return <pre className="log" dangerouslySetInnerHTML={{ __html: html }} />
}

export function Runs() {
  const [status, setStatus] = useState('problems')
  const [source, setSource] = useState('')
  const [q, setQ] = useState('')
  const [hidden, setHidden] = useState<string[]>(() => JSON.parse(localStorage.getItem('hiddenSources') || '[]'))
  const [runs, setRuns] = useState<{ items: Run[]; total: number } | null>(null)
  const [sources, setSources] = useState<Source[]>([])
  const [open, setOpen] = useState<Run | null>(null)
  const [detail, setDetail] = useState<Run | null>(null)
  const [error, setError] = useState('')

  const load = useCallback(() => {
    const p = new URLSearchParams({ status, source, exclude: hidden.join(','), q, limit: '300' })
    api('/api/runs?' + p).then((d) => { setRuns(d); setError('') }).catch((e) => setError(e.message))
  }, [status, source, hidden, q])

  useEffect(() => { // debounce covers typing in the search box
    const t = setTimeout(load, 300)
    const i = setInterval(load, 30000)
    return () => { clearTimeout(t); clearInterval(i) }
  }, [load])
  useEffect(() => { api<Source[]>('/api/sources').then(setSources).catch(() => {}) }, [])

  const show = (r: Run) => {
    setOpen(r); setDetail(null)
    api<Run>('/api/runs/' + r.urn.split(':').pop()).then(setDetail).catch((e) => setError(e.message))
  }
  const text = detail?.report || (detail ? '(no log yet — run has not reported)' : 'Loading…')
  const download = () => {
    const a = document.createElement('a')
    a.href = URL.createObjectURL(new Blob([text]))
    a.download = `${open?.source_name}-${open?.start_ms}.log`
    a.click()
  }

  const hide = (urns: string[]) => { setHidden(urns); localStorage.setItem('hiddenSources', JSON.stringify(urns)) }
  const hideOptions = sources.map((s) => ({ value: s.source_urn, label: s.source_name }))
  const sourceOptions = [{ value: '', label: 'All sources' }, ...sources.map((s) => ({ value: s.source_urn, label: `${s.source_name} (${s.failed} failed)` }))]

  return (
    <Box paddingTop="5">
      <Box.Flex gap="4" alignItems="flex-end" flexWrap="wrap" marginBottom="4">
        <Box width="2/12"><Select labelText="Status" options={STATUSES} value={status} onChange={(o) => setStatus(o?.value ?? '')} /></Box>
        <Box width="3/12"><Select labelText="Source" options={sourceOptions} value={source} onChange={(o) => setSource(o?.value ?? '')} /></Box>
        <Box width="3/12">
          <MultiSelect labelText="Hide sources" placeholder="None" options={hideOptions}
            value={hideOptions.filter((o) => hidden.includes(o.value))} onChange={(v) => hide((v ?? []).map((o) => o.value))} />
        </Box>
        <Box width="3/12"><Input labelText="Search log text" placeholder="e.g. Traceback" value={q} onChange={(e) => setQ(e.target.value)} /></Box>
      </Box.Flex>

      {error && <Alert type="error" title="Could not load runs">{error}</Alert>}
      {runs && runs.items.length === 0 ? (
        <EmptyState title="No runs match">Try a different status or source filter.</EmptyState>
      ) : runs && (
        <>
          <DataTable
            ariaLabel="Ingestion runs"
            pagination
            columns={[
              { type: 'status', headerName: 'Status', status: chipOf, width: 120 },
              { type: 'text', headerName: 'Source', field: 'source_name', width: 220 },
              { type: 'text', headerName: 'Started', field: 'start_ms', formatter: (v) => fmtTime(v as number), width: 180 },
              { type: 'text', headerName: 'Took', field: 'duration_ms', formatter: (v) => fmtDur(v as number), width: 80 },
              { type: 'text', headerName: 'Why', field: 'summary', formatter: (v) => { const s = String(v ?? ''); return s.length > 110 ? s.slice(0, 110) + '…' : s } },
              { type: 'action', headerName: 'Log', headerInvisible: true, action: (r) => ({ text: 'View log', onClick: () => show(r) }), width: 110 },
            ]}
            rows={runs.items.map((r) => ({ ...r, id: r.urn }))}
          />
          <Box marginTop="3"><Typography.Small color="default">{runs.items.length} of {runs.total} runs · collected from DataHub's Kafka, so history starts when the app started (or the last 24h)</Typography.Small></Box>
        </>
      )}

      <Drawer
        open={!!open}
        onClose={() => setOpen(null)}
        size="lg"
        title={open?.source_name ?? 'Run'}
        primaryAction={{ text: 'Download log', onClick: download }}
        secondaryActions={{ text: 'Close', onClick: () => setOpen(null) }}
      >
        {open && (
          <Box.Flex gap="3" alignItems="center" marginBottom="4">
            <StatusChip {...chipOf(open)} />
            <Typography.Small color="default">{fmtTime(open.start_ms)} · took {fmtDur(open.duration_ms)}</Typography.Small>
          </Box.Flex>
        )}
        <LogView text={text} q={q} />
      </Drawer>
    </Box>
  )
}
