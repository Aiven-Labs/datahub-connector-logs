import {
  Alert, Box, Button, Card, Checkbox, CheckboxGroup, DataTable, Dialog, DropdownMenu, EmptyState,
  Input, Modal, MultiSelect, Select, Textarea, Typography,
} from '@aivenio/aquarium'
import more from '@aivenio/aquarium/icons/more'
import { useCallback, useEffect, useState } from 'react'
import { api, type Dest, type DestType, type Field, type Source } from './api'

const ALL_STATUSES = ['FAILURE', 'TIMEOUT', 'ABORTED', 'CANCELLED', 'SUCCESS']
const DEFAULT_STATUSES = ['FAILURE', 'TIMEOUT', 'ABORTED', 'CANCELLED']

type Form = { name: string; config: Record<string, string>; statuses: string[]; source_urns: string[]; max_report_kb: number; backfill: boolean }

function FieldInput({ f, value, secretSet, onChange }: { f: Field; value: string; secretSet: boolean; onChange: (v: string) => void }) {
  const labelText = f.label + (f.required && !secretSet ? ' *' : '')
  const placeholder = f.secret && secretSet ? '(unchanged)' : f.placeholder
  if (f.type === 'select')
    return <Select labelText={labelText} options={(f.options ?? []).map((o) => ({ value: o, label: o }))} value={value} onChange={(o) => onChange(o?.value ?? '')} />
  if (f.type === 'textarea')
    return <Textarea labelText={labelText} placeholder={placeholder} rows={3} value={value} onChange={(e) => onChange(e.target.value)} />
  return <Input labelText={labelText} placeholder={placeholder} type={f.type === 'password' ? 'password' : 'text'} autoComplete="off" value={value} onChange={(e) => onChange(e.target.value)} />
}

function IntegrationModal({ type, dest, sources, onClose, onSaved }: { type: DestType | null; dest: Dest | null; sources: Source[]; onClose: () => void; onSaved: () => void }) {
  const [form, setForm] = useState<Form | null>(null)
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null)
  const [busy, setBusy] = useState<'test' | 'save' | null>(null)

  useEffect(() => {
    if (!type) return
    setMsg(null)
    setForm(dest
      ? { name: dest.name, config: dest.config, statuses: dest.statuses, source_urns: dest.source_urns, max_report_kb: dest.max_report_kb, backfill: false }
      : { name: type.label, config: {}, statuses: DEFAULT_STATUSES, source_urns: [], max_report_kb: 900, backfill: false })
  }, [type, dest])

  if (!type || !form) return <Modal open={false} onClose={onClose} title="" />
  const set = (p: Partial<Form>) => setForm({ ...form, ...p })
  const body = () => ({ id: dest?.id, type: type.type, ...form, enabled: dest?.enabled ?? true })

  const run = async (kind: 'test' | 'save') => {
    setBusy(kind); setMsg(null)
    try {
      if (kind === 'test') {
        const r = await api('/api/destination-test', 'POST', body())
        setMsg(r.ok ? { ok: true, text: 'Test event delivered.' } : { ok: false, text: r.error })
      } else {
        await api(dest ? `/api/destinations/${dest.id}` : '/api/destinations', dest ? 'PUT' : 'POST', body())
        onSaved()
      }
    } catch (e: any) { setMsg({ ok: false, text: e.message }) }
    setBusy(null)
  }

  const srcOptions = sources.map((s) => ({ value: s.source_urn, label: s.source_name }))
  return (
    <Modal
      open
      onClose={onClose}
      size="md"
      title={`${dest ? 'Edit' : 'Add'} ${type.label} integration`}
      primaryAction={{ text: 'Save', onClick: () => run('save'), loading: busy === 'save' }}
      secondaryActions={[{ text: 'Test connection', onClick: () => run('test'), loading: busy === 'test' }, { text: 'Cancel', onClick: onClose }]}
    >
      <Box.Flex flexDirection="column" gap="4">
        {msg && <Alert type={msg.ok ? 'success' : 'error'}>{msg.text}</Alert>}
        <Input labelText="Name" value={form.name} onChange={(e) => set({ name: e.target.value })} />
        {type.fields.map((f) => (
          <FieldInput key={f.name} f={f} value={form.config[f.name] ?? (f.secret ? '' : f.default ?? '')}
            secretSet={!!dest?.secrets_set.includes(f.name)} onChange={(v) => set({ config: { ...form.config, [f.name]: v } })} />
        ))}
        <Typography.DefaultStrong>What to send</Typography.DefaultStrong>
        <CheckboxGroup labelText="Run statuses" cols="3" value={form.statuses} onChange={(v) => set({ statuses: v ?? [] })}>
          {ALL_STATUSES.map((s) => <Checkbox key={s} value={s}>{s}</Checkbox>)}
        </CheckboxGroup>
        <MultiSelect
          labelText="Sources"
          description="Leave empty to send every source."
          options={srcOptions}
          value={srcOptions.filter((o) => form.source_urns.includes(o.value))}
          onChange={(v) => set({ source_urns: (v ?? []).map((o) => o.value) })}
        />
        <Input labelText="Max log size per run (KB)" description="The end of the log is kept when it is longer." type="number"
          value={String(form.max_report_kb)} onChange={(e) => set({ max_report_kb: +e.target.value })} />
        {!dest && (
          <Checkbox checked={form.backfill} onChange={(e) => set({ backfill: e.target.checked })}>
            Also send runs already collected (default: only runs that finish from now on)
          </Checkbox>
        )}
      </Box.Flex>
    </Modal>
  )
}

export function Integrations() {
  const [types, setTypes] = useState<DestType[]>([])
  const [sources, setSources] = useState<Source[]>([])
  const [dests, setDests] = useState<Dest[] | null>(null)
  const [editing, setEditing] = useState<{ type: DestType; dest: Dest | null } | null>(null)
  const [deleting, setDeleting] = useState<Dest | null>(null)
  const [error, setError] = useState('')

  const load = useCallback(() => api<Dest[]>('/api/destinations').then(setDests).catch((e) => setError(e.message)), [])
  useEffect(() => {
    api<DestType[]>('/api/destination-types').then(setTypes)
    api<Source[]>('/api/sources').then(setSources)
    load()
    const t = setInterval(load, 15000)
    return () => clearInterval(t)
  }, [load])

  const typeOf = (d: Dest) => types.find((t) => t.type === d.type)
  const act = (p: Promise<unknown>) => p.then(load).catch((e) => setError(e.message))
  const onAction = (key: string | number, d: Dest) => {
    if (key === 'edit') setEditing({ type: typeOf(d)!, dest: d })
    else if (key === 'toggle') act(api(`/api/destinations/${d.id}`, 'PUT', { ...d, enabled: !d.enabled }))
    else if (key === 'retry') act(api(`/api/destinations/${d.id}/retry`, 'POST'))
    else if (key === 'delete') setDeleting(d)
  }
  const sends = (d: Dest) =>
    `${d.statuses.includes('SUCCESS') ? 'All runs' : 'Failed runs'} · ${d.source_urns.length ? `${d.source_urns.length} source(s)` : 'all sources'}`

  return (
    <Box paddingTop="5">
      <Typography.Heading>Send logs to…</Typography.Heading>
      <Box.Flex gap="4" flexWrap="wrap" marginTop="4" marginBottom="7">
        {types.map((t) => (
          <Card key={t.type} title={t.label} primaryAction={{ text: 'Add integration', onClick: () => setEditing({ type: t, dest: null }) }}>
            Forward ingestion run logs to {t.label}.
          </Card>
        ))}
      </Box.Flex>

      <Typography.Heading>Active integrations</Typography.Heading>
      <Box marginTop="4">
        {error && <Alert type="error" onDismiss={() => setError('')}>{error}</Alert>}
        {dests && dests.length === 0 ? (
          <EmptyState title="No integrations yet">Pick a destination above to start forwarding logs.</EmptyState>
        ) : dests && (
          <DataTable
            ariaLabel="Integrations"
            columns={[
              { type: 'item', headerName: 'Name', item: (d) => ({ title: d.name, caption: typeOf(d)?.label ?? d.type }) },
              { type: 'status', headerName: 'State', status: (d) => (d.enabled ? { status: 'success', text: 'Active' } : { status: 'neutral', text: 'Paused' }), width: 110 },
              { type: 'text', headerName: 'Sends', field: 'id', formatter: (_, d) => sends(d) },
              { type: 'number', headerName: 'Delivered', field: 'sent', width: 100 },
              { type: 'number', headerName: 'Pending', field: 'retrying', width: 90 },
              { type: 'number', headerName: 'Failed', field: 'dead', width: 90 },
              { type: 'text', headerName: 'Last error', field: 'last_error', formatter: (v) => String(v ?? '') },
            ]}
            rows={dests}
            menuHeaderName="Actions"
            menu={(d) => (
              <DropdownMenu.Items>
                <DropdownMenu.Item id="edit">Edit</DropdownMenu.Item>
                <DropdownMenu.Item id="toggle">{d.enabled ? 'Pause' : 'Resume'}</DropdownMenu.Item>
                {d.dead || d.retrying ? <DropdownMenu.Item id="retry">Retry failed</DropdownMenu.Item> : null}
                <DropdownMenu.Item id="delete" kind="danger">Delete</DropdownMenu.Item>
              </DropdownMenu.Items>
            )}
            onAction={onAction as any}
          />
        )}
      </Box>

      <IntegrationModal type={editing?.type ?? null} dest={editing?.dest ?? null} sources={sources}
        onClose={() => setEditing(null)} onSaved={() => { setEditing(null); load() }} />
      <Dialog
        open={!!deleting}
        type="danger"
        title="Delete this integration?"
        onClose={() => setDeleting(null)}
        primaryAction={{ text: 'Delete', onClick: () => { act(api(`/api/destinations/${deleting!.id}`, 'DELETE')); setDeleting(null) } }}
        secondaryAction={{ text: 'Cancel', onClick: () => setDeleting(null) }}
      >
        “{deleting?.name}” and its delivery history will be removed. Nothing already delivered is recalled.
      </Dialog>
    </Box>
  )
}
