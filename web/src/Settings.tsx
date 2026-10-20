import { Alert, Box, Button, Input, Textarea, Typography } from '@aivenio/aquarium'
import { useEffect, useState } from 'react'
import { api } from './api'

type Form = { gms_url: string; token: string; kafka_bootstrap: string; kafka_ca: string; kafka_cert: string; kafka_key: string }
type Check = { ok: boolean; detail: string }
const EMPTY: Form = { gms_url: '', token: '', kafka_bootstrap: '', kafka_ca: '', kafka_cert: '', kafka_key: '' }

export function Settings({ onConfigured }: { onConfigured: (configured: boolean) => void }) {
  const [form, setForm] = useState<Form>(EMPTY)
  const [secretsSet, setSecretsSet] = useState<string[]>([])
  const [checks, setChecks] = useState<{ datahub: Check; kafka: Check } | null>(null)
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null)
  const [busy, setBusy] = useState<'test' | 'save' | null>(null)

  const apply = (s: any) => { setForm({ ...EMPTY, ...s }); setSecretsSet(s.secrets_set); onConfigured(s.configured) }
  useEffect(() => { api('/api/settings').then(apply) }, [])

  const set = (p: Partial<Form>) => setForm({ ...form, ...p })
  const secret = (k: string) => (secretsSet.includes(k) ? '(unchanged)' : undefined)

  const run = async (kind: 'test' | 'save') => {
    setBusy(kind); setMsg(null)
    try {
      if (kind === 'test') { setChecks(await api('/api/settings/test', 'POST', form)) }
      else {
        apply(await api('/api/settings', 'PUT', form))
        setMsg({ ok: true, text: 'Saved. The app reconnects to Kafka within a few seconds.' })
      }
    } catch (e: any) { setMsg({ ok: false, text: e.message }) }
    setBusy(null)
  }

  return (
    <Box paddingTop="5" maxWidth="4xl">
      <Box.Flex flexDirection="column" gap="4">
        <Typography.Heading>1. DataHub</Typography.Heading>
        <Input labelText="DataHub URL (GMS)" description="The backend URL, not the web UI. The app reads the schema registry and ingestion sources from it."
          placeholder="https://datahub-gms.example.com" value={form.gms_url} onChange={(e) => set({ gms_url: e.target.value })} />
        <Input labelText="Access token" description="Settings → Access Tokens in DataHub. Its user needs the Manage Ingestion privilege."
          type="password" autoComplete="off" placeholder={secret('token')} value={form.token} onChange={(e) => set({ token: e.target.value })} />
        {checks && <Alert type={checks.datahub.ok ? 'success' : 'error'} title="DataHub">{checks.datahub.detail}</Alert>}

        <Box marginTop="4"><Typography.Heading>2. DataHub's Kafka</Typography.Heading></Box>
        <Input labelText="Bootstrap servers" description="host:port of the Kafka cluster DataHub publishes to. Inside the same VPC use the private port."
          placeholder="kafka-example.aivencloud.com:12345" value={form.kafka_bootstrap} onChange={(e) => set({ kafka_bootstrap: e.target.value })} />
        <Textarea labelText="CA certificate (PEM)" rows={4} placeholder="-----BEGIN CERTIFICATE-----" value={form.kafka_ca} onChange={(e) => set({ kafka_ca: e.target.value })} />
        <Textarea labelText="Client certificate (PEM)" rows={4} placeholder="-----BEGIN CERTIFICATE-----" value={form.kafka_cert} onChange={(e) => set({ kafka_cert: e.target.value })} />
        <Textarea labelText="Client key (PEM)" rows={4} placeholder={secret('kafka_key') ?? '-----BEGIN PRIVATE KEY-----'} value={form.kafka_key} onChange={(e) => set({ kafka_key: e.target.value })} />
        {checks && <Alert type={checks.kafka.ok ? 'success' : 'error'} title="Kafka">{checks.kafka.detail}</Alert>}

        {msg && <Alert type={msg.ok ? 'success' : 'error'}>{msg.text}</Alert>}
        <Box.Flex gap="3">
          <Button.Secondary onClick={() => run('test')} disabled={!!busy}>{busy === 'test' ? 'Testing…' : 'Test connection'}</Button.Secondary>
          <Button.Primary onClick={() => run('save')} disabled={!!busy}>Save</Button.Primary>
        </Box.Flex>
      </Box.Flex>
    </Box>
  )
}
