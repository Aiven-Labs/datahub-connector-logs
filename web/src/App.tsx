import { Box, Button, StatusChip, Tabs, Typography } from '@aivenio/aquarium'
import { useEffect, useState } from 'react'
import { api, fmtTime, type Health } from './api'
import { Integrations } from './Integrations'
import { Login } from './Login'
import { Runs } from './Runs'

export function App() {
  const [user, setUser] = useState<string | null | undefined>(undefined) // undefined = still checking
  useEffect(() => {
    api<{ user: string }>('/api/me').then((r) => setUser(r.user)).catch(() => setUser(null))
    const out = () => setUser(null)
    window.addEventListener('unauth', out)
    return () => window.removeEventListener('unauth', out)
  }, [])
  if (user === undefined) return null
  return user === null ? <Login onLogin={setUser} /> : <Main user={user} />
}

function Main({ user }: { user: string }) {
  const [tab, setTab] = useState<number | string>(location.hash === '#integrations' ? 'integrations' : 'runs')
  const pick = (t: number | string) => { setTab(t); history.replaceState(null, '', '#' + t) } // survives a refresh
  const [health, setHealth] = useState<Health>({})
  useEffect(() => {
    const load = () => api<Health>('/api/health').then(setHealth).catch(() => {})
    load()
    const t = setInterval(load, 30000)
    return () => clearInterval(t)
  }, [])

  const chip = health.consumer_error
    ? { status: 'danger' as const, text: `Kafka consumer error: ${health.consumer_error.slice(0, 80)}` }
    : health.last_event_ms
      ? { status: 'success' as const, text: `Last DataHub event ${fmtTime(health.last_event_ms)}` }
      : { status: 'neutral' as const, text: 'Waiting for events…' }

  return (
    <Box maxWidth="7xl" marginX="auto" padding="6">
      <Box.Flex justifyContent="space-between" alignItems="center" marginBottom="5">
        <div>
          <Typography.LargeHeading>DataHub ingestion logs</Typography.LargeHeading>
          <Typography.Small color="default">Browse ingestion runs and forward their logs to Datadog, Kafka or OpenSearch.</Typography.Small>
        </div>
        <Box.Flex gap="4" alignItems="center">
          <StatusChip {...chip} badge />
          <Button.Ghost onClick={() => api('/api/logout', 'POST').finally(() => window.dispatchEvent(new Event('unauth')))}>
            Sign out ({user})
          </Button.Ghost>
        </Box.Flex>
      </Box.Flex>
      <Tabs value={tab} onChange={pick}>
        <Tabs.Tab title="Runs" value="runs"><Runs /></Tabs.Tab>
        <Tabs.Tab title="Integrations" value="integrations"><Integrations /></Tabs.Tab>
      </Tabs>
    </Box>
  )
}
