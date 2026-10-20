import { Alert, Box, Button, Input, Typography } from '@aivenio/aquarium'
import { useState, type FormEvent } from 'react'
import { api } from './api'

export function Login({ onLogin }: { onLogin: (user: string) => void }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true); setError('')
    try { onLogin((await api<{ user: string }>('/api/login', 'POST', { username, password })).user) }
    catch (err: any) { setError(err.message); setBusy(false) }
  }

  return (
    <Box maxWidth="sm" marginX="auto" paddingTop="l5" padding="6">
      <form onSubmit={submit}>
        <Box.Flex flexDirection="column" gap="4">
          <Typography.LargeHeading>DataHub ingestion logs</Typography.LargeHeading>
          {error && <Alert type="error">{error}</Alert>}
          <Input labelText="Username" autoComplete="username" autoFocus value={username} onChange={(e) => setUsername(e.target.value)} />
          <Input labelText="Password" type="password" autoComplete="current-password" value={password} onChange={(e) => setPassword(e.target.value)} />
          <Button.Primary type="submit" disabled={busy || !username || !password}>Sign in</Button.Primary>
        </Box.Flex>
      </form>
    </Box>
  )
}
