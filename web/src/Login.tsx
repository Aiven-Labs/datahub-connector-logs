import { Alert, Box, Button, Input, Typography } from '@aivenio/aquarium'
import { useEffect, useState, type FormEvent } from 'react'
import { api } from './api'

export function Login({ onLogin }: { onLogin: (user: string) => void }) {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const [setup, setSetup] = useState<{ needed: boolean; token_required: boolean } | null>(null) // first run: create the admin
  const [confirm, setConfirm] = useState('')
  const [token, setToken] = useState('')
  useEffect(() => { api('/api/setup').then(setSetup).catch(() => {}) }, [])
  const creating = !!setup?.needed

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    setBusy(true); setError('')
    try { onLogin((await api<{ user: string }>(creating ? '/api/setup' : '/api/login', 'POST', { username, password, token })).user) }
    catch (err: any) { setError(err.message); setBusy(false) }
  }

  return (
    <Box maxWidth="sm" marginX="auto" paddingTop="l5" padding="6">
      <form onSubmit={submit}>
        <Box.Flex flexDirection="column" gap="4">
          <Typography.LargeHeading>DataHub ingestion logs</Typography.LargeHeading>
          {creating && <Alert type="success" title="Welcome">Create the administrator account for this app.</Alert>}
          {error && <Alert type="error">{error}</Alert>}
          <Input labelText="Username" autoComplete="username" autoFocus value={username} onChange={(e) => setUsername(e.target.value)} />
          <Input labelText="Password" type="password" autoComplete={creating ? 'new-password' : 'current-password'} description={creating ? 'At least 8 characters' : undefined} value={password} onChange={(e) => setPassword(e.target.value)} />
          {creating && <Input labelText="Repeat password" type="password" autoComplete="new-password" value={confirm} onChange={(e) => setConfirm(e.target.value)} />}
          {creating && setup?.token_required && <Input labelText="Setup token" description="The SETUP_TOKEN value set on the server" type="password" value={token} onChange={(e) => setToken(e.target.value)} />}
          <Button.Primary type="submit" disabled={busy || !username || !password || (creating && (password !== confirm || (!!setup?.token_required && !token)))}>{creating ? 'Create account' : 'Sign in'}</Button.Primary>
        </Box.Flex>
      </form>
    </Box>
  )
}
