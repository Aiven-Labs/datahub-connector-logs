export type Run = {
  urn: string; source_urn: string | null; source_name: string | null; source_type: string | null
  status: string; start_ms: number | null; duration_ms: number | null; summary: string | null
  stuck: boolean; report?: string | null
}
export type Source = { source_urn: string; source_name: string; runs: number; failed: number }
export type Field = {
  name: string; label: string; type: 'text' | 'password' | 'textarea' | 'select'
  default?: string; options?: string[]; required?: boolean; secret?: boolean; placeholder?: string
}
export type DestType = { type: string; label: string; fields: Field[] }
export type Dest = {
  id: number; name: string; type: string; enabled: boolean; config: Record<string, string>; secrets_set: string[]
  statuses: string[]; source_urns: string[]; max_report_kb: number
  sent: number; retrying: number; dead: number; last_error: string | null
}
export type Health = { consumer_error?: string | null; last_event_ms?: number | null }

export async function api<T = any>(url: string, method = 'GET', body?: unknown): Promise<T> {
  const r = await fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: body === undefined ? undefined : JSON.stringify(body) })
  const j = await r.json().catch(() => ({}))
  if (r.status === 401 && url !== '/api/login') window.dispatchEvent(new Event('unauth'))
  if (!r.ok) throw new Error(j.detail || r.statusText)
  return j
}

export const fmtTime = (ms?: number | null) => (ms ? new Date(ms).toLocaleString() : '-')
export const fmtDur = (ms?: number | null) => (ms == null ? '-' : ms < 60000 ? `${(ms / 1000).toFixed(0)}s` : `${(ms / 60000).toFixed(1)}m`)
