import '@fontsource-variable/inter'
import '@aivenio/aquarium/dist/styles.css'
import './dark.css'
import './styles.css'
import { Context } from '@aivenio/aquarium'
import { createRoot } from 'react-dom/client'
import { App } from './App'

createRoot(document.getElementById('root')!).render(
  <Context>
    <App />
  </Context>,
)
