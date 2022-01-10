import React, { FunctionComponent, useState } from 'react'
import { FormControl, InputLabel, MenuItem, Select } from '@material-ui/core'

type HostDef = {
  name: string
  host: string
}

export const servers: HostDef[] = [
  { name: 'Trader Prod Local', host: 'http://localhost:8180' },
  { name: 'Trader Dry Local', host: 'http://localhost:8187' },
  { name: 'Trader Prod VPN', host: 'http://10.104.0.2:8180' },
  { name: 'Trader Dry VPN', host: 'http://10.104.0.2:8187' },
]

export const currentGraphQLUri: () => HostDef = () => {
  const uri = localStorage.getItem('targetHost')
  return servers.find((s) => s.host == uri) || servers[0]
}

type SettingsProps = {}
export const Settings: FunctionComponent<SettingsProps> = () => {
  const [targetHost, setTargetHost] = useState(currentGraphQLUri().host)
  const handleTargetHostChange = (
    event: React.ChangeEvent<{ value: unknown }>
  ) => {
    const host = event.target.value as string
    setTargetHost(host)
    localStorage.setItem('targetHost', host)
  }
  return (
    <div style={{ height: 400, width: '100%' }}>
      <FormControl>
        <InputLabel htmlFor="target-server">Target Host</InputLabel>
        <Select
          autoFocus
          value={targetHost}
          onChange={handleTargetHostChange}
          inputProps={{
            name: 'target-server',
            id: 'target-server',
          }}
        >
          {servers.map(({ host, name }) => (
            <MenuItem key={name} value={host}>
              {name}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
    </div>
  )
}
