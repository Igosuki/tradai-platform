import gql from 'graphql-tag'
import { useStratsExtQuery, useStratsQuery } from '../generated-frontend'
import React, { FunctionComponent, useState } from 'react'
import CollapsibleTable from '../components/CollapsibleTable'
import { FormControlLabel, Switch } from '@material-ui/core'
import { ActionToolbar } from './ActionToolbar'
import { OperationHistory } from './OperationHistory'
import { ApolloClient, ApolloConsumer } from '@apollo/client'
import { Models } from './Models'

export const STRATEGIES_GQL = gql`
  query strats {
    strats {
      type
      id
    }
  }
`

export const STRATEGIES_EXT_GQL = gql`
  query stratsExt {
    strats {
      type
      id
      state
      status
    }
  }
  mutation changeStratState($tktc: TypeAndKeyInput!, $fm: StateFieldMutation!) {
    state(tk: $tktc, fm: $fm)
  }

  mutation cancel($tktc: TypeAndKeyInput!) {
    cancelOngoingOp(tk: $tktc)
  }

  mutation resetModel($tktc: TypeAndKeyInput!, $mr: ModelReset!) {
    resetModel(tk: $tktc, mr: $mr)
  }

  mutation lifecycleCmd($tktc: TypeAndKeyInput!, $lc: StrategyLifecycleCmd!) {
    lifecycleCmd(tk: $tktc, slc: $lc)
  }
`

interface OwnProps {
  client: ApolloClient<any>
}

type StrategyProps = OwnProps

const columns = [
  { field: 'id', headerName: 'ID', width: 300 },
  { field: 'type', headerName: 'Type', width: 300 },
]
const extended_columns = [
  {
    field: 'state',
    headerName: 'Position',
    width: 300,
    render: (state: string) => {
      let parsed = JSON.parse(state)
      return parsed.position
    },
  },
  {
    field: 'state',
    headerName: 'PnL',
    width: 300,
    render: (state: string) => {
      const pnl = parseFloat(JSON.parse(state).pnl)
      if (pnl < 100) {
        return <span style={{ color: 'red' }}>{pnl}</span>
      } else if (pnl > 100) {
        return <span style={{ color: 'green' }}>{pnl}</span>
      } else {
        return <span style={{ color: 'blue' }}>{pnl}</span>
      }
    },
  },
  {
    field: 'state',
    headerName: 'Value Strat',
    width: 300,
    render: (state: string) => {
      let parsed = JSON.parse(state)
      return `value_strat: ${parsed.value_strat}, nominal_position: ${parsed.nominal_position}`
    },
  },
  { field: 'status', headerName: 'Status', width: 300 },
]

const ActionToolbarWithConsumer = (props: any) => {
  return (
    <ApolloConsumer>
      {(client) => <ActionToolbar client={client} {...props} />}
    </ApolloConsumer>
  )
}

const Strategy: FunctionComponent<StrategyProps> = (props) => {
  const [isExtended, setExtended] = useState(false)
  const { data } = useStratsQuery({
    skip: isExtended,
  })
  // const resp = props.client.readQuery({
  //     query: STRATEGIES_GQL,
  // })
  const stratsExtQuery = useStratsExtQuery({
    skip: !isExtended,
  })

  const handleChangeExtended = (event: React.ChangeEvent<HTMLInputElement>) => {
    setExtended(event.target.checked)
  }

  const rows = isExtended ? stratsExtQuery?.data?.strats : data?.strats

  return (
    <div style={{ height: 400, width: '100%' }}>
      <CollapsibleTable
        rows={rows || []}
        columns={isExtended ? columns.concat(extended_columns) : columns}
        collapsedComponent={(row) => (
          <>
            <Models strat={row} />
            <OperationHistory strat={row} />
          </>
        )}
        defaultSortCol={'id'}
        title={'Strategies'}
        toolbar={ActionToolbarWithConsumer}
        actions={ActionToolbarWithConsumer}
      />
      <FormControlLabel
        control={
          <Switch checked={isExtended} onChange={handleChangeExtended} />
        }
        label="Extended table"
      />
    </div>
  )
}

export default Strategy
