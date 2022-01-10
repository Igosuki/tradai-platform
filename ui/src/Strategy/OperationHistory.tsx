import {
  StratsQuery,
  useOperationsQuery,
  useOrderTransactionsLazyQuery,
  useOrderTransactionsQuery,
} from '../generated-frontend'
import { DataGrid, GridRenderCellParams } from '@mui/x-data-grid'
import React, { FC, useState } from 'react'
import { Tooltip } from '@material-ui/core'
import InfoIcon from '@material-ui/icons/Info'
import { gql } from '@apollo/client'
import { makeStyles } from '@material-ui/core/styles'

export const OPERATIONS_GQL = gql`
  fragment opHistFields on OperationHistory {
    kind
    id
    transactions {
      value
      pair
      time
      pos
      price
      lastOrder
      trade {
        kind
      }
      qty
    }
  }
  query operations($tk: TypeAndKeyInput!) {
    operations: operations(tk: $tk) {
      ...opHistFields
    }
  }
  query orderTransactions($xch: String!, $id: String!) {
    transactions: orderTransactions(exchange: $xch, orderId: $id)
  }
`

const useStyles = makeStyles((theme) => ({
  tooltipWidth: {
    maxWidth: 'none',
  },
}))

interface TransactionHistoryTooltipProps {
  orderId: any
}

const TransactionHistoryTooltip = ({
  orderId,
}: TransactionHistoryTooltipProps) => {
  const classes = useStyles()
  const [getTransactions, { data }] = useOrderTransactionsLazyQuery({
    variables: { xch: 'binance', id: orderId },
  })

  return (
    <Tooltip
      classes={{ tooltip: classes.tooltipWidth }}
      onClick={() => getTransactions()}
      title={
        <pre>
          {data
            ? data.transactions.map((tr) =>
                JSON.stringify(JSON.parse(tr), null, 2)
              )
            : 'click to show data'}
        </pre>
      }
    >
      <InfoIcon />
    </Tooltip>
  )
}

const operation_columns = [
  { field: 'id', headerName: 'ID', width: 300 },
  { field: 'okind', headerName: 'Kind', width: 100 },
  { field: 'time', headerName: 'time', width: 300 },
  { field: 'value', headerName: 'value', width: 100 },
  { field: 'pos', headerName: 'pos', width: 100 },
  { field: 'price', headerName: 'price', width: 100 },
  { field: 'qty', headerName: 'qty', width: 200 },
  {
    field: 'trade',
    headerName: 'trade',
    width: 100,
    valueGetter: (params: any) => {
      return params?.row?.trade.kind
    },
  },

  {
    field: 'transactions',
    headerName: 'Transactions',
    width: 300,
    valueGetter: (params: any) => {
      return params?.row?.lastOrder
        ? JSON.parse(params?.row?.lastOrder)['id']
        : null
    },
    renderCell: (params: GridRenderCellParams) =>
      params.value ? (
        <TransactionHistoryTooltip orderId={params.value} />
      ) : (
        <span></span>
      ),
  },
  {
    field: 'lastOrder',
    headerName: 'Last Order',
    width: 300,
    valueGetter: (params: any) => {
      return params?.row?.lastOrder ? JSON.parse(params?.row?.lastOrder) : {}
    },
    renderCell: (params: GridRenderCellParams) => (
      <Tooltip
        title={
          <pre>{params.value ? JSON.stringify(params.value, null, 2) : ''}</pre>
        }
      >
        <InfoIcon />
      </Tooltip>
    ),
  },
]

export const OperationHistory = ({
  strat,
}: {
  strat: ArrayElement<StratsQuery['strats']>
}) => {
  const { data } = useOperationsQuery({
    variables: { tk: { id: strat.id, type: strat.type } },
  })
  const flattenedRows = (data ? data.operations : []).flatMap((o) => {
    const { id, kind, transactions } = o
    return transactions.map((t) => {
      return { id, okind: kind, ...t }
    })
  })
  return (
    <DataGrid
      autoHeight={true}
      rows={flattenedRows}
      columns={operation_columns}
      pageSize={5}
      rowsPerPageOptions={[5]}
      disableSelectionOnClick
      //components={{ Toolbar: GridToolbar }}
    />
  )
}
