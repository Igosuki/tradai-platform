import { StratsQuery, useModelsQuery } from '../generated-frontend'
import { DataGrid } from '@mui/x-data-grid'
import React from 'react'
import { gql } from '@apollo/client'

export const MODELS_GQL = gql`
  fragment modelFields on Model {
    id
    json
  }
  query models($tk: TypeAndKeyInput!) {
    models: models(tk: $tk) {
      ...modelFields
    }
  }
`

const model_columns = [
  { field: 'id', headerName: 'ID', width: 300 },
  { field: 'json', headerName: 'Value', width: 800 },
]

export const Models = ({
  strat,
}: {
  strat: ArrayElement<StratsQuery['strats']>
}) => {
  const { data } = useModelsQuery({
    variables: { tk: { id: strat.id, type: strat.type } },
  })
  const rows = data ? data.models : []
  return (
    <DataGrid
      autoHeight={true}
      rows={rows}
      columns={model_columns}
      pageSize={5}
      rowsPerPageOptions={[5]}
      disableSelectionOnClick
      //components={{ Toolbar: GridToolbar }}
    />
  )
}
