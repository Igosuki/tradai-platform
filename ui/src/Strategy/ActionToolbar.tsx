import { ActionToolbarProps } from '../components/CollapsibleTable'
import {
  MutableField,
  StrategyLifecycleCmd,
  useCancelMutation,
  useChangeStratStateMutation,
  useLifecycleCmdMutation,
  useResetModelMutation,
} from '../generated-frontend'
import {
  Button,
  createStyles,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  IconButton,
  InputLabel,
  MenuItem,
  Paper,
  PaperProps,
  Select,
  TextField,
  Tooltip,
} from '@material-ui/core'
import EditIcon from '@material-ui/icons/Edit'
import ResetIcon from '@material-ui/icons/RotateLeft'
import CancelIcon from '@material-ui/icons/Cancel'
import StopIcon from '@material-ui/icons/PanTool'
import FilterListIcon from '@material-ui/icons/FilterList'
import React, { FunctionComponent, useState } from 'react'
import { makeStyles, Theme } from '@material-ui/core/styles'
import { DataGrid } from '@mui/x-data-grid'
import Draggable from 'react-draggable'
import { enumKeys } from '../util'
import { STRATEGIES_GQL } from './Strategy'
import LoadingButton from '../components/LoadingButton'

function DraggablePaper(props: PaperProps) {
  return (
    <Draggable
      handle="#draggable-dialog-title"
      cancel={'[class*="MuiDialogContent-root"]'}
    >
      <Paper {...props} />
    </Draggable>
  )
}

type ActionDialogProps = {
  open: boolean
  setOpen: (open: boolean) => void
  title: string
}

const ActionDialog: FunctionComponent<ActionDialogProps> = (props) => {
  const { open, setOpen, title } = props
  const handleClose = () => {
    setOpen(false)
  }

  return (
    <div>
      <Dialog
        open={open}
        scroll={'paper'}
        onClose={handleClose}
        maxWidth={'lg'}
        fullWidth={true}
        aria-labelledby="strat-action-modal"
        aria-describedby="strat-action-modal-description"
        PaperComponent={DraggablePaper}
      >
        <DialogTitle style={{ cursor: 'move' }} id="draggable-dialog-title">
          {title}
        </DialogTitle>
        <DialogContent>{props.children}</DialogContent>
        <DialogActions>
          <Button autoFocus onClick={handleClose} color="primary">
            Cancel
          </Button>
        </DialogActions>
      </Dialog>
    </div>
  )
}

type SelectedActionModalProps = {
  selected: Record<string, any>[]
  open: boolean
  setOpen: (open: boolean) => void
}

const selectedStratActionColumns = [
  { field: 'id', headerName: 'ID', width: 100 },
  { field: 'type', headerName: 'Type', width: 200 },
  {
    field: 'response',
    headerName: 'Response',
    width: 300,
    valueGetter: (params: any) => {
      return params?.row?.response
    },
  },
]

const useActionFormStyles = makeStyles((theme: Theme) =>
  createStyles({
    form: {
      display: 'flex',
      flexDirection: 'row',
      margin: 'auto',
      width: 'fit-content',
    },
    formControl: {
      marginTop: theme.spacing(2),
      minWidth: 120,
    },
    formControlLabel: {
      marginTop: theme.spacing(1),
    },
  })
)

const stratKey = (strat: any) => `${strat.id}_${strat.type}`

const EditStateModal: FunctionComponent<SelectedActionModalProps> = (props) => {
  const classes = useActionFormStyles()
  const [mutableField, setMutableField] = useState<MutableField>(
    MutableField.NominalPosition
  )
  const [mutableFieldValue, setMutableFieldValue] = useState<number>(0.0)
  const [responses, setResponses] = useState<Record<string, any>>({})
  const [changeStratState, { loading }] = useChangeStratStateMutation({})
  const gridRows = (props.selected || []).map((s) => {
    return { response: responses[stratKey(s)], ...s }
  })
  const handleMutableFieldChange = (
    event: React.ChangeEvent<{ value: unknown }>
  ) => {
    setMutableField(event.target.value as MutableField)
  }
  const handleMutableFieldValueChange = (
    event: React.ChangeEvent<{ value: unknown }>
  ) => {
    setMutableFieldValue(event.target.value as number)
  }

  const handleSaveState = async () => {
    const responses = await Promise.all(
      props.selected.map(async (strat) => {
        if (mutableField === null) {
          return
        }
        const { data, errors } = await changeStratState({
          variables: {
            fm: {
              field: mutableField || MutableField.NominalPosition,
              value: mutableFieldValue * 1.0,
            },
            tktc: { type: strat.type, id: strat.id },
          },
        })
        return { [stratKey(strat)]: data ? data.state : errors }
      })
    )
    setResponses(Object.assign({}, ...responses))
  }

  return (
    <ActionDialog
      open={props.open}
      setOpen={props.setOpen}
      title={'Edit state'}
    >
      <form className={classes.form} noValidate>
        <FormControl className={classes.formControl}>
          <InputLabel htmlFor="mutable-field">State Field</InputLabel>
          <Select
            autoFocus
            value={mutableField}
            onChange={handleMutableFieldChange}
            inputProps={{
              name: 'mutable-field',
              id: 'mutable-field',
            }}
          >
            {enumKeys(MutableField).map((k) => (
              <MenuItem key={k} value={MutableField[k]}>
                {MutableField[k]}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
        <FormControl className={classes.formControl}>
          <TextField
            id="mutable-field-value"
            label="Value"
            type="number"
            value={mutableFieldValue}
            onChange={handleMutableFieldValueChange}
            InputLabelProps={{
              shrink: true,
            }}
          />
        </FormControl>
      </form>
      <LoadingButton
        variant="contained"
        color="secondary"
        //className={classes.button}
        startIcon={<EditIcon />}
        onClick={() => handleSaveState()}
        loading={loading}
      >
        Save state
      </LoadingButton>
      <hr />
      <DataGrid
        autoHeight={true}
        rows={gridRows}
        columns={selectedStratActionColumns}
        pageSize={5}
        rowsPerPageOptions={[5]}
        disableSelectionOnClick
        getRowId={(row) => row.id}
      />
    </ActionDialog>
  )
}

const ResetModelsModal: FunctionComponent<SelectedActionModalProps> = (
  props
) => {
  const [resetModel, { loading }] = useResetModelMutation({})
  const [responses, setResponses] = useState<Record<string, any>>({})
  const handleResetModels = async () => {
    const responses = await Promise.all(
      props.selected.map(async (strat) => {
        const { data, errors } = await resetModel({
          variables: {
            mr: {
              name: null,
              restartAfter: true,
              stopTrading: true,
            },
            tktc: { type: strat.type, id: strat.id },
          },
        }).then(
          (data) => {
            return {
              data: data.data,
              errors: undefined,
            }
          },
          (error: any) => {
            return { errors: error, data: undefined }
          }
        )
        console.log('data and errors', data, errors)
        return { [stratKey(strat)]: data ? data.resetModel : errors }
      })
    )
    console.log(responses)
    setResponses(Object.assign({}, ...responses))
  }
  const gridRows = (props.selected || []).map((s) => {
    return { response: responses[stratKey(s)], ...s }
  })

  return (
    <ActionDialog
      open={props.open}
      setOpen={props.setOpen}
      title={'Reset Models'}
    >
      <LoadingButton
        variant="contained"
        color="secondary"
        //className={classes.button}
        startIcon={<ResetIcon />}
        onClick={() => handleResetModels()}
        loading={loading}
      >
        Reset Models
      </LoadingButton>
      <hr />
      <DataGrid
        autoHeight={true}
        rows={gridRows}
        columns={selectedStratActionColumns}
        pageSize={5}
        rowsPerPageOptions={[5]}
        disableSelectionOnClick
        getRowId={(row) => row.id}
      />
    </ActionDialog>
  )
}

const StopStratModal: FunctionComponent<SelectedActionModalProps> = (props) => {
  const classes = useActionFormStyles()
  const [lifecycleCmdMutation, { loading }] = useLifecycleCmdMutation({})
  const [responses, setResponses] = useState<Record<string, any>>({})
  const [lifecycleCmd, setLifecycleCmd] = useState<StrategyLifecycleCmd>(
    StrategyLifecycleCmd.ResumeTrading
  )
  const handleLifecycleCmd = async () => {
    const responses = await Promise.all(
      props.selected.map(async (strat) => {
        const { data, errors } = await lifecycleCmdMutation({
          variables: {
            tktc: { type: strat.type, id: strat.id },
            lc: lifecycleCmd,
          },
        })
        return {
          [stratKey(strat)]: data ? data.lifecycleCmd : errors,
        }
      })
    )
    setResponses(Object.assign({}, ...responses))
  }
  const gridRows = (props.selected || []).map((s) => {
    return { response: responses[stratKey(s)], ...s }
  })
  const handleLifecycleCmdChange = (
    event: React.ChangeEvent<{ value: unknown }>
  ) => {
    setLifecycleCmd(event.target.value as StrategyLifecycleCmd)
  }

  return (
    <ActionDialog
      open={props.open}
      setOpen={props.setOpen}
      title={'Stop Strategies'}
    >
      <form className={classes.form} noValidate>
        <FormControl className={classes.formControl}>
          <InputLabel htmlFor="mutable-field">Strategy command</InputLabel>
          <Select
            autoFocus
            value={lifecycleCmd}
            onChange={handleLifecycleCmdChange}
            inputProps={{
              name: 'strategy-cmd',
              id: 'strategy-cmd',
            }}
          >
            {enumKeys(StrategyLifecycleCmd).map((k) => (
              <MenuItem key={k} value={StrategyLifecycleCmd[k]}>
                {StrategyLifecycleCmd[k]}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </form>
      <LoadingButton
        variant="contained"
        color="secondary"
        //className={classes.button}
        startIcon={<StopIcon />}
        onClick={() => handleLifecycleCmd()}
        loading={loading}
      >
        Send Command
      </LoadingButton>
      <hr />
      <DataGrid
        autoHeight={true}
        rows={gridRows}
        columns={selectedStratActionColumns}
        pageSize={5}
        rowsPerPageOptions={[5]}
        disableSelectionOnClick
        getRowId={(row) => row.id}
      />
    </ActionDialog>
  )
}

const CancelOngoingOperationModal: FunctionComponent<SelectedActionModalProps> =
  (props) => {
    const [cancelOngoingOp, { loading }] = useCancelMutation({})
    const [responses, setResponses] = useState<Record<string, any>>({})
    const handleCancelOngoingOp = async () => {
      const responses = await Promise.all(
        props.selected.map(async (strat) => {
          const { data, errors } = await cancelOngoingOp({
            variables: {
              tktc: { type: strat.type, id: strat.id },
            },
          })
          return {
            [stratKey(strat)]: data ? data.cancelOngoingOp : errors,
          }
        })
      )
      setResponses(Object.assign({}, ...responses))
    }
    const gridRows = (props.selected || []).map((s) => {
      return { response: responses[stratKey(s)], ...s }
    })
    return (
      <ActionDialog
        open={props.open}
        setOpen={props.setOpen}
        title={'Cancel ongoing operations'}
      >
        <LoadingButton
          variant="contained"
          color="secondary"
          //className={classes.button}
          startIcon={<CancelIcon />}
          onClick={() => handleCancelOngoingOp()}
          loading={loading}
        >
          Cancel ongoing operations
        </LoadingButton>
        <hr />
        <DataGrid
          autoHeight={true}
          rows={gridRows}
          columns={selectedStratActionColumns}
          pageSize={5}
          rowsPerPageOptions={[5]}
          disableSelectionOnClick
          getRowId={(row) => row.id}
        />
      </ActionDialog>
    )
  }

export const ActionToolbar = ({
  numSelected,
  selected,
  client,
}: ActionToolbarProps) => {
  const [openResetModels, setOpenResetModels] = useState(false)
  const [openEditState, setOpenEditState] = useState(false)
  const [openCancelOngoingOperation, setOpenCancelOngoingOperation] =
    useState(false)
  const [openStop, setOpenStop] = useState(false)
  const resp = client.readQuery({
    query: STRATEGIES_GQL,
    variables: {},
  })

  const rows = selected.map((id) => {
    return resp.strats.find((s: any) => s.id === id)
  })
  return (
    <>
      <Tooltip title="Edit state">
        <span>
          <IconButton
            aria-label="edit"
            disabled={numSelected <= 0}
            onClick={() => setOpenEditState(true)}
          >
            <EditIcon />
          </IconButton>
        </span>
      </Tooltip>
      <Tooltip title="Reset models">
        <span>
          <IconButton
            aria-label="reset"
            disabled={numSelected <= 0}
            onClick={() => setOpenResetModels(true)}
          >
            <ResetIcon />
          </IconButton>
        </span>
      </Tooltip>
      <Tooltip title="Cancel ongoing operation">
        <span>
          <IconButton
            aria-label="cancel"
            disabled={numSelected <= 0}
            onClick={() => setOpenCancelOngoingOperation(true)}
          >
            <CancelIcon />
          </IconButton>
        </span>
      </Tooltip>
      {numSelected > 0 ? (
        <Tooltip title="Stop">
          <IconButton aria-label="stop" onClick={() => setOpenStop(true)}>
            <StopIcon />
          </IconButton>
        </Tooltip>
      ) : (
        <Tooltip title="Filter list">
          <IconButton aria-label="filter list">
            <FilterListIcon />
          </IconButton>
        </Tooltip>
      )}
      <EditStateModal
        selected={rows}
        open={openEditState}
        setOpen={setOpenEditState}
      />
      <ResetModelsModal
        selected={rows}
        open={openResetModels}
        setOpen={setOpenResetModels}
      />
      <CancelOngoingOperationModal
        selected={rows}
        open={openCancelOngoingOperation}
        setOpen={setOpenCancelOngoingOperation}
      />
      <StopStratModal selected={rows} open={openStop} setOpen={setOpenStop} />
    </>
  )
}
