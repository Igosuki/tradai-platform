import {
  Box,
  Checkbox,
  Collapse,
  createStyles,
  FormControlLabel,
  IconButton,
  lighten,
  Paper,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TableSortLabel,
  Toolbar,
  Typography,
} from '@material-ui/core'
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown'
import KeyboardArrowUpIcon from '@material-ui/icons/KeyboardArrowUp'
import { makeStyles, Theme } from '@material-ui/core/styles'
import { FC, useState } from 'react'
import { GridColDef } from '@mui/x-data-grid'
import { TableRowProps } from '@material-ui/core/TableRow/TableRow'
import clsx from 'clsx'
import { ApolloClient } from '@apollo/client'

const useToolbarStyles = makeStyles((theme: Theme) =>
  createStyles({
    root: {
      paddingLeft: theme.spacing(2),
      paddingRight: theme.spacing(1),
    },
    highlight:
      theme.palette.type === 'light'
        ? {
            color: theme.palette.secondary.main,
            backgroundColor: lighten(theme.palette.secondary.light, 0.85),
          }
        : {
            color: theme.palette.text.primary,
            backgroundColor: theme.palette.secondary.dark,
          },
    title: {
      flex: '1 1 100%',
    },
  })
)

interface EnhancedTableToolbarProps {
  numSelected: number
  title?: string
  components?: FC<any>
  selected: any
}

const EnhancedTableToolbar = (props: EnhancedTableToolbarProps) => {
  const classes = useToolbarStyles()
  const { numSelected, selected } = props

  return (
    <Toolbar
      className={clsx(classes.root, {
        [classes.highlight]: numSelected > 0,
      })}
    >
      {numSelected > 0 ? (
        <Typography
          className={classes.title}
          color="inherit"
          variant="subtitle1"
          component="div"
        >
          {numSelected} selected
        </Typography>
      ) : (
        <Typography
          className={classes.title}
          variant="h6"
          id="tableTitle"
          component="div"
        >
          {props.title}
        </Typography>
      )}
      {props.components ? props.components({ numSelected, selected }) : <></>}
    </Toolbar>
  )
}

const identity = (a: any) => a

const useRowStyles = makeStyles({
  root: {
    '& > *': {
      borderBottom: 'unset',
    },
  },
})

type CollapsibleRowProps<T extends Data> = {
  row: T
  columns: TableColumn[]
  collapsedComponent?: FC<any>
  isItemSelected: boolean
  labelId: string
  actions?: FC<any>
}

function CollapsibleRow<T extends Data>(
  props: CollapsibleRowProps<T> & TableRowProps
) {
  const {
    row,
    columns,
    collapsedComponent,
    isItemSelected,
    labelId,
    actions,
    ...rowProps
  } = props
  const [open, setOpen] = useState(false)
  const classes = useRowStyles()

  return (
    <>
      <TableRow key={'main'} className={classes.root} {...rowProps}>
        <TableCell key={'checkbox'}>
          <Checkbox
            checked={isItemSelected}
            inputProps={{ 'aria-labelledby': labelId }}
          />
          {collapsedComponent ? (
            <IconButton
              aria-label="expand row"
              size="small"
              onClick={() => setOpen(!open)}
            >
              {open ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
            </IconButton>
          ) : (
            <></>
          )}
        </TableCell>
        {columns.map((c) => (
          <TableCell key={c.field} scope="row">
            {(c.render ? c.render : identity)(row[c.field])}
          </TableCell>
        ))}

        {actions ? (
          <TableCell scope="row">
            {actions({
              numSelected: 1,
              selected: [row.id],
            })}
          </TableCell>
        ) : (
          <></>
        )}
      </TableRow>
      {collapsedComponent ? (
        <TableRow key={'collapsible'}>
          <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={6}>
            <Collapse in={open} timeout="auto" unmountOnExit>
              <Box margin={1}>{collapsedComponent(row)}</Box>
            </Collapse>
          </TableCell>
        </TableRow>
      ) : (
        <></>
      )}
    </>
  )
}

function stableSort<T>(array: T[], comparator: (a: T, b: T) => number) {
  const stabilizedThis = array.map((el, index) => [el, index] as [T, number])
  stabilizedThis.sort((a, b) => {
    const order = comparator(a[0], b[0])
    if (order !== 0) return order
    return a[1] - b[1]
  })
  return stabilizedThis.map((el) => el[0])
}

function descendingComparator<T>(a: T, b: T, orderBy: keyof T) {
  if (b[orderBy] < a[orderBy]) {
    return -1
  }
  if (b[orderBy] > a[orderBy]) {
    return 1
  }
  return 0
}

type Order = 'asc' | 'desc'

function getComparator<Key extends keyof any>(
  order: Order,
  orderBy: Key
): (
  a: { [key in Key]: number | string },
  b: { [key in Key]: number | string }
) => number {
  return order === 'desc'
    ? (a, b) => descendingComparator(a, b, orderBy)
    : (a, b) => -descendingComparator(a, b, orderBy)
}

const useTableStyles = makeStyles((theme: Theme) =>
  createStyles({
    root: {
      width: '100%',
    },
    paper: {
      width: '100%',
      marginBottom: theme.spacing(2),
    },
    table: {
      minWidth: 750,
    },
    visuallyHidden: {
      border: 0,
      clip: 'rect(0 0 0 0)',
      height: 1,
      margin: -1,
      overflow: 'hidden',
      padding: 0,
      position: 'absolute',
      top: 20,
      width: 1,
    },
  })
)

type Data = {
  [key: string]: any
  id: string
}

declare type GetterFn = (field: any) => any

declare type TableColumn = GridColDef & {
  render?: GetterFn
}

export type ActionToolbarProps = {
  numSelected: number
  selected: Data[]
  client: ApolloClient<any>
}

type CollapsibleTableProps<T extends Data> = {
  rows: T[]
  columns: TableColumn[]
  collapsedComponent?: FC<any>
  defaultSortCol: string
  toolbar?: FC<any>
  title?: string
  actions?: FC<any>
}

export default function CollapsibleTable<T extends Data>(
  props: CollapsibleTableProps<T>
) {
  const classes = useTableStyles()
  const {
    columns,
    rows,
    collapsedComponent,
    defaultSortCol,
    toolbar,
    title,
    actions,
  } = props
  const [order, setOrder] = useState<Order>('asc')
  const [orderBy, setOrderBy] = useState<string>(defaultSortCol)
  const [selected, setSelected] = useState<string[]>([])
  const [dense, setDense] = useState(false)
  const [page, setPage] = useState(0)
  const [rowsPerPage, setRowsPerPage] = useState(5)

  const handleChangeDense = (event: React.ChangeEvent<HTMLInputElement>) => {
    setDense(event.target.checked)
  }

  const handleRequestSort = (
    event: React.MouseEvent<unknown>,
    property: string
  ) => {
    const isAsc = orderBy === property && order === 'asc'
    setOrder(isAsc ? 'desc' : 'asc')
    setOrderBy(property)
  }

  const handleSelectAllClick = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.checked) {
      const newSelecteds = rows.map((n) => n.id)
      setSelected(newSelecteds)
      return
    }
    setSelected([])
  }

  const handleClick = (event: React.MouseEvent<unknown>, id: string) => {
    const selectedIndex = selected.indexOf(id)
    let newSelected: string[] = []

    if (selectedIndex === -1) {
      newSelected = newSelected.concat(selected, id)
    } else if (selectedIndex === 0) {
      newSelected = newSelected.concat(selected.slice(1))
    } else if (selectedIndex === selected.length - 1) {
      newSelected = newSelected.concat(selected.slice(0, -1))
    } else if (selectedIndex > 0) {
      newSelected = newSelected.concat(
        selected.slice(0, selectedIndex),
        selected.slice(selectedIndex + 1)
      )
    }

    setSelected(newSelected)
  }

  const handleChangePage = (event: unknown, newPage: number) => {
    setPage(newPage)
  }

  const handleChangeRowsPerPage = (
    event: React.ChangeEvent<HTMLInputElement>
  ) => {
    setRowsPerPage(parseInt(event.target.value, 10))
    setPage(0)
  }

  const createSortHandler =
    (property: string) => (event: React.MouseEvent<unknown>) => {
      handleRequestSort(event, property)
    }

  const isSelected = (name: string) => selected.indexOf(name) !== -1
  const numSelected = selected.length
  const rowCount = rows.length

  return (
    <div className={classes.root}>
      <Paper className={classes.paper}>
        <EnhancedTableToolbar
          numSelected={selected.length}
          components={toolbar}
          title={title}
          selected={selected}
        />

        <TableContainer component={Paper}>
          <Table
            className={classes.table}
            aria-label="collapsible table"
            size={dense ? 'small' : 'medium'}
          >
            <TableHead>
              <TableRow>
                <TableCell>
                  <Checkbox
                    indeterminate={numSelected > 0 && numSelected < rowCount}
                    checked={rowCount > 0 && numSelected === rowCount}
                    onChange={handleSelectAllClick}
                    inputProps={{
                      'aria-label': 'select all desserts',
                    }}
                  />
                </TableCell>
                {columns.map((c, i) => (
                  <TableCell
                    key={c.field}
                    sortDirection={orderBy === c.field ? order : false}
                  >
                    <TableSortLabel
                      active={orderBy === c.field}
                      direction={orderBy === c.field ? order : 'asc'}
                      onClick={createSortHandler(c.field)}
                    >
                      {c.headerName}
                      {orderBy === c.field ? (
                        <span className={classes.visuallyHidden}>
                          {order === 'desc'
                            ? 'sorted descending'
                            : 'sorted ascending'}
                        </span>
                      ) : null}
                    </TableSortLabel>
                  </TableCell>
                ))}

                {actions ? <TableCell>Actions</TableCell> : <></>}
              </TableRow>
            </TableHead>
            <TableBody>
              {stableSort<Data>(rows, getComparator(order, orderBy))
                .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                .map((row: Data, i) => {
                  const isItemSelected = isSelected(row.id.toString())
                  const labelId = `enhanced-table-checkbox-${i}`
                  return (
                    <CollapsibleRow
                      key={row.id}
                      row={row}
                      collapsedComponent={collapsedComponent}
                      columns={columns}
                      hover
                      onClick={(event: any) =>
                        handleClick(event, row.id.toString())
                      }
                      isItemSelected={isItemSelected}
                      labelId={labelId}
                      role="checkbox"
                      aria-checked={isItemSelected}
                      tabIndex={-1}
                      selected={isItemSelected}
                      actions={actions}
                    />
                  )
                })}
            </TableBody>
          </Table>
        </TableContainer>
        <TablePagination
          rowsPerPageOptions={[5, 10, 25, 50, 100]}
          component="div"
          count={rows.length}
          rowsPerPage={rowsPerPage}
          page={page}
          onPageChange={handleChangePage}
          onRowsPerPageChange={handleChangeRowsPerPage}
        />
      </Paper>
      <FormControlLabel
        control={<Switch checked={dense} onChange={handleChangeDense} />}
        label="Dense padding"
      />
    </div>
  )
}
