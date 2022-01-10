import {
  Button,
  ButtonProps,
  CircularProgress,
  createStyles,
} from '@material-ui/core'
import { makeStyles, Theme } from '@material-ui/core/styles'
import React from 'react'
import { green } from '@material-ui/core/colors'

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    root: {
      display: 'flex',
      alignItems: 'center',
    },
    wrapper: {
      margin: theme.spacing(1),
      position: 'relative',
    },
    buttonProgress: {
      color: green[500],
      position: 'absolute',
      top: '50%',
      left: '50%',
      marginTop: -12,
      marginLeft: -12,
    },
  })
)

type LoadingButtonProps = {
  loading?: boolean
}

const LoadingButton = (props: LoadingButtonProps & ButtonProps) => {
  const classes = useStyles()
  const { loading, children, ...rest } = props

  return (
    <div className={classes.wrapper}>
      <Button disabled={loading} {...rest}>
        {children}
      </Button>
      {loading && (
        <CircularProgress size={24} className={classes.buttonProgress} />
      )}
    </div>
  )
}

export default LoadingButton
