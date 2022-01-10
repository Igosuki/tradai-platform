import React, { useEffect, useState } from 'react'
//import logo from './logo.svg'
import './App.css'
import {
  ApolloClient,
  ApolloProvider,
  createHttpLink,
  InMemoryCache,
} from '@apollo/client'
import { ThemeProvider } from '@material-ui/core/styles'
import { globalStyles, theme } from './theme'
import { CssBaseline } from '@material-ui/core'
import { Global } from '@emotion/react'
import MainApp from './MainApp'

const createClient = (targetHost: string) => {
  const uri = localStorage.getItem('targetHost') || targetHost
  const link = createHttpLink({
    uri: uri,
    credentials: 'include',
  })

  return new ApolloClient({
    cache: new InMemoryCache(),
    link: link,
  })
}

function App() {
  const [apollo, setApollo] = useState(createClient('http://localhost:8180'))
  useEffect(() => {
    function checkTargetHost() {
      const item = localStorage.getItem('targetHost')
      console.log(item)
      if (item) {
        console.warn('changing graphql target host', item)
        setApollo(createClient(item))
      }
    }

    window.addEventListener('storage', checkTargetHost)

    return () => {
      window.removeEventListener('storage', checkTargetHost)
    }
  }, [])

  return (
    <ApolloProvider client={apollo}>
      <ThemeProvider theme={theme}>
        <Global styles={globalStyles} />
        {/* CssBaseline kickstart an elegant, consistent, and simple baseline to build upon. */}
        <CssBaseline />
        <MainApp />
      </ThemeProvider>
    </ApolloProvider>
  )
}

export default App
