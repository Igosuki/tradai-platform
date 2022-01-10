import React, { FunctionComponent } from 'react'
import Layout from './Layout'
import { BrowserRouter as Router, Route } from 'react-router-dom'
import routes from './routes'

interface OwnProps {}
type Props = OwnProps

const MainApp: FunctionComponent<Props> = (props) => {
  return (
    <Router>
      <Layout>
        {routes.map((route) => (
          <Route
            exact
            key={route.path}
            path={route.path}
            component={route.component}
          />
        ))}
      </Layout>
    </Router>
  )
}

export default MainApp
