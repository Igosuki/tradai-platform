// @material-ui/icons
import Dashboard from '@material-ui/icons/Dashboard'
import ShoppingBasket from '@material-ui/icons/ShoppingBasket'
import SettingsIcon from '@material-ui/icons/Settings'
import Strategy from './Strategy/Strategy'
import { Settings } from './Settings/Settings'

const dashboardRoutes = [
  {
    path: '/',
    name: 'Dashboard',
    icon: Dashboard,
    component: () => <h2>Dashboard</h2>,
  },
  {
    icon: ShoppingBasket,
    name: 'Strats',
    path: '/strats',
    component: Strategy,
  },
  {
    icon: SettingsIcon,
    name: 'Settings',
    path: '/settings',
    component: Settings,
  },
]

export default dashboardRoutes
