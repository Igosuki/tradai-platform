import emotionNormalize from 'emotion-normalize'
import { css } from '@emotion/react'
import { unstable_createMuiStrictModeTheme as createTheme } from '@material-ui/core'
// Create a theme instance.
let theme = createTheme({
  palette: {
    // type: 'dark', // For Dark Theme

    // primary: {
    //   main: purple[500],
    // },
    // secondary: {
    //   main: '#11cb5f',
    // },
    // error: {
    //   main: red.A400,
    // },
    background: {
      default: '#fff',
    },
  },
  typography: {
    // In Chinese and Japanese the characters are usually larger,
    // so a smaller fontsize may be appropriate.
    fontSize: 14,
  },
})

theme.typography.h3 = {
  fontSize: '1.2rem',
  '@media (min-width:600px)': {
    fontSize: '1.5rem',
  },
  [theme.breakpoints.up('md')]: {
    fontSize: '2.4rem',
  },
}
// theme = responsiveFontSizes(theme);

const globalStyles = css`
  ${emotionNormalize}

  *,
    *::before,
    *::after {
    -webkit-font-smoothing: antialiased; /* Chrome, Safari */
    -moz-osx-font-smoothing: grayscale; /* Firefox */

    box-sizing: border-box;
  }

  html {
    font-size: 16px; /* 1 rem = 16px */
  }

  body {
    font-family: 'Rubik', sans-serif;
  }

  html,
  body,
  #__next {
    height: 100%;
    overflow: auto;
  }

  p {
    line-height: 1.2rem;
  }

  a {
    text-decoration: none;
    transition: all 0.125s ease-in-out;

    &:active {
      color: inherit;
    }
  }

  button {
    background: none;
    padding: 0;
    border: none;
    cursor: pointer;
  }
  .dropzone {
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding: 20px;
    border-width: 2px;
    border-radius: 2px;
    border-color: #eeeeee;
    border-style: dashed;
    background-color: #fafafa;
    color: #bdbdbd;
    outline: none;
    transition: border 0.24s ease-in-out;
  }
  .dropzone:focus,
  .dropzone:active,
  .dropzone:hover {
    border-color: #3498db;
    opacity: 1;
  }
`

export { theme, globalStyles }
