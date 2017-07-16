import React from 'react';
import {
  Home,
  Conversion,
  Churn,
  Dashboard,
  Sidebar,
  ContentNav,
  Footer
} from '../components/Components';

import {
  BrowserRouter as Router,
  Route,
  IndexRoute,
  Link
} from 'react-router-dom'

class App extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      tab: 'dashboard',
    };
  }
  render() {
    return (
      <Router>
        <div className='content'>
          <Sidebar
            changeTab={(tab) => this.setState({ tab })}
            tab={this.state.tab}
          />

          <div className="main-panel " id='app'>
              <ContentNav title={this.state.tab}/>

              <div className='container-fluid content'>
                <Route exact path='/' component={Home} />
                <Route exact path='/dashboard'component={Dashboard} />
                <Route exact path='/conversions' component={Conversion}/>
                <Route exact path='/churns' component={Churn} />
              </div>

              <Footer />
          </div>
        </div>
      </Router>
    )
  }
}


export default App
