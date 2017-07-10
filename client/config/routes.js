import React from 'react';
import { Home, Conversion, Churn } from '../components/Components';

import {
  BrowserRouter as Router,
  Route,
  IndexRoute,
  Link
} from 'react-router-dom'

var routes = (
  <Router>
    <div>
      <nav className="navbar navbar-default" role="navigation">
        <div className="container-fluid">
          <div className="navbar-header">
            <Link className="navbar-brand" to="/">Conversion Deck</Link>
          </div>
        </div>
      </nav>

      <div className='main-container'>
        <Route exact path='/' component={Home} />
        <Route exact path='/conversions' component={Conversion}/>
        <Route exact path='/churns' component={Churn} />
      </div>
    </div>
  </Router>
);

module.exports = routes;
