import React from 'react';
import { Home, Conversion, Churn, Sidebar, ContentNav, Footer } from '../components/Components';

import {
  BrowserRouter as Router,
  Route,
  IndexRoute,
  Link
} from 'react-router-dom'

var routes = (
  <Router>
    <div className='content'>
      <Sidebar />

      <div className="main-panel" id='app'>
          <ContentNav />

          <div className='container'>
            <Route exact path='/' component={Home} />
            <Route exact path='/conversions' component={Conversion}/>
            <Route exact path='/churns' component={Churn} />
          </div>

          <Footer />
      </div>
    </div>
  </Router>
);

module.exports = routes;
