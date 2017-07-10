import React, { Component } from 'react';
import { Link } from 'react-router-dom';
import Churn from './Churn'
import Conversion from './Conversion'
import { Table, Thead, Th } from 'reactable'

class ContentNav extends React.Component {
  render() {
    return (
      <nav className="navbar navbar-default">
          <div className="container-fluid">
              <div className="navbar-header">
                  <button type="button" className="navbar-toggle">
                      <span className="sr-only">Toggle navigation</span>
                      <span className="icon-bar bar1"></span>
                      <span className="icon-bar bar2"></span>
                      <span className="icon-bar bar3"></span>
                  </button>
                  <a className="navbar-brand" href="#">Dashboard</a>
              </div>
          </div>
      </nav>
    )
  }
}
class Footer extends React.Component {
  render() {
    return <footer className="footer">
        <div className="container-fluid">
            <nav className="pull-left"></nav>
            <div className="copyright pull-right"></div>
        </div>
    </footer>
  }
}
class Sidebar extends React.Component {
  render() {
    return (
      <div className="sidebar" data-background-color="white" data-active-color="danger">
        <div className="sidebar-wrapper">
              <div className="logo">
                  <a href="/" className="simple-text">
                      Conversion Deck
                  </a>
              </div>

              <ul className="nav">
                  <li className="active">
                      <Link to="/">
                          <i className="ti-dashboard"></i>
                          <p>Dashboard</p>
                      </Link>
                  </li>
                  <li>
                      <Link to="/conversions">
                          <i className="ti-face-smile"></i>
                          <p>Conversion</p>
                      </Link>
                  </li>
                  <li>
                      <Link to="/churns">
                          <i className="ti-face-sad"></i>
                          <p>Churn</p>
                      </Link>
                  </li>
              </ul>
        </div>
      </div>
    )
  }
}

class Home extends Component {
  render() {
    return (
      <div className="container">
        <div className="jumbotron text-center">
          <h1>Hello there</h1>

          <Link to='/conversions'>
            <button className="btn btn-primary">Conversions</button>
          </Link>

          <Link to='/churns'>
            <button className="btn btn-primary">Churns</button>
          </Link>
        </div>
      </div>
    );
  }

}


export { Home, Conversion, Churn, Sidebar, Footer, ContentNav };
