import React, { Component } from 'react';
import {Link} from 'react-router';

class Main extends Component {
  render() {
    return (
      <div>
        <nav className="navbar navbar-default" role="navigation">
          <div className="container-fluid">
            <div className="navbar-header">
              <a className="navbar-brand" href="#">Name</a>
            </div>
          </div>
        </nav>
        <div className='main-container'>
          {this.props.children}
        </div>
      </div>
    );
  }
}

class Home extends Component {
  render() {
    return (
      <div className="container">
        <div className="jumbotron text-center">
          <h1>Hello there</h1>
          <Link to='/view'>
            <button className="btn btn-primary">Start</button>
          </Link>
        </div>
      </div>
    );
  }

}


export {Main, Home};
