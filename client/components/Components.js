import React, { Component } from 'react';
import { Link } from 'react-router-dom';

class Conversion extends React.Component {
  render() {
    return <h1>Conversions!!!</h1>
  }
}

class Churn extends React.Component {
  render() {
    return <h1>Churns!!!</h1>
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


export { Home, Conversion, Churn };
