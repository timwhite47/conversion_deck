import React, { Component } from 'react';
import { Link } from 'react-router-dom';
class Card extends React.Component {
  render() {
    return (
      <div className="col-lg-3 col-sm-6">
        <div className="card">
            <div className="content">
                <div className="row">
                    <div className="col-xs-5">
                        <div className={`icon-big icon-${this.props.type} text-center`}>
                            <i className={this.props.icon}></i>
                        </div>
                    </div>
                    <div className="col-xs-7">
                        <div className="numbers">
                            <h6>{this.props.metric}</h6>
                            {this.props.value}
                        </div>
                    </div>
                </div>
                <div className="footer">
                    <hr />
                    <div className="stats">
                        {this.props.footer}
                    </div>
                </div>
            </div>
        </div>
      </div>
    )
  }
}

class ChurnStats extends React.Component {
  render() {
    const { stats } = this.props;
    
    return (
      <div className="row">
        <h3>
          {'Churns'}
        </h3>
        <p className='category'>
          <Link to="/conversions">
            {'View All Churns'}
          </Link>
        </p>
        <Card
          icon='ti-face-sad'
          metric='Likely Churn'
          type='success'
          value={stats['churn_likely']}
        />

        <Card
          icon='ti-face-sad'
          metric='Possible Churn'
          type='warning'
          value={stats['churn_possible']}
        />

        <Card
          icon='ti-face-sad'
          metric='Unikely Churn'
          type='danger'
          value={stats['churn_unlikely']}
        />
      </div>
    )
  }
}

class ConversionStats extends React.Component {
  render() {
    const { stats } = this.props;

    return (
      <div className="row">
        <h3>{'Conversions'}</h3>
        <p className='category'>
          <Link to="/conversions">
            {'View All Conversions'}
          </Link>
        </p>
        <Card
          icon='ti-face-smile'
          metric='Likely Conversion'
          type='success'
          value={stats['conversion_likely']}
        />

        <Card
          icon='ti-face-smile'
          metric='Possible Conversion'
          type='warning'
          value={stats['conversion_possible']}
        />

        <Card
          icon='ti-face-smile'
          metric='Unlikely Conversion'
          type='danger'
          value={stats['conversion_unlikely']}
        />
      </div>
    )
  }
}
class Dashboard extends Component {
  constructor(props) {
    super(props);
    this.state = {
      loading: true
    };
  }
  componentDidMount() {
    this.setStats()
  }
  setStats() {
    const loading = false

    fetch('/api/dashboard')
      .then((response) => response.json())
      .then((stats) => this.setState({ stats, loading }))
  }
  render() {
    if (this.state.loading) {
      return (
        <p>{'Loading...'}</p>
      )
    }

    return (
      <div className="container-fluid content">
        <ChurnStats stats={this.state.stats}/>
        <ConversionStats stats={this.state.stats}/>
      </div>
    );
  }
}

export default Dashboard;
