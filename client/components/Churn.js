import React, { Component } from 'react';
import { Table, Thead, Th } from 'reactable'
import { Pagination } from 'react-bootstrap'

class Churn extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: true
    };
  }

  componentWillMount() {
    this.fetchConversions()
  }

  fetchConversions(clickedPage) {
    fetch(`/api/churns?page=${clickedPage || this.state.page || 0}`)
      .then((resp) => resp.json())
      .then(({ data: churns, count, page_count, page}) => {
        this.setState({
          loading: false, churns, count, page_count, page
        });
      })

  }
  render() {
    if (this.state.loading) {
      return (
        <p>
          Loading...
        </p>
      )
    }
    return (
      <div className="content table-responsive table-full-width">
        <Table className="table table-striped" data={this.state.churns}>
          <Thead>
            <Th column="distinct_id">
              <em className="name-header">{'Mixpanel ID'}</em>
            </Th>

            <Th column="churn_proba">
              <strong className="name-header">{'Churn Probability'}</strong>
            </Th>

            <Th column="account_age">
              <em className="name-header">{"Account Age (Days)"}</em>
            </Th>

            <Th column="camp_deliveries">
              <em className="name-header">{"Campaigns Delivered"}</em>
            </Th>

            <Th column="slide_start">
              <em className="name-header">{'Slides Started'}</em>
            </Th>

            <Th column="editor_opened">
              <em className="name-header">{'Editor Opened'}</em>
            </Th>

            <Th column="deck_created">
              <em className="name-header">{'Decks Created'}</em>
            </Th>
            <Th column="signin">
              <em className="name-header">{'Sign Ins'}</em>
            </Th>

          </Thead>
        </Table>

        <Pagination
          bsSize="large"
          first
          last
          ellipsis
          boundaryLinks
          maxButtons={10}
          items={this.state.page_count}
          activePage={this.state.page}
          onSelect={(page) => this.fetchConversions(page)} />
      </div>
    );
  }

}

export default Churn;
