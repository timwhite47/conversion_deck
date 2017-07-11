import React, { Component } from 'react';
import { Table, Thead, Th } from 'reactable'
import { Pagination } from 'react-bootstrap'

class Conversion extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: true,
    };
  }
  componentWillMount() {
    this.fetchConversions()
  }
  fetchConversions(clickedPage) {
    fetch(`/api/conversions?page=${clickedPage || this.state.page || 0}`)
      .then((resp) => resp.json())
      .then(({ data: conversions, count, page_count, page}) => {
        this.setState({
          loading: false, conversions, count, page_count, page
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
        <Table className="table table-striped" data={this.state.conversions}>
          <Thead>
            <Th column="conversion_proba">
              <strong className="name-header">Chance of Conversion</strong>
            </Th>
            <Th column="distinct_id">
              <em className="Distint ID">Mixpanel ID</em>
            </Th>
            <Th column="pricing_page">
              <em>
                Visit Pricing Page
              </em>
            </Th>
            <Th column="limit_notification">
              <em>
                Limit Notifications Displayed
              </em>
            </Th>
            <Th column="export_ppt">
              <em>
                Export PPT
              </em>
            </Th>
            <Th column="new_deck">
              <em>
                New Deck
              </em>
            </Th>
            <Th column="editor_opened">
              <em>
                Editor Opened
              </em>
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

export default Conversion;
