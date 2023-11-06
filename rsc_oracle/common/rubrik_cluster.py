# Copyright 2020 Rubrik, Inc.
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to
#  deal in the Software without restriction, including without limitation the
#  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
#  sell copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#  DEALINGS IN THE SOFTWARE.


"""
Class for an Oracle host object.
"""
import datetime
import pytz
import logging
from gql import gql
from rsc_oracle.common import connection


class RubrikCluster:
    """
    Rubrik RBS (snappable) Oracle backup object.
    """

    def __init__(self, connection_name, cluster_name, timeout=180):
        self.logger = logging.getLogger(__name__ + '.RubrikRSCOracleHost')
        self.cdm_timeout = timeout
        self.connection = connection_name
        self.name = cluster_name
        self.id = self.get_cluster_id()

    def get_cluster_id(self):
        """
            Get the cluster id using the cluster name.

            Args:
                self (object): Cluster Name
            Returns:
                cluster_id (str): The Rubrik cluster id.
            """
        query = gql(
            """
            query ClusterConnection($filter: ClusterFilterInput) {
              clusterConnection(filter: $filter) {
                nodes {
                  id
                  name
                }
              }
            }
                    """
        )

        query_variables = {
            "filter": {
            "name": self.name
            }
        }

        rubrik_cluster = self.connection.graphql_query(query, query_variables)['clusterConnection']['nodes']
        if len(rubrik_cluster) == 0:
            self.connection.delete_session()
            raise OracleClusterError(f"No clusters found with the the cluster name: {self.name}")
        elif len(rubrik_cluster) == 1:
            cluster_id = rubrik_cluster[0]['id']
        else:
            self.connection.delete_session()
            raise OracleClusterError(f"Multiple clusters found with name: {self.name} found. Please check the cluster name.")
        return cluster_id


class OracleClusterError(connection.NoTraceBackWithLineNumber):
    """
    Renames object so error is named with calling script
    """
    pass
