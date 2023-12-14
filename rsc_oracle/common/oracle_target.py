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
from rsc_oracle.common import connection,rubrik_cluster


class OracleTarget:
    """
    Rubrik RBS (snappable) Oracle backup object.
    """

    def __init__(self, connection_name, name, cluster_id, rac=False, timeout=180):
        self.logger = logging.getLogger(__name__ + '.RubrikRSCOracleHost')
        self.cdm_timeout = timeout
        self.name = name
        self.connection = connection_name
        self.cluster_id = cluster_id
        self.rac = rac
        self.id = None
        self.rac_name = None
        if rac:
            self.logger.debug("Source is RAC. Searching for RAC targets.")
            self.get_oracle_rac_id()
        else:
            self.logger.debug("Source is Single instance. Searching for host targets.")
            self.get_oracle_host_id()

    def get_oracle_host_id(self):
        """
            Get the Oracle object id using database name and the hostname.

            Args:
                self (object): Database Object
            Returns:
                oracle_db_id (str): The Rubrik database object id.
            """
        query = gql(
            """
            query OracleHosts($typeFilter: [HierarchyObjectTypeEnum!], $filter: [Filter!]) {
              oracleTopLevelDescendants(typeFilter: $typeFilter, filter: $filter) {
                nodes {
                  ... on OracleHost {
                    id
                    name
                    cluster {
                    id
                    name
                  }
                  }
                }
              }
            }
                    """
        )


        query_variables = {
          "typeFilter": "OracleHost",
          "filter": [
            {
              "field": "NAME",
              "texts": [self.name]
            },
            {
              "field": "IS_RELIC",
              "texts": ["false"]
            },
            {
              "field": "CLUSTER_ID",
              "texts": [self.cluster_id]
            }
          ]
        }

        oracle_hosts = self.connection.graphql_query(query, query_variables)['oracleTopLevelDescendants']['nodes']
        self.logger.debug(f"Oracle hosts returned containing name {self.name}: {oracle_hosts}")
        if len(oracle_hosts) == 0:
            self.connection.delete_session()
            raise OracleTargetError(f"No hosts found with the target host name: {self.name}")
        elif len(oracle_hosts) == 1:
            self.logger.debug("Found host")
            self.id = oracle_hosts[0]['id']
        else:
            self.logger.debug("Found multiple hosts")
            cluster_matches = []
            for host in oracle_hosts:
                if host['cluster']['id'] == self.self.cluster_id:
                    cluster_matches.append(host)
            if len(cluster_matches) == 0:
                self.connection.delete_session()
                raise OracleTargetError(f"No hosts found with the target host name: {self.name} on cluster: {self.cluster_id}")
            elif len(cluster_matches) == 1:
                self.id = cluster_matches[0]['id']
            else:
                self.connection.delete_session()
                raise OracleTargetError(
                    f"Multiple hosts with name: {self.name} found on cluster: {self.cluster_id}. Please check the Oracle hosts on that cluster.")
        return

    def get_oracle_rac_id(self):
        """
            Get the Oracle object id using database name and the hostname.

            Args:
                self (object): Database Object
            Returns:
                oracle_db_id (str): The Rubrik database object id.
            """
        query = gql(
            """
            query OracleRacs($typeFilter: [HierarchyObjectTypeEnum!], $filter: [Filter!]){
              oracleTopLevelDescendants(typeFilter: $typeFilter, filter: $filter){
                nodes {
                  ... on OracleRac {
                    name
                    id
                    connectionStatus {
                      connectivity
                      timestampMillis
                    }
                    nodes {
                      hostFid
                      nodeName
                      status
                    }
                    cluster {
                      id
                      name
                    }
                  }
                  objectType
                }
              }
            }
            """
        )

        query_variables = {
          "typeFilter": "OracleRac",
          "filter": [
            {
              "field": "NAME",
              "texts": [self.name]
            },
            {
              "field": "IS_RELIC",
              "texts": ["false"]
            },
            {
              "field": "CLUSTER_ID",
              "texts": [self.cluster_id]
            }
          ]
        }

        oracle_racs = self.connection.graphql_query(query, query_variables)['oracleTopLevelDescendants']['nodes']
        self.logger.debug(f"Oracle hosts returned containing name {self.name} on cluster {self.cluster_id}: {oracle_racs}")
        if len(oracle_racs) == 0:
            self.get_oracle_rac_id_by_host()
        elif len(oracle_racs) == 1:
            self.logger.debug("Found RAC using RAC name")
            self.id = oracle_racs[0]['id']
            self.rac_name = oracle_racs[0]['name']
        else:
            self.connection.delete_session()
            raise OracleTargetError(
                f"Multiple hosts with name: {self.name} found on cluster: {self.id}. Please check the Oracle hosts on that cluster.")
        return

    def get_oracle_rac_id_by_host(self):
        """
            Get the Oracle object id using database name and the hostname.

            Args:
                self (object): Database Object
            Returns:
                oracle_db_id (str): The Rubrik database object id.
            """
        query = gql(
            """
            query OracleRacs($typeFilter: [HierarchyObjectTypeEnum!], $filter: [Filter!]){
              oracleTopLevelDescendants(typeFilter: $typeFilter, filter: $filter){
                nodes {
                  ... on OracleRac {
                    name
                    id
                    connectionStatus {
                      connectivity
                      timestampMillis
                    }
                    nodes {
                      hostFid
                      nodeName
                      status
                    }
                    cluster {
                      id
                      name
                    }
                  }
                  objectType
                }
              }
            }
            """
        )

        query_variables = {
          "typeFilter": "OracleRac",
          "filter": [
            {
              "field": "IS_RELIC",
              "texts": ["false"]
            },
            {
              "field": "CLUSTER_ID",
              "texts": [self.cluster_id]
            }
          ]
        }

        oracle_racs = self.connection.graphql_query(query, query_variables)['oracleTopLevelDescendants']['nodes']
        self.logger.debug(f"oracle_racs returned containing name {self.name} on cluster {self.cluster_id}: {oracle_racs}")
        if len(oracle_racs) == 0:
            self.connection.delete_session()
            raise OracleTargetError(f"No RAC clusters found on cluster id: {self.cluster_id}")
        else:
            self.logger.debug(f"Looking for RAC cluster containing hostname: {self.name}")
            rac_matches = []
            for rac in oracle_racs:
                for node in rac['nodes']:
                    if self.name.lower() in node['nodeName'].lower():
                        rac_matches.append(rac)
            self.logger.debug(f"rac_matches: {rac_matches}, length {len(rac_matches)}")
            if len(rac_matches) == 0:
                    self.connection.delete_session()
                    raise OracleTargetError(f"No RAC clusters found running on host name: {self.name} on cluster: {self.id}")
            elif len(rac_matches) == 1:
                self.logger.debug("Found RAC using RAC node name")
                self.id = rac_matches[0]['id']
                self.rac_name = oracle_racs[0]['name']
            else:
                self.connection.delete_session()
                raise OracleTargetError(
                    f"Multiple RAC clusters with host name: {self.name} found on cluster: {self.id}. Please check the Oracle hosts on that cluster.")
        return


class OracleTargetError(connection.NoTraceBackWithLineNumber):
    """
    Renames object so error is named with calling script
    """
    pass
