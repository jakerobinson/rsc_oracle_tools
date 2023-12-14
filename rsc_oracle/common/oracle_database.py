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
Class for an Oracle database object.
"""
import datetime
import pytz
import logging
from gql import gql
from rsc_oracle.common import connection
from rsc_oracle.common import rubrik_cluster


class OracleDatabase:
    """
    Rubrik RBS (snappable) Oracle backup object.
    """

    def __init__(self, connection_name, database_name, database_host=None, cluster_name=None, relic="false", timeout=180):
        self.logger = logging.getLogger(__name__ + '.RubrikRscOracleDatabase')
        self.cdm_timeout = timeout
        self.database_name = database_name
        self.database_host = database_host
        self.connection = connection_name
        self.relic = relic
        self.cluster_name = cluster_name
        self.cluster_id = None
        self.timezone = None
        self.id = None
        self.dataguard = False
        self.get_oracle_db_id()

    def get_oracle_db_id(self):
        """
            Get the Oracle object id using database name and the hostname.

            Args:
                self (object): Database Object
            Returns:
                oracle_db_id (str): The Rubrik database object id.
            """
        query = gql(
            """
            query OracleDatabase($filter: [Filter!]) {
            oracleDatabases(filter: $filter) {
                nodes {
                  name
                  id
                  cluster {
                    name
                    id
                    timezone
                  }
                  dataGuardType
                  dataGuardGroup {
                    dataGuardType
                    dbRole
                    dbUniqueName
                    id
                  }
                  dbRole
                  dbUniqueName
                  isLiveMount
                  isRelic
                  physicalPath {
                    fid
                    name
                    objectType
                  }
                }
              }
            }
                    """
        )

        if self.cluster_name:
            cluster = rubrik_cluster.RubrikCluster(self.connection, self.cluster_name)
            self.cluster_id = cluster.id
            self.logger.debug(f"Cluster returned name: {cluster.name}, id: {cluster.id}")
            query_variables = {
                "filter": [
                    {
                        "field": "IS_RELIC",
                        "texts": [self.relic]
                    },
                    {
                        "field": "CLUSTER_ID",
                        "texts": [cluster.id]
                    },
                    {
                        "field": "NAME",
                        "texts": [self.database_name]
                    }
                ],
            }
        else:
            query_variables = {
                "filter": [
                    {
                        "field": "IS_RELIC",
                        "texts": [self.relic]
                    },
                    {
                        "field": "IS_REPLICATED",
                        "texts": ["false"]
                    },
                    {
                        "field": "NAME",
                        "texts": [self.database_name]
                    }
                ],
            }

        all_name_match_databases = self.connection.graphql_query(query, query_variables)['oracleDatabases']['nodes']
        self.logger.debug(f"Oracle DBs with name {self.database_name} returned: {all_name_match_databases}")
        self.logger.debug("Ignoring Live Mount databases found with name: {}".format(self.database_name))
        name_match_databases = []
        for node in all_name_match_databases:
            if node['isLiveMount'] == False:
                name_match_databases.append(node)
        self.logger.debug(f"Oracle DBs not live mounted with name {self.database_name} returned: {name_match_databases}")
        if len(name_match_databases) == 0:
            self.logger.debug(f"No Oracle DBs with name {self.database_name} found in oracleDatabases, trying oracleTopLevelDescendants.")
            query = gql(
                """
                query OracleDGGroups($filter: [Filter!], $typeFilter: [HierarchyObjectTypeEnum!]) {
                  oracleTopLevelDescendants(filter: $filter, typeFilter: $typeFilter) {
                    nodes {
                      ... on OracleDataGuardGroup {
                        objectType
                        name
                        id
                        cluster {
                          name
                          id
                          timezone
                        }
                        isRelic
                        dbUniqueName
                        dbRole
                        dataGuardType
                        dataGuardGroupId
                        descendantConnection {
                          nodes {
                            cluster {
                              name
                              id
                            }
                            id
                            name
                            physicalPath {
                              fid
                              name
                            }
                            ... on OracleDatabase {
                              dbUniqueName
                              isRelic
                              dbRole
                              isLiveMount
                              dataGuardGroup {
                                id
                                name
                                physicalPath {
                                  fid
                                  name
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                """
            )
            if self.cluster_name:
                cluster = rubrik_cluster.RubrikCluster(self.connection, self.cluster_name)
                self.logger.debug(f"Cluster returned name: {cluster.name}, id: {cluster.id}")
                query_variables = {
                    "filter": [
                        {
                            "field": "IS_RELIC",
                            "texts": ["false"]
                        },
                        {
                            "field": "IS_REPLICATED",
                            "texts": ["false"]
                        },
                        {
                            "field": "CLUSTER_ID",
                            "texts": [cluster.id]
                        }
                    ],
                    "typeFilter": "ORACLE_DATA_GUARD_GROUP",
                }
            else:
                query_variables = {
                    "filter": [
                        {
                            "field": "IS_RELIC",
                            "texts": ["false"]
                        },
                        {
                            "field": "IS_REPLICATED",
                            "texts": ["false"]
                        }
                    ],
                    "typeFilter": "ORACLE_DATA_GUARD_GROUP",
                }
            dg_groups = self.connection.graphql_query(query, query_variables)
            self.logger.debug("All dg groups returned: {}".format(dg_groups))
            dg_ids = []
            for dg_group in dg_groups['oracleTopLevelDescendants']['nodes']:
                for connection in dg_group['descendantConnection']['nodes']:
                    if connection['dbUniqueName'] == self.database_name:
                        self.logger.debug("Found DB with dbUniqueName")
                        self.dataguard = True
                        dg_ids.append([dg_group['id'], dg_group['cluster']['id'], dg_group['cluster']['name'], dg_group['cluster']['timezone']])
            if not dg_ids:
                self.connection.delete_session()
                raise OracleDatabaseError("No database found for database with name or db unique name: {}.".format(self.database_name))
            elif dg_ids.count(dg_ids[0]) == len(dg_ids):
                self.id = dg_ids[0][0]
                self.cluster_id = dg_ids[0][1]
                self.cluster_name = dg_ids[0][2]
                self.timezone = dg_ids[0][3]
            else:
                self.connection.delete_session()
                raise OracleDatabaseError("Multiple DG Groups found for database with name or db unique name: {}.".format(self.database_name))
            if not self.id:
                self.connection.delete_session()
                raise OracleDatabaseError("No database found for database with name or db unique name: {}.".format(self.database_name))
        elif len(name_match_databases) == 1:
            if name_match_databases[0]['dataGuardType'] == 'DATA_GUARD_MEMBER':
                self.id = name_match_databases[0]['dataGuardGroup']['id']
                self.dataguard = True
            else:
                self.id = name_match_databases[0]['id']
            self.cluster_id = name_match_databases[0]['cluster']['id']
            self.cluster_name = name_match_databases[0]['cluster']['name']
            self.timezone = name_match_databases[0]['cluster']['timezone']
        else:
            self.logger.debug("Multiple databases found with name: {}".format(self.database_name))
            if self.database_host:
                self.logger.debug("Checking for hostname match in physicalPath: {}".format(name_match_databases))
                for node in name_match_databases:
                    for path in node['physicalPath']:
                        if self.database_host in path['name']:
                            if node['dataGuardType'] == 'DATA_GUARD_MEMBER':
                                self.id = node['dataGuardGroup']['id']
                                self.dataguard = True
                            else:
                                self.id = node['id']
                            self.cluster_id = node['cluster']['id']
                            self.cluster_name = node['cluster']['name']
                            self.timezone = node['cluster']['timezone']
            else:
                self.logger.debug("Checking if the multiple databases found are part of the same DG Group")
                hosts = []
                dg_ids = []
                for node in name_match_databases:
                    if node['dataGuardType'] == 'DATA_GUARD_MEMBER':
                        dg_ids.append([node['dataGuardGroup']['id'], node['cluster']['id'], node['cluster']['name'], node['cluster']['timezone']])
                if not dg_ids:
                    self.connection.delete_session()
                    raise OracleDatabaseError(f"Multiple databases found with name or db unique name: {self.database_name}. Try specifying the host name also.")
                elif dg_ids.count(dg_ids[0]) == len(dg_ids):
                    self.id = dg_ids[0][0]
                    self.cluster_id = dg_ids[0][1]
                    self.cluster_name = dg_ids[0][2]
                    self.timezone = dg_ids[0][3]
                    self.dataguard = True
                else:
                    self.connection.delete_session()
                    raise OracleDatabaseError(
                        "Multiple DG Groups found for database with name or db unique name: {}.".format(
                            self.database_name))
            if not self.id:
                self.connection.delete_session()
                raise OracleDatabaseError(
                    "Database {} found on multiple hosts/RAC clusters: {}. You must specify a host or rac cluster name to obtain a unique id.".format(
                        self.database_name, hosts))

    def get_details(self):
        if self.dataguard:
            self.logger.debug("Database is part of a Dataguard Group. Using Dataguard Group details...")
            query = gql(
                """
                query DataGuardGroupQuery($fid: UUID!) {
                  oracleDataGuardGroup(fid: $fid) {
                    name
                    id
                    cluster {
                      id
                      name
                      timezone
                    }
                    dataGuardType
                    dbUniqueName
                    isRelic
                    numChannels
                    numInstances
                    slaAssignment
                    effectiveSlaDomain {
                      ... on ClusterSlaDomain {
                        id
                        name
                      }
                      ... on GlobalSlaReply {
                        id
                        name
                      }
                    }
                    snapshotConnection {
                      nodes {
                        date
                        id
                      }
                    }
                    descendantConnection {
                      nodes {
                        id
                        name
                        ... on OracleDatabase {
                          dbUniqueName
                          dbRole
                          physicalPath {
                            fid
                            name
                            objectType
                          }
                        }
                      }
                    }
                  }
                }
                """
            )
            query_variables = {
                "fid": self.id
            }
            database_details = self.connection.graphql_query(query, query_variables)['oracleDataGuardGroup']
        else:
            query = gql(
                """     
                query OracleDatabase($fid: UUID!) {
                oracleDatabase(fid: $fid) {
                    id
                    name
                    dataGuardType
                    isLiveMount
                    isRelic
                    numChannels
                    physicalPath {
                        name
                        fid
                        objectType
                    }
                    numInstances
                    slaAssignment
                    effectiveSlaDomain {
                      ... on ClusterSlaDomain {
                        name
                      }
                      ... on GlobalSlaReply {
                        id
                        name
                      }
                    }
                    logBackupFrequency
                    logRetentionHours
                    cluster {
                        id
                        name
                        timezone
                    }
                    snapshotConnection {
                      nodes {
                        id
                        date
                        cluster {
                          name
                        }
                        cdmId
                      }
                    }
                    }
                }
                """
            )
            query_variables = {
                "fid": self.id
            }
            database_details = self.connection.graphql_query(query, query_variables)['oracleDatabase']
        return database_details

    def get_log_backup_details(self):
        query = gql(
            """
            query OracleDatabaseLogBackupConfig($input: OracleDbInput!) {
              oracleDatabaseLogBackupConfig(input: $input) {
                hostLogRetentionHours
                logBackupFrequencyMin
                logRetentionHours
              }
            }
            """
        )
        query_variables = {
            "input": {"id": self.id}
        }
        log_backup_details = self.connection.graphql_query(query, query_variables)
        return log_backup_details['oracleDatabaseLogBackupConfig']

    def get_recovery_ranges(self):
        query = gql(
            """
            query OracleRecoverableRanges($input: GetOracleDbRecoverableRangesInput!) {
              oracleRecoverableRanges(input: $input) {
                data {
                  beginTime
                  endTime
                  status
                }
                total
              }
            }
            """
        )
        query_variables = {
            "input": {"id": self.id}
        }
        recovery_ranges = self.connection.graphql_query(query, query_variables)
        return recovery_ranges['oracleRecoverableRanges']['data']

    def get_rac_details(self, rac_id):
        query = gql(
            """
            query OracleRac($fid: UUID!) {
              oracleRac(fid: $fid) {
                id
                name
                nodes {
                  hostFid
                  nodeName
                  status
                }
              }
            }
            """
        )
        query_variables = {
             "fid": rac_id
        }
        rac_details = self.connection.graphql_query(query, query_variables)
        return rac_details['oracleRac']

    @staticmethod
    def get_cluster_timezone(connection, cluster_id):
        query = gql(
            """
            query Cluster($clusterUuid: UUID!) {
              cluster(clusterUuid: $clusterUuid) {
                timezone
              }
            }
            """
        )
        query_variables = {
            "clusterUuid": cluster_id
        }
        cluster_timezone = connection.connection.graphql_query(query, query_variables)
        return cluster_timezone['cluster']['timezone']

    @staticmethod
    def get_oracle_databases(connection):
        query = gql(
        """
            query OracleDatabases($filter: [Filter!]) {
              oracleDatabases(filter: $filter) {
                nodes {
                  name
                  dbUniqueName
                  isLiveMount
                  numInstances
                  physicalPath {
                    name
                    objectType
                  }
                  cluster {
                    name
                    }
                  dbRole
                  dataGuardType
                  dataGuardGroup {
                    name
                    dbUniqueName
                  }
                  slaAssignment              
                  effectiveSlaDomain {
                    ... on GlobalSlaReply {
                      name
                    }
                    ... on ClusterSlaDomain {
                      name
                    }
                  }                  
                }
              }
            }
                    """
        )

        query_variables = {
            "filter": [
                {
                    "field": "IS_RELIC",
                    "texts": ["false"]
                },
                {
                    "field": "IS_REPLICATED",
                    "texts": ["false"]
                }
            ],
        }

        all_databases = connection.graphql_query(query, query_variables)
        for db in all_databases['oracleDatabases']['nodes']:
            dataguard_group, rac, host_cluster = None, None, None
            for path in db['physicalPath']:
                if path['objectType'] == 'ORACLE_DATA_GUARD_GROUP':
                    dataguard_group = path['name']
                if path['objectType'] == 'OracleRac':
                    rac = True
                    host_cluster = path['name']
                if path['objectType'] == 'OracleHost':
                    host_cluster = path['name']

        return all_databases

    @staticmethod
    def cluster_time(time_string, timezone):
        """
        Converts a time string in a timezone to a user friendly string in that time zone.

        Args:
            time_string (str): Time string.
            timezone (str): Time zone.
        Returns:
            time_string (str): Time string converted to the supplied time zone.
        """
        cluster_timezone = pytz.timezone(timezone)
        utc = pytz.utc
        if time_string.endswith('Z'):
            time_string = time_string[:-1]
            datetime_object = utc.localize(datetime.datetime.fromisoformat(time_string))
        else:
            datetime_object = cluster_timezone.localize(datetime.datetime.fromisoformat(time_string))
        cluster_time_object = cluster_timezone.normalize(datetime_object.astimezone(cluster_timezone))
        return cluster_time_object.isoformat()

    @staticmethod
    def epoch_time(iso_time_string, timezone):
        """
        Converts a time string in ISO 8601 format to epoch time using the time zone.

        Args:
            iso_time_string (str): A time string in ISO 8601 format. If the string ends with Z it is considered to be in ZULU (GMT)
            timezone (str): The timezone.
        Returns:
            epoch_time (str): the epoch time.
        """
        if iso_time_string.endswith('Z'):
            iso_time_string = iso_time_string[:-1]
            utc = pytz.utc
            datetime_object = utc.localize(datetime.datetime.fromisoformat(iso_time_string))
        else:
            cluster_timezone = pytz.timezone(timezone)
            datetime_object = cluster_timezone.localize(datetime.datetime.fromisoformat(iso_time_string))
        return int(datetime_object.timestamp()) * 1000


class OracleDatabaseError(connection.NoTraceBackWithLineNumber):
    """
    Renames object so error is named with calling script
    """
    pass
