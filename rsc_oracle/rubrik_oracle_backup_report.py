import click
import logging
import sys
from tabulate import tabulate
import rsc_oracle.common.connection
from gql import gql
import concurrent.futures


@click.command()
@click.option('--debug_level', '-d', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(debug_level):
    """
    Displays information about all non-relic Oracle databases.
    Recommended console line size is 180 characters.
    """
    numeric_level = getattr(logging, debug_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: {}'.format(debug_level))
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(numeric_level)
    console_formatter = logging.Formatter('%(asctime)s: %(message)s')
    ch.setFormatter(console_formatter)
    logger.addHandler(ch)

    keyfile = "/Users/julianz/Repos/rubrik_oracle_gql/OracleApiTesting.json"
    rubrik = rsc_oracle.common.connection.RubrikConnection(keyfile)
    print("*" * 110)
    # print("Connected to cluster: {}, version: {}, Timezone: {}.".format(rubrik.name, rubrik.version, rubrik.timezone))
    # databases = rubrik.connection.get("internal", "/oracle/db")
    query = gql(
        """
        query getAllDatabases {
      oracleDatabases(filter: [{field: IS_RELIC, texts: ["false"]}, {field: IS_REPLICATED, texts: ["false"]}]) {
        count
        nodes {
          instances {
            hostId
            instanceName
          }
          id
          dataGuardType
          dataGuardGroup {
            id
          }
        }
      }
    }
        """
    )

    db_ids = rubrik.graphql_query(query)
    print(db_ids)
    # rubrik.delete_session()


    db_data = []
    dg_group_ids = []
    db_headers = ["Host/Cluster", "Database", "DG_Group", "SLA", "Log Freq", "Last DB BKUP", "Last LOG BKUP", "Missed", "CDM"]
    db_data = []
    db_list = []
    for db in db_ids['oracleDatabases']['nodes']:
        if db['dataGuardType'] == 'NON_DATA_GUARD':
            db_list.append(db['id'])
        elif db['dataGuardType'] == 'DATA_GUARD_MEMBER':
            db_list.append(db['dataGuardGroup']['id'])
    db_list = list(set(db_list))
    logger.debug("Thread list: {}".format(db_list))
    rubrik.delete_session()
    exit()

    global element_list
    element_list = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(get_db_data, db_list)

    logger.debug("Get_db_data return: {}".format(element_list))
    element_list.sort(key=lambda x: (x[0], x[1]))
    print("*" * 110)
    print(tabulate(element_list, headers=db_headers))
    print('\r\r\r')
    rubrik.delete_session()


# def get_db_data(id):
#
#     oracle_db_details = rubrik.connection.get("v1", "/oracle/db/{0}".format(id))
#     logging.debug("Oracle db details: {}".format(oracle_db_details))
#     if oracle_db_details['dataGuardType'] == 'DATA_GUARD_MEMBER':
#         logging.debug("DG Group: {}".format(oracle_db_details['dbUniqueName']))
#         for member in oracle_db_details['dataGuardGroupMembers']:
#             logging.debug("DG_GROUP member: {}".format(member['dbUniqueName']))
#             db_element = [''] * 9
#             if 'standaloneHostName' in member.keys():
#                 db_element[0] = member['standaloneHostName']
#             elif 'racName' in member.keys():
#                 db_element[0] = member['racName']
#             db_element[1] = member['dbUniqueName'] + '-' + member['role']
#             db_element[2] = oracle_db_details['dbUniqueName']
#             db_element[3] = oracle_db_details['effectiveSlaDomainName']
#             if 'logBackupFrequencyInMinutes' in oracle_db_details.keys():
#                 db_element[4] = oracle_db_details['logBackupFrequencyInMinutes']
#             else:
#                 db_element[4] = "None"
#             if 'lastSnapshotTime' in oracle_db_details.keys():
#                 db_element[5] = oracle_db_details['lastSnapshotTime'][:-5]
#             else:
#                 db_element[5] = "None"
#             if 'latestRecoveryPoint' in oracle_db_details.keys():
#                 db_element[6] = oracle_db_details['latestRecoveryPoint']
#                 db_element[6] = format(
#                     rbs_oracle_common.RubrikRbsOracleDatabase.cluster_time(oracle_db_details['latestRecoveryPoint'],
#                                                                            rubrik.timezone)[:-6])
#             else:
#                 db_element[6] = "None"
#             db_element[7] = oracle_db_details['numMissedSnapshot']
#             if oracle_db_details['isDbLocalToTheCluster']:
#                 db_element[8] = "Local"
#             else:
#                 db_element[8] = "Remote"
#             logging.debug("Element added: {}".format(db_element))
#             element_list.append(db_element)
#     elif oracle_db_details['dataGuardType'] == 'NonDataGuard':
#         db_element = [''] * 9
#         if 'standaloneHostName' in oracle_db_details.keys():
#             db_element[0] = oracle_db_details['standaloneHostName']
#         elif 'racName' in oracle_db_details.keys():
#             db_element[0] = oracle_db_details['racName']
#         db_element[1] = oracle_db_details['sid']
#         db_element[2] = 'None'
#         db_element[3] = oracle_db_details['effectiveSlaDomainName']
#         if 'logBackupFrequencyInMinutes' in oracle_db_details.keys():
#             db_element[4] = oracle_db_details['logBackupFrequencyInMinutes']
#         else:
#             db_element[4] = "None"
#         if 'lastSnapshotTime' in oracle_db_details.keys():
#             db_element[5] = oracle_db_details['lastSnapshotTime'][:-5]
#         else:
#             db_element[5] = "None"
#         if 'latestRecoveryPoint' in oracle_db_details.keys():
#             db_element[6] = format(
#                 rbs_oracle_common.RubrikRbsOracleDatabase.cluster_time(oracle_db_details['latestRecoveryPoint'],
#                                                                        rubrik.timezone)[:-6])
#         else:
#             db_element[6] = "None"
#         db_element[7] = oracle_db_details['numMissedSnapshot']
#         if oracle_db_details['isDbLocalToTheCluster']:
#             db_element[8] = "Local"
#         else:
#             db_element[8] = "Remote"
#         element_list.append(db_element)
#     return


class RubrikOracleBackupInfoError(rsc_oracle.common.connection.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
