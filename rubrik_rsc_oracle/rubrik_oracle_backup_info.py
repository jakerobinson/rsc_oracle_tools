import click
import logging
import sys
from tabulate import tabulate
from rubrik_rsc_oracle.common import connection
from rubrik_rsc_oracle.common.oracle_database import OracleDatabase


@click.command()
@click.option('--database_name', '-d', type=str, required=False,  help='The database name')
@click.option('--host_name', '-h', type=str, required=False,  help='The database host or RAC cluster')
@click.option('--keyfile', '-k', type=str, required=False,  help='The connection keyfile path')
@click.option('--insecure', is_flag=True,  help='Flag to use insecure connection')
@click.option('--debug_level', type=str, default='WARNING', help='Logging level: DEBUG, INFO, WARNING or CRITICAL.')
def cli(database_name, host_name, keyfile, insecure, debug_level):
    """
    Displays information about the Oracle database object, the available snapshots, and recovery ranges.
    If no source_host_db is supplied, all non-relic Oracle databases will be listed.
    Recommended console line size is 120 characters.
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

    rubrik = connection.RubrikConnection(keyfile, insecure)

    if database_name:
        database = OracleDatabase(rubrik, database_name, host_name)
        logger.debug("Database ID: {}".format(database.id))
        database.get_details()
        # oracle_db_info = database.get_oracle_db_info()
        # print("*" * 95)
        # if 'dataGuardType' in oracle_db_info.keys():
        #     if oracle_db_info['dataGuardType'] == 'DataGuardGroup':
        #         print("Data Guard Group Details ")
        #         print("Data Guard Group Name: {0}   ID: {1}".format(oracle_db_info['name'], oracle_db_info['id']))
        #     else:
        #         print("Database Details ")
        #         print("Database name: {0}   ID: {1}".format(oracle_db_info['name'], oracle_db_info['id']))
        # if 'standaloneHostName' in oracle_db_info.keys():
        #     print("Host Name: {}".format(oracle_db_info['standaloneHostName']))
        # elif 'racName' in oracle_db_info.keys():
        #     print("Rac Cluster Name: {}    Instances: {}".format(oracle_db_info['racName'], oracle_db_info['numInstances']))
        # if 'dataGuardType' in oracle_db_info.keys():
        #     if oracle_db_info['dataGuardType'] == 'DataGuardGroup':
        #         for member in oracle_db_info['dataGuardGroupMembers']:
        #             print("DB Unique Name: {0}    Host: {1}    Role: {2}".format(member['dbUniqueName'], member['standaloneHostName'], member['role']))
        # print("SLA: {}    Log Backup Frequency: {} min.    Log Retention: {} hrs.".format(oracle_db_info['effectiveSlaDomainName'], oracle_db_info['logBackupFrequencyInMinutes'], oracle_db_info['logRetentionHours']))
        # oracle_snapshot_info = database.get_oracle_db_snapshots()
        # logger.debug(oracle_snapshot_info)
        # print("*" * 95)
        # print("Available Database Backups (Snapshots):")
        # for snap in oracle_snapshot_info['data']:
        #     print("Database Backup Date: {}   Snapshot ID: {}".format(database.cluster_time(snap['date'], rubrik.timezone)[:-6], snap['id']))
        #
        # oracle_db_recoverable_range_info = database.get_oracle_db_recoverable_range()
        # print("*" * 95)
        # print("Recoverable ranges:")
        # for recovery_range in oracle_db_recoverable_range_info['data']:
        #     print("Begin Time: {}   End Time: {}".format(database.cluster_time(recovery_range['beginTime'], rubrik.timezone)[:-6],
        #                                                  database.cluster_time(recovery_range['endTime'], rubrik.timezone)[:-6]))
    else:
        databases = OracleDatabase.get_oracle_databases(rubrik)['oracleDatabases']['nodes']
        db_data = []
        db_headers = ["Database", "Host/Cluster", "Instances", "DG_Group", "CDM Cluster", "SLA", "Assignment"]
        for db in databases:
            db_element = [''] * 7
            db_element[0] = db['dbUniqueName'].lower()
            for path in db['logicalPath']:
                if path['objectType'] == 'OracleRac' or path['objectType'] == 'OracleHost':
                    db_element[1] = path['name']
            db_element[2] = db['numInstances']
            if db['dataGuardType'] == 'DATA_GUARD_MEMBER':
                db_element[3] = db['dataGuardGroup']['dbUniqueName']
            db_element[4] = db['cluster']['name']
            db_element[5] = db['effectiveSlaDomain']['name']
            db_element[6] = db['slaAssignment']
            db_data.append(db_element)
        db_data.sort(key=lambda x: (x[0], x[1]))
        print("-" * 130)
        print(tabulate(db_data, headers=db_headers))
        print("-" * 130)
    rubrik.delete_session()
    return


class RubrikOracleBackupInfoError(connection.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
