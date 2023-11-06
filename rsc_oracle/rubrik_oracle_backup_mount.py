import click
import logging
import sys
from tabulate import tabulate
from rsc_oracle.common import connection
from rsc_oracle.common import oracle_database


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
        database = oracle_database.OracleDatabase(rubrik, database_name, host_name)
        logger.debug("Database ID: {}".format(database.id))
        database_details = database.get_details()
        logger.debug(f"DB Details: {database_details}")
        log_backup_details = database.get_log_backup_details()
        logger.debug(f"DB log backup Details: {log_backup_details}")
        recovery_ranges = database.get_recovery_ranges()
        logger.debug(f"Backup recovery ranges: {recovery_ranges}")
        timezone = database_details['cluster']['timezone']
        print("-" * 95)
        if database.dataguard:
            print("Data Guard Group Details ")
            print(f"Data Guard Group Name: {database_details['name']}    ID: {database_details['id']}")
            for node in database_details['descendantConnection']['nodes']:
                host_type = "None"
                for path in node['physicalPath']:
                    if path['objectType'] == 'OracleHost':
                        host_type = "Host Name"
                        host_name = path['name']
                    elif path['objectType'] == 'OracleRac':
                        host_type = "RAC Name"
                        host_name = path['name']
                print(f"Unique Name: {node['dbUniqueName']}     {host_type}: {host_name}     Role: {node['dbRole']}")
        else:
            print("Database name: {0}   ID: {1}".format(database_details['name'], database_details['id']))
            if database_details['physicalPath'][0]['objectType'] == 'OracleRac':
                print(f"RAC Cluster Name: {database_details['physicalPath'][0]['name']}    Number of instances: {database_details['numInstances']}")
                rac_details = database.get_rac_details(database_details['physicalPath'][0]['fid'])
                print(f"RAC Nodes:")
                for node in rac_details['nodes']:
                    print(f"{node['nodeName']}   Status: {node['status']}")
            else:
                print(f"Host Name: {database_details['physicalPath'][0]['name']}")
        print(f"SLA: {database_details['effectiveSlaDomain']['name']}   SLA Assignment: {database_details['slaAssignment']} ")
        print(f"Log Backup Frequency: {log_backup_details['logBackupFrequencyMin']} minutes    Log Retention: {int(log_backup_details['logRetentionHours'] / 24)} Days")
        print(f"Backup Channels: {database_details['numChannels']}")
        print(f"Cluster: {database_details['cluster']['name']}    Timezone: {timezone}")
        print("-" * 95)
        print("Available Database Backups (Snapshots):")
        for snap in database_details['snapshotConnection']['nodes']:
            print("Database Backup Date: {}   Snapshot ID: {}".format(
                database.cluster_time(snap['date'], timezone)[:-6], snap['id']))
        print("-" * 95)
        print("Recoverable ranges:")
        for recovery_range in recovery_ranges:
            print("Begin Time: {}   End Time: {}".format(
                database.cluster_time(recovery_range['beginTime'], timezone)[:-6],
                database.cluster_time(recovery_range['endTime'], timezone)[:-6]))
        print('-' * 95)
    else:
        databases = oracle_database.OracleDatabase.get_oracle_databases(rubrik)['oracleDatabases']['nodes']
        db_data = []
        db_headers = ["Database", "DB Unique Name", "Role", "DG_Group", "Host/Cluster", "Instances", "CDM Cluster", "SLA", "Assignment"]
        for db in databases:
            if not db['isLiveMount']:
                db_element = [''] * 9
                db_element[0] = db['name'].lower()
                for path in db['physicalPath']:
                    if path['objectType'] == 'OracleRac' or path['objectType'] == 'OracleHost':
                        db_element[4] = path['name']
                db_element[5] = db['numInstances']
                if db['dataGuardType'] == 'DATA_GUARD_MEMBER':
                    db_element[1] = db['dbUniqueName']
                    db_element[2] = db['dbRole']
                    db_element[3] = db['dataGuardGroup']['name']
                db_element[6] = db['cluster']['name']
                db_element[7] = db['effectiveSlaDomain']['name']
                db_element[8] = db['slaAssignment']
                db_data.append(db_element)
        db_data.sort(key=lambda x: (x[0], x[1]))
        print("-" * 162)
        print(tabulate(db_data, headers=db_headers))
        print("-" * 162)
    rubrik.delete_session()
    return


class RubrikOracleBackupInfoError(connection.NoTraceBackWithLineNumber):
    """
        Renames object so error is named with calling script
    """
    pass


if __name__ == "__main__":
    cli()
